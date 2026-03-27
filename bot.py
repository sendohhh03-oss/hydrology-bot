import os
import logging
import asyncio
import zipfile
import tempfile
from datetime import datetime, timedelta

import aiohttp
from flask import Flask, request, jsonify
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware

# ========== КОНФИГУРАЦИЯ ==========
BOT_TOKEN = os.environ.get("BOT_TOKEN")
COPERNICUS_EMAIL = os.environ.get("COPERNICUS_EMAIL")
COPERNICUS_PASSWORD = os.environ.get("COPERNICUS_PASSWORD")
# ==================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

app = Flask(__name__)

class CopernicusClient:
    def __init__(self, email, password):
        self.email = email
        self.password = password
        self.access_token = None
        self.token_expiry = None
        
    async def get_token(self):
        if self.access_token and self.token_expiry and self.token_expiry > datetime.now():
            return self.access_token
            
        auth_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        auth_data = {
            "client_id": "cdse-public",
            "grant_type": "password",
            "username": self.email,
            "password": self.password
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(auth_url, data=auth_data) as response:
                if response.status == 200:
                    data = await response.json()
                    self.access_token = data["access_token"]
                    self.token_expiry = datetime.now() + timedelta(seconds=data["expires_in"] - 60)
                    return self.access_token
                else:
                    raise Exception("Ошибка авторизации")
    
    async def search_sentinel2(self, bbox, start_date, end_date, max_cloud_cover=30, max_items=5):
        token = await self.get_token()
        
        polygon = f"POLYGON(({bbox[0]} {bbox[1]}, {bbox[2]} {bbox[1]}, {bbox[2]} {bbox[3]}, {bbox[0]} {bbox[3]}, {bbox[0]} {bbox[1]}))"
        
        url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
        params = {
            "$filter": f"Collection/Name eq 'SENTINEL-2' and OData.CSC.Intersects(area=geography'SRID=4326;{polygon}') and ContentDate/Start gt {start_date}T00:00:00.000Z and ContentDate/Start lt {end_date}T23:59:59.999Z and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/Value le {max_cloud_cover})",
            "$top": max_items,
            "$orderby": "ContentDate/Start desc",
            "$expand": "Attributes"
        }
        headers = {"Authorization": f"Bearer {token}"}
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params, headers=headers, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("value", [])
                    return []
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                return []
    
    async def download_product(self, product_id, output_path):
        token = await self.get_token()
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products('{product_id}')/$value"
        headers = {"Authorization": f"Bearer {token}"}
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers, timeout=300) as response:
                    if response.status == 200:
                        with open(output_path, 'wb') as f:
                            while True:
                                chunk = await response.content.read(8192)
                                if not chunk:
                                    break
                                f.write(chunk)
                        return True
                    return False
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                return False

def parse_coordinates(text):
    text = text.strip().replace(',', ' ')
    parts = text.split()
    
    if len(parts) == 4:
        try:
            coords = [float(p) for p in parts]
            if coords[0] < coords[2] and coords[1] < coords[3]:
                return coords
        except:
            pass
    
    if len(parts) == 2:
        try:
            lon, lat = float(parts[0]), float(parts[1])
            return [lon - 0.2, lat - 0.2, lon + 0.2, lat + 0.2]
        except:
            pass
    
    return None

copernicus = CopernicusClient(COPERNICUS_EMAIL, COPERNICUS_PASSWORD)

@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    await message.reply(
        "🌊 *Гидрологический бот*\n\n"
        "Отправьте координаты:\n"
        "• Прямоугольник: `51.5 47.3 52.0 47.8`\n"
        "• Точка: `51.837 47.512`\n\n"
        "Команды:\n"
        "/search координаты - последний месяц\n"
        "/search_full координаты год\n\n"
        "Пример: `/search 51.5 47.3 52.0 47.8`",
        parse_mode=types.ParseMode.MARKDOWN
    )

@dp.message_handler(commands=['search'])
async def search(message: types.Message):
    args = message.text.split()[1:]
    if not args:
        await message.reply("❌ Укажите координаты")
        return
    
    coords = parse_coordinates(" ".join(args))
    if not coords:
        await message.reply("❌ Неверный формат")
        return
    
    end = datetime.now()
    start = end - timedelta(days=30)
    await message.reply(f"🔍 Ищу снимки...")
    await process(message, coords, start, end)

@dp.message_handler(commands=['search_full'])
async def search_full(message: types.Message):
    args = message.text.split()[1:]
    if len(args) < 2:
        await message.reply("❌ Пример: /search_full 51.5 47.3 52.0 47.8 2024")
        return
    
    coords = parse_coordinates(" ".join(args[:-1]))
    year = args[-1]
    
    if not coords:
        await message.reply("❌ Неверный формат координат")
        return
    
    try:
        start = datetime(int(year), 1, 1)
        end = datetime(int(year), 12, 31)
        await message.reply(f"🔍 Ищу снимки за {year}...")
        await process(message, coords, start, end)
    except:
        await message.reply("❌ Неверный год")

@dp.message_handler()
async def handle(message: types.Message):
    coords = parse_coordinates(message.text)
    if coords:
        end = datetime.now()
        start = end - timedelta(days=30)
        await message.reply(f"🔍 Ищу снимки...")
        await process(message, coords, start, end)

async def process(message: types.Message, bbox, start_date, end_date):
    status = await message.reply("🔄 Поиск...")
    
    try:
        products = await copernicus.search_sentinel2(bbox, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        
        if not products:
            await status.edit_text("😔 Снимки не найдены")
            return
        
        await status.edit_text(f"✅ Найдено {len(products)} снимков. Скачиваю...")
        
        with tempfile.TemporaryDirectory() as tmpdir:
            files = []
            for i, p in enumerate(products[:3], 1):
                date = p.get("ContentDate", {}).get("Start", "")[:10]
                await status.edit_text(f"📥 Скачиваю {i}/{len(products[:3])}: {date}")
                
                out = os.path.join(tmpdir, f"{p['Id']}.zip")
                if await copernicus.download_product(p['Id'], out):
                    files.append(out)
                await asyncio.sleep(1)
            
            if not files:
                await status.edit_text("❌ Ошибка скачивания")
                return
            
            archive = os.path.join(tmpdir, "images.zip")
            with zipfile.ZipFile(archive, 'w') as z:
                for f in files:
                    z.write(f, os.path.basename(f))
            
            await status.edit_text("📤 Отправляю...")
            with open(archive, 'rb') as f:
                await bot.send_document(
                    message.chat.id,
                    types.InputFile(f, filename=f"copernicus_{start_date.strftime('%Y%m%d')}.zip"),
                    caption=f"✅ {len(files)} снимков"
                )
            await status.delete()
            
    except Exception as e:
        await status.edit_text(f"❌ {str(e)}")

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        update = types.Update.to_object(request.json)
        dp.process_update(update)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error"}), 500

@app.route('/')
def index():
    return "Bot is running!"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)
   
    @dp.message_handler()
async def echo_all(message: types.Message):
    print(f"Получено сообщение: {message.text}")
    await message.reply("✅ Бот работает!")
