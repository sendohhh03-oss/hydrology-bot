import os
import logging
import asyncio
import zipfile
import tempfile
import json
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, Document
from aiogram.filters import Command
from aiogram.enums import ParseMode

# Конфигурация
BOT_TOKEN = "8711360999:AAHAmamDv2TCdGyHtWE8XfTP67wO-CkEGH8"  # Вставьте ваш токен
CLIENT_ID = "sh-ceb5f1fe-334a-4e25-aee6-5ee82d1633c7"  # Вставьте Client ID
CLIENT_SECRET = "FzJ4U90CQWUjFev3aFuIYMr37mYsuIPn"  # Вставьте Client Secret

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

class CopernicusClient:
    """Клиент для работы с Copernicus Data Space Ecosystem"""
    
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expiry = None
        
    async def get_token(self):
        """Получение OAuth2 токена"""
        if self.access_token and self.token_expiry and self.token_expiry > datetime.now():
            return self.access_token
            
        auth_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        
        # Используем публичный клиент для получения токена
        auth_data = {
            "client_id": "cdse-public",
            "grant_type": "password",
            "username": "sendohhh.03@gmail.com",  # Вставьте ваш email от Copernicus
            "password": "IZIZ02iziz02!"  # Вставьте ваш пароль от Copernicus
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(auth_url, data=auth_data) as response:
                if response.status == 200:
                    data = await response.json()
                    self.access_token = data["access_token"]
                    self.token_expiry = datetime.now() + timedelta(seconds=data["expires_in"] - 60)
                    return self.access_token
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка авторизации: {error_text}")
                    raise Exception(f"Ошибка авторизации: {error_text}")
    
    async def search_sentinel2(self, bbox, start_date, end_date, max_cloud_cover=30, max_items=10):
        """
        Поиск снимков Sentinel-2 по области и датам
        bbox: [min_lon, min_lat, max_lon, max_lat]
        """
        token = await self.get_token()
        
        # Формируем WKT полигон из bbox
        polygon_wkt = f"POLYGON(({bbox[0]} {bbox[1]}, {bbox[2]} {bbox[1]}, {bbox[2]} {bbox[3]}, {bbox[0]} {bbox[3]}, {bbox[0]} {bbox[1]}))"
        
        # Формируем OData запрос (исправленный синтаксис)
        query_filter = (
            f"Collection/Name eq 'SENTINEL-2' and "
            f"OData.CSC.Intersects(area=geography'SRID=4326;{polygon_wkt}') and "
            f"ContentDate/Start gt {start_date}T00:00:00.000Z and "
            f"ContentDate/Start lt {end_date}T23:59:59.999Z"
        )
        
        # Добавляем фильтр по облачности
        if max_cloud_cover:
            query_filter += f" and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/Value le {max_cloud_cover})"
        
        params = {
            "$filter": query_filter,
            "$top": max_items,
            "$orderby": "ContentDate/Start desc",
            "$expand": "Attributes"
        }
        
        headers = {"Authorization": f"Bearer {token}"}
        
        # Используем правильный URL для каталога
        odata_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(odata_url, params=params, headers=headers, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        products = data.get("value", [])
                        logger.info(f"Найдено продуктов: {len(products)}")
                        return products
                    else:
                        error_text = await response.text()
                        logger.error(f"Ошибка поиска: {error_text}")
                        return []
            except Exception as e:
                logger.error(f"Исключение при поиске: {e}")
                return []
    
    async def download_product(self, product_id, output_path):
        """Скачивание продукта по ID"""
        token = await self.get_token()
        
        # Формируем URL для скачивания
        download_url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products('{product_id}')/$value"
        headers = {"Authorization": f"Bearer {token}"}
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(download_url, headers=headers, timeout=300) as response:
                    if response.status == 200:
                        with open(output_path, 'wb') as f:
                            while True:
                                chunk = await response.content.read(8192)
                                if not chunk:
                                    break
                                f.write(chunk)
                        return True
                    else:
                        logger.error(f"Ошибка скачивания {product_id}: {response.status}")
                        return False
            except Exception as e:
                logger.error(f"Исключение при скачивании: {e}")
                return False

# Инициализация клиента Copernicus
copernicus = CopernicusClient(CLIENT_ID, CLIENT_SECRET)

def parse_coordinates(text):
    """Парсинг координат из сообщения"""
    import re
    
    # Убираем лишние пробелы
    text = text.strip()
    
    # Формат 1: десятичные через запятую или пробел
    try:
        parts = text.replace(',', ' ').split()
        if len(parts) == 4:
            coords = [float(p) for p in parts]
            if coords[0] < coords[2] and coords[1] < coords[3]:
                return coords
    except:
        pass
    
    # Формат 2: одна точка с градусами, минутами, секундами
    try:
        def dms_to_decimal(dms_string):
            pattern = r'(\d+)°(\d+)\'([\d,]+)\"([NSEW])'
            match = re.search(pattern, dms_string.replace(',', '.'))
            if match:
                degrees = int(match.group(1))
                minutes = int(match.group(2))
                seconds = float(match.group(3))
                direction = match.group(4)
                decimal = degrees + minutes/60 + seconds/3600
                if direction in ['S', 'W']:
                    decimal = -decimal
                return decimal
            return None
        
        coords_parts = text.split()
        if len(coords_parts) == 2:
            lon = dms_to_decimal(coords_parts[0])
            lat = dms_to_decimal(coords_parts[1])
            if lon is not None and lat is not None:
                # Создаем область 0.2° вокруг точки (около 20 км)
                return [lon - 0.2, lat - 0.2, lon + 0.2, lat + 0.2]
    except:
        pass
    
    # Формат 3: одна точка в десятичных
    try:
        parts = text.replace(',', ' ').split()
        if len(parts) == 2:
            lon = float(parts[0])
            lat = float(parts[1])
            return [lon - 0.2, lat - 0.2, lon + 0.2, lat + 0.2]
    except:
        pass
    
    return None

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "🌊 *Гидрологический бот для Copernicus*\n\n"
        "Я помогу вам скачать спутниковые снимки Sentinel-2.\n\n"
        "📌 *Формат координат:*\n"
        "• Прямоугольник: `51.5,47.3,52.0,47.8`\n"
        "• Точка: `51.837331,47.512254`\n"
        "• Градусы: `51°50'14,393\"E 47°30'44,114\"N`\n\n"
        "📅 *Команды:*\n"
        "/search [координаты] - поиск за последний месяц\n"
        "/search_full [координаты] [год] - поиск за указанный год\n"
        "Или просто отправьте координаты",
        parse_mode=ParseMode.MARKDOWN
    )

@dp.message(Command("search"))
async def cmd_search(message: Message):
    args = message.text.split()[1:] if len(message.text.split()) > 1 else []
    if not args:
        await message.answer("❌ Укажите координаты. Пример: /search 51.5,47.3,52.0,47.8")
        return
    
    coords_text = " ".join(args)
    coords = parse_coordinates(coords_text)
    
    if not coords:
        await message.answer("❌ Неверный формат координат")
        return
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    await message.answer(f"🔍 Ищу снимки за период {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}...")
    await search_and_send(message, coords, start_date, end_date)

@dp.message(Command("search_full"))
async def cmd_search_full(message: Message):
    args = message.text.split()[1:] if len(message.text.split()) > 1 else []
    if len(args) < 2:
        await message.answer("❌ Укажите координаты и год. Пример: /search_full 51.5,47.3,52.0,47.8 2023")
        return
    
    coords_text = " ".join(args[:-1])
    year = args[-1]
    
    coords = parse_coordinates(coords_text)
    if not coords:
        await message.answer("❌ Неверный формат координат")
        return
    
    try:
        start_date = datetime(int(year), 1, 1)
        end_date = datetime(int(year), 12, 31)
    except:
        await message.answer("❌ Неверный формат года")
        return
    
    await message.answer(f"🔍 Ищу снимки за {year} год...")
    await search_and_send(message, coords, start_date, end_date)

@dp.message()
async def handle_coordinates(message: Message):
    coords = parse_coordinates(message.text)
    if coords:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        await message.answer(f"🔍 Ищу снимки за последний месяц...")
        await search_and_send(message, coords, start_date, end_date)

async def search_and_send(message: Message, bbox, start_date, end_date):
    status_msg = await message.answer("🔄 Выполняется поиск снимков...")
    
    try:
        products = await copernicus.search_sentinel2(
            bbox=bbox,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            max_cloud_cover=30
        )
        
        if not products:
            await status_msg.edit_text(
                "😔 Снимки не найдены.\n\n"
                "Возможные причины:\n"
                "• В этой области нет подходящих снимков\n"
                "• Период поиска слишком короткий\n"
                "• Облачность выше 30%\n\n"
                "Попробуйте:\n"
                "• Увеличить область поиска\n"
                "• Расширить временной промежуток\n"
                "• Использовать команду /search_full [координаты] 2024"
            )
            return
        
        await status_msg.edit_text(f"✅ Найдено {len(products)} снимков. Начинаю скачивание...")
        
        with tempfile.TemporaryDirectory() as tmpdir:
            downloaded_files = []
            
            for i, product in enumerate(products, 1):
                product_id = product["Id"]
                cloud_cover = "N/A"
                for attr in product.get("Attributes", []):
                    if attr.get("Name") == "cloudCover":
                        cloud_cover = attr.get("Value", "N/A")
                        break
                
                date = product.get("ContentDate", {}).get("Start", "Unknown")[:10]
                
                await status_msg.edit_text(f"📥 Скачиваю {i}/{len(products)}: {date} (облачность: {cloud_cover}%)")
                
                output_file = os.path.join(tmpdir, f"{product_id}.zip")
                success = await copernicus.download_product(product_id, output_file)
                
                if success:
                    downloaded_files.append(output_file)
                await asyncio.sleep(1)
            
            if not downloaded_files:
                await status_msg.edit_text("❌ Не удалось скачать ни одного снимка")
                return
            
            archive_path = os.path.join(tmpdir, "copernicus_images.zip")
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in downloaded_files:
                    zipf.write(file_path, os.path.basename(file_path))
            
            await status_msg.edit_text("📤 Отправляю архив...")
            
            with open(archive_path, 'rb') as f:
                await message.answer_document(
                    types.BufferedInputFile(
                        f.read(),
                        filename=f"copernicus_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.zip"
                    ),
                    caption=f"🌍 Снимки Sentinel-2\n"
                            f"📅 Период: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}\n"
                            f"📊 Всего файлов: {len(downloaded_files)}\n"
                            f"☁️ Облачность: до 30%\n"
                            f"📍 Область: {bbox}"
                )
            
            await status_msg.delete()
            
    except Exception as e:
        logger.exception("Ошибка при обработке запроса")
        await status_msg.edit_text(f"❌ Произошла ошибка: {str(e)}")

async def main():
    logger.info("Запуск бота...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())