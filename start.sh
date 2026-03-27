#!/bin/bash
gunicorn bot:app --timeout 300 --bind 0.0.0.0:10000
