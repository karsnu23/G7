#!/bin/bash
# Render build script — install deps for shopi_api_v2
pip install -U pip setuptools wheel
pip install aiohttp curl_cffi fastapi uvicorn requests phonenumbers psutil orjson aiosqlite
