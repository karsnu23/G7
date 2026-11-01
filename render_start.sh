#!/bin/bash
# ============================================
# SHOPI_API_V2 — RENDER START (auto-locate + $PORT bind)
# Render-style: repo root me files, PORT env from platform, granian>gunicorn>uvicorn.
# Includes the FacebookPay GraphQL fix (Shopify killed that type).
# ============================================

pip install -U pip setuptools wheel >/dev/null 2>&1
pip install aiohttp curl_cffi fastapi uvicorn requests phonenumbers psutil orjson aiosqlite 2>&1 | tail -1

# shopi_api_v2.py + engine/ locate (repo root or one level deep)
BASE=$(dirname "$(find "$(pwd)" -maxdepth 2 -name "shopi_api_v2.py" 2>/dev/null | head -1)")
if [ -z "$BASE" ] || [ ! -d "$BASE/engine" ]; then
  echo "!!! shopi_api_v2.py / engine/ repo me NAHI mila — zip upload karo !!!"
  exit 1
fi
echo ">>> found: $BASE"
cd "$BASE"

# import verify (no cache poison)
rm -rf engine/__pycache__ __pycache__
if ! python -c "from engine import server" 2>/dev/null; then
  echo "!!! IMPORT FAIL:"
  python -c "from engine import server" 2>&1 | tail -5
  exit 1
fi
echo ">>> IMPORT OK"

# engine pick: granian > gunicorn+uvicorn-worker > uvicorn (same ladder as original)
PORT="${PORT:-5000}"
WORKERS=$(python -c "from engine.system_tuner import get_hardware_profile; print(get_hardware_profile()['granian_workers'])" 2>/dev/null || echo 2)
THREADS=$(python -c "from engine.system_tuner import get_hardware_profile; print(get_hardware_profile()['granian_threads'])" 2>/dev/null || echo 2)
echo ">>> tuned: workers=$WORKERS threads=$THREADS port=$PORT"

if python -c "import granian" >/dev/null 2>&1; then
  echo ">>> engine: granian"
  exec python -m granian --interface asgi --workers "$WORKERS" --threads "$THREADS" \
    --host 0.0.0.0 --port "$PORT" shopi_api_v2:app
elif python -c "import gunicorn" >/dev/null 2>&1; then
  echo ">>> engine: gunicorn+uvicorn workers"
  exec python -m gunicorn -k uvicorn.workers.UvicornWorker \
    -w "$WORKERS" -b 0.0.0.0:"$PORT" --timeout 180 --graceful-timeout 160 \
    --max-requests 8000 --max-requests-jitter 800 shopi_api_v2:app
else
  echo ">>> engine: uvicorn fallback"
  exec python -c "import uvicorn; uvicorn.run('shopi_api_v2:app', host='0.0.0.0', port=int('${PORT}'))"
fi
