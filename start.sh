#!/bin/bash
# ============================================
# netX CC CHECKER API — RENDER DEPLOY (start.sh)
# FIXED.PY/cc_main.py auto-locate + lowercase copy + gunicorn 8 workers
# Render free/standard: 8 x 250 = 2000 cards in-flight
# ============================================

pip install -U pip setuptools wheel >/dev/null 2>&1

# ── deps: pinned versions (exact same as the working FIXED.PY deploy) ──
cat > requirements.txt << 'EOF'
fastapi==0.141.1
uvicorn==0.52.1
gunicorn==23.0.0
pydantic==2.13.4
aiohttp==3.14.3
curl_cffi==0.16.3
EOF
pip install -r requirements.txt

# ── API file dhundo (case-insensitive, kahin bhi ho — cc_main.py bhi chalega) ──
API_PATH=$(find /opt/render/project/src -maxdepth 3 \( -iname "FIXED.PY" -o -iname "fixed.py" -o -iname "cc_main.py" \) 2>/dev/null | head -1)

if [ -z "$API_PATH" ]; then
  echo "!!! API file repo me NAHI mila (FIXED.PY / cc_main.py) — upload karo !!!"
  exit 1
fi

echo ">>> found: $API_PATH"

# COPY to working-dir with lowercase .py ext (import-safe — .PY uppercase ext
# python3.14 standard-import me nahi jata):
cp "$API_PATH" /opt/render/project/src/cc_main_app.py
cd /opt/render/project/src

# stale bytecode hatao (preload poison fix)
rm -rf __pycache__

echo ">>> launching gunicorn cc_main_app:app (8 workers = 2000 in-flight)"
python -m gunicorn cc_main_app:app -k uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:$PORT \
  --workers 8 \
  --worker-connections 2000 \
  --timeout 180 \
  --graceful-timeout 160 \
  --keep-alive 30 \
  --max-requests 8000 \
  --max-requests-jitter 800 \
  --preload \
  --access-logfile - \
  --error-logfile -
