#!/bin/bash
# Render start — FIXED.PY auto-locate + lower-ext copy (import-safe)
pip install -U pip setuptools wheel >/dev/null 2>&1
pip install -r requirements.txt

# FIXED.PY dhundo (case-insensitive, kahin bhi ho):
FIXED_PATH=$(find /opt/render/project/src -maxdepth 3 \( -iname "FIXED.PY" -o -iname "fixed.py" \) 2>/dev/null | head -1)

if [ -z "$FIXED_PATH" ]; then
  echo "!!! FIXED.PY repo me NAHI mila — upload karo !!!"
  exit 1
fi

echo ">>> found: $FIXED_PATH"

# COPY to working-dir with lowercase .py ext (import-safe — .PY uppercase ext
# python3.14 standard-import me nahi jata):
cp "$FIXED_PATH" /opt/render/project/src/fixed_app.py
cd /opt/render/project/src

echo ">>> launching gunicorn fixed_app:app"
python -m gunicorn fixed_app:app -k uvicorn.workers.UvicornWorker \
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
