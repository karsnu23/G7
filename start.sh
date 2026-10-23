#!/bin/bash
# Render start — FIXED.PY auto-locate (repo me kahin bhi ho, pakad lega)
pip install -U pip setuptools wheel >/dev/null 2>&1
pip install -r requirements.txt

# FIXED.PY dhundo (repo-root ya kisi bhi subfolder me):
FIXED_PATH=$(find /opt/render/project/src -maxdepth 3 -name "FIXED.PY" 2>/dev/null | head -1)
if [ -z "$FIXED_PATH" ]; then
  FIXED_PATH=$(find /opt/render/project/src -maxdepth 3 -iname "fixed.py" 2>/dev/null | head -1)
fi

if [ -z "$FIXED_PATH" ]; then
  echo "!!! FIXED.PY repo me NAHI mila — upload karo repo-ROOT me !!!"
  exit 1
fi

APP_DIR=$(dirname "$FIXED_PATH")
echo ">>> FIXED.PY found at: $FIXED_PATH"
echo ">>> cd $APP_DIR"
cd "$APP_DIR"

python -m gunicorn FIXED:app -k uvicorn.workers.UvicornWorker \
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
