#!/bin/bash
pip install -U pip setuptools wheel >/dev/null
pip install -r requirements.txt
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
