release: cd webapp && python migrate.py upgrade && playwright install chromium
web: cd webapp && uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
worker: cd webapp && celery -A services.celery_app worker --loglevel=info --concurrency=4