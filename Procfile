release: cd webapp && python migrate.py upgrade
web: playwright install chromium && cd webapp && uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
