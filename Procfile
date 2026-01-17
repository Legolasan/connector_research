release: cd webapp && python migrate.py upgrade
web: cd webapp && uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
