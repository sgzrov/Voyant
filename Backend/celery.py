import os
from celery import Celery
from pathlib import Path
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env", override=False)
load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

def make_celery() -> Celery:
    broker = os.getenv("CELERY_BROKER_URL") or os.getenv("REDIS_URL")
    backend = os.getenv("CELERY_RESULT_BACKEND") or broker
    app = Celery("voyant", broker=broker, backend=backend)
    app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        imports=("Backend.background_tasks.csv_ingest",),
        worker_concurrency=1,
        broker_connection_retry_on_startup=True,
    )
    return app

celery = make_celery()



