import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

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
    )
    return app


celery = make_celery()



