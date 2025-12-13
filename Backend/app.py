import logging
import os
from dotenv import load_dotenv
from fastapi import FastAPI

from Backend.subapps.chat.routes import router as chat_router
from Backend.subapps.health.routes import router as health_router

load_dotenv()

def _configure_logging() -> None:
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    try:
        log_level_name = os.getenv("VOYANT_LOG_LEVEL", "INFO").upper()
        log_level = getattr(logging, log_level_name, logging.INFO)
    except Exception:
        log_level = logging.INFO

    root_logger.setLevel(log_level)
    root_handler = logging.StreamHandler()
    root_handler.setLevel(log_level)
    root_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    root_logger.addHandler(root_handler)


_configure_logging()

app = FastAPI()
app.include_router(health_router)
app.include_router(chat_router)
