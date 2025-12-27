import logging
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI

from Backend.subapps.chat_routes import router as chat_router
from Backend.subapps.upload_routes import router as uploads_router


_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env", override=False)

def _configure_logging() -> None:
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


_configure_logging()

app = FastAPI()
app.include_router(chat_router)
app.include_router(uploads_router)
