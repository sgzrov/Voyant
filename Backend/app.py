from dotenv import load_dotenv
from fastapi import FastAPI

from Backend.subapps.health_app import router

load_dotenv()

app = FastAPI()
app.include_router(router)
