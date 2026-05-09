from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from database import Base, engine
from routes.analytics import router
from routes.sessions import router as sessions_router

load_dotenv()

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Analytics Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
app.include_router(sessions_router)
