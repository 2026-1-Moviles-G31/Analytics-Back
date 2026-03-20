from fastapi import FastAPI
from database import engine, Base
from routes.analytics import router

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Analytics Backend")
app.include_router(router)


