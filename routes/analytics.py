from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from database import get_db
from models import Event
from typing import List
from pydantic import BaseModel, Field
from typing import Optional

router = APIRouter()

# --- Schema for incoming events ---
class EventIn(BaseModel):
    event: str
    page: str
    feature: Optional[str] = None
    session: str
    type: str
    userID: str
    milliseconds_spent: int
    timestamp: int
    recovered: bool = False

class EventBatch(BaseModel):
    events: List[EventIn]

# --- POST /analytics/events (receives from Flutter app) ---
@router.post("/analytics/events")
def receive_events(batch: EventBatch, db: Session = Depends(get_db)):
    for e in batch.events:
        db_event = Event(**e.dict())
        db.add(db_event)
    db.commit()
    return {"status": "ok", "received": len(batch.events)}

# --- GET /analytics/events (sends to your frontend) ---
@router.get("/analytics/events")
def get_events(db: Session = Depends(get_db)):
    events = db.query(Event).order_by(Event.timestamp.desc()).limit(1000).all()
    return events