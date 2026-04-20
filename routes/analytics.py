import os
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database import get_db
from models import Event

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


# =======================================================
# KOTLIN ANALYTICS
# =======================================================


# BQ Top 10 crashes
@router.get("/api/kotlin/top-crashes")
def get_top_crashes():

    # Read credentials from environment variables
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not key_path:
        raise HTTPException(
            status_code=500, detail="GOOGLE_APPLICATION_CREDENTIALS not set"
        )

    try:
        # Initialize BigQuery client
        client = bigquery.Client.from_service_account_json(key_path)

        # SQL Query
        query = """
            SELECT
                issue_title AS crash_location,
                issue_subtitle AS error_reason,
                COUNT(*) AS crash_count
            FROM
                `nose-ac2dd.firebase_crashlytics.com_example_tutoring_ANDROID_app_crash_issue_details`
            WHERE
                event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            GROUP BY
                crash_location, error_reason
            ORDER BY
                crash_count DESC
            LIMIT 10
        """

        query_job = client.query(query)
        results = query_job.result()

        # Mapping the results to a json list
        top_crashes = []
        for row in results:
            top_crashes.append(
                {
                    "location": row.crash_location,
                    "reason": row.error_reason,
                    "count": row.crash_count,
                }
            )
        return {"status": "success", "data": top_crashes}
    except GoogleAPIError as e:
        raise HTTPException(
            status_code=500, detail=f"Error connecting to BigQuery: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
