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
    try:
        # Initialize BigQuery client
        client = bigquery.Client()

        # SQL Query
        query = """
            SELECT
                JSON_VALUE(data, '$.screen_name') AS screen_name,
                JSON_VALUE(data, '$.error_type') AS error_type,
                COUNT(*) AS count
            FROM
                `nose-ac2dd.app_analytics.crashes_raw_raw_changelog`
            WHERE
                operation = 'CREATE'
                AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            GROUP BY
                1, 2
            ORDER BY
                count DESC
            LIMIT 10
        """

        query_job = client.query(query)
        top_crashes = [
            {
                "location": row.screen_name,
                "reason": row.error_type,
                "count": row.count,
            }
            for row in query_job.result()
        ]
        return {"status": "success", "data": top_crashes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# BQ Feature Time Spent
@router.get("/api/kotlin/feature-time-spent")
def get_feature_time_spent():
    try:
        # Initialize BigQuery client
        client = bigquery.Client()

        # SQL Query
        query = """
            SELECT
                JSON_VALUE(data, '$.feature_name') AS feature,
                ROUND(SUM(CAST(JSON_VALUE(data, '$.duration_ms') AS FLOAT64)) / 60000, 1) AS minutes
            FROM
                `nose-ac2dd.app_analytics.feature_usage_raw_changelog`
            WHERE
                operation = 'CREATE'
                -- FILTRO CLAVE: Solo traer lo que tenga nombre
                AND JSON_VALUE(data, '$.feature_name') IS NOT NULL
                AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            GROUP BY 1
            ORDER BY minutes DESC
            LIMIT 10
        """

        query_job = client.query(query)
        top_features = [
            {
                "feature": row.feature,
                "minutes": row.minutes,
            }
            for row in query_job.result()
            if row.feature is not None
        ]
        return {"status": "success", "data": top_features}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
