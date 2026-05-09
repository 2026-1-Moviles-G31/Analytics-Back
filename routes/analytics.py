import os
import json
from datetime import date
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database import get_db
from models import Event

router = APIRouter()


def _get_required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise HTTPException(
            status_code=500,
            detail=f"Missing required environment variable: {name}",
        )
    return value


def _get_project_id_from_credentials_file() -> Optional[str]:
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        return None

    path = Path(credentials_path)
    if not path.is_absolute():
        path = Path.cwd() / path

    if not path.exists():
        return None

    try:
        with path.open("r", encoding="utf-8") as credentials_file:
            payload = json.load(credentials_file)
        return payload.get("project_id")
    except (OSError, json.JSONDecodeError):
        return None


def _get_bigquery_project_id() -> str:
    project = (
        os.environ.get("BIGQUERY_PROJECT_ID")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("GCLOUD_PROJECT")
        or _get_project_id_from_credentials_file()
    )
    if not project:
        raise HTTPException(
            status_code=500,
            detail=(
                "Missing BigQuery project configuration. Set BIGQUERY_PROJECT_ID, "
                "GOOGLE_CLOUD_PROJECT, or GOOGLE_APPLICATION_CREDENTIALS with a "
                "service account JSON that includes project_id."
            ),
        )
    return project


def _get_tutor_conversion_table_ref() -> str:
    project = _get_bigquery_project_id()
    dataset = _get_required_env("BIGQUERY_DATASET")
    table = _get_required_env("BIGQUERY_TUTOR_EVENTS_TABLE")
    return f"`{project}.{dataset}.{table}`"


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


@router.get("/analytics/tutor-conversion")
def get_tutor_conversion(
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
):
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="start_date must be less than or equal to end_date",
        )

    table_ref = _get_tutor_conversion_table_ref()
    project = _get_bigquery_project_id()

    query = f"""
        WITH filtered_events AS (
          SELECT
            tutor_id,
            user_id,
            event_type,
            timestamp
          FROM {table_ref}
          WHERE event_type IN ('view_tutor', 'book_tutor')
            AND tutor_id IS NOT NULL
            AND user_id IS NOT NULL
            AND (@start_date IS NULL OR DATE(timestamp) >= @start_date)
            AND (@end_date IS NULL OR DATE(timestamp) <= @end_date)
        ),
        views_per_tutor AS (
          SELECT
            tutor_id,
            COUNT(DISTINCT user_id) AS views
          FROM filtered_events
          WHERE event_type = 'view_tutor'
          GROUP BY tutor_id
        ),
        bookings_per_tutor AS (
          SELECT
            tutor_id,
            COUNT(DISTINCT user_id) AS bookings
          FROM filtered_events
          WHERE event_type = 'book_tutor'
          GROUP BY tutor_id
        )
        SELECT
          v.tutor_id,
          v.views,
          COALESCE(b.bookings, 0) AS bookings,
          SAFE_DIVIDE(COALESCE(b.bookings, 0), v.views) AS conversion_rate
        FROM views_per_tutor v
        LEFT JOIN bookings_per_tutor b
          ON v.tutor_id = b.tutor_id
        ORDER BY conversion_rate DESC, bookings DESC, views DESC, tutor_id
    """

    try:
        client = bigquery.Client(project=project)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        )

        rows = client.query(query, job_config=job_config).result()
        data = [
            {
                "tutor_id": row.tutor_id,
                "views": row.views,
                "bookings": row.bookings,
                "conversion_rate": row.conversion_rate,
            }
            for row in rows
        ]
        return {"status": "success", "data": data}
    except GoogleAPIError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"BigQuery query failed: {exc}",
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
