import json
import os
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from fastapi import HTTPException
from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter


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


def _get_firestore_project_id() -> Optional[str]:
    return (
        os.environ.get("FIRESTORE_PROJECT_ID")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("GCLOUD_PROJECT")
        or os.environ.get("BIGQUERY_PROJECT_ID")
        or _get_project_id_from_credentials_file()
    )


@lru_cache(maxsize=1)
def get_firestore_client() -> firestore.Client:
    project = _get_firestore_project_id()

    try:
        if project:
            return firestore.Client(project=project)
        return firestore.Client()
    except DefaultCredentialsError as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "Firestore credentials are not configured correctly. "
                "Set GOOGLE_APPLICATION_CREDENTIALS or FIRESTORE_PROJECT_ID."
            ),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Unable to initialize Firestore client: {exc}",
        ) from exc


def _parse_session_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise HTTPException(
                status_code=500,
                detail="Latest completed session has an empty session_date value.",
            )

        iso_candidate = normalized.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(iso_candidate).date()
        except ValueError:
            pass

        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(normalized, fmt).date()
            except ValueError:
                continue

    raise HTTPException(
        status_code=500,
        detail="Latest completed session has an unsupported session_date format.",
    )


def get_latest_completed_session_summary(student_id: str) -> dict[str, Any]:
    client = get_firestore_client()

    try:
        query = (
            client.collection("sessions")
            .where(filter=FieldFilter("student_id", "==", student_id))
            .where(filter=FieldFilter("status", "==", "completed"))
            .order_by("session_date", direction=firestore.Query.DESCENDING)
            .limit(1)
        )
        snapshots = list(query.stream())
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Firestore query failed: {exc}",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected Firestore error: {exc}",
        ) from exc

    if not snapshots:
        raise HTTPException(
            status_code=404,
            detail="No completed sessions found for this student.",
        )

    session_data = snapshots[0].to_dict() or {}
    raw_session_date = session_data.get("session_date")
    if raw_session_date is None:
        raise HTTPException(
            status_code=500,
            detail="Latest completed session is missing session_date.",
        )

    session_date = _parse_session_date(raw_session_date)
    today = datetime.now(timezone.utc).date()
    days_since_last_session = (today - session_date).days

    return {
        "daysSinceLastSession": days_since_last_session,
        "lastSubject": str(session_data.get("subject") or ""),
        "lastSessionDate": session_date.isoformat(),
    }
