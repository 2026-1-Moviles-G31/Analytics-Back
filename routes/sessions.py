from fastapi import APIRouter, Query
from pydantic import BaseModel

from services.firestore_sessions import get_latest_completed_session_summary

router = APIRouter(tags=["sessions"])


class LastCompletedSessionResponse(BaseModel):
    daysSinceLastSession: int
    lastSubject: str
    lastSessionDate: str


@router.get(
    "/sessions/last-completed",
    response_model=LastCompletedSessionResponse,
)
def get_last_completed_session(
    student_id: str = Query(..., min_length=1),
):
    return get_latest_completed_session_summary(student_id)
