from fastapi import APIRouter, HTTPException

from app.schemas.entities import RpaExecuteRequest, RpaPromoteRequest, RpaQueueRequest
from app.services import rpa_service

router = APIRouter(tags=["rpa"])


@router.get("/rpa/batches")
def list_batches():
    return rpa_service.list_batches()


@router.get("/rpa/batches/{batch_id}")
def get_batch(batch_id: int):
    try:
        return rpa_service.get_batch(batch_id)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/sources/{source_id}/rpa-queue")
def create_queue(source_id: int, body: RpaQueueRequest):
    try:
        return rpa_service.create_queue(source_id, body.target)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc


@router.post("/rpa/batches/{batch_id}/execute")
def execute(batch_id: int, body: RpaExecuteRequest):
    try:
        return rpa_service.execute_queue(
            batch_id,
            step_id=body.step_id,
            limit=body.limit,
            mode=body.mode,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc


@router.get("/rpa/intake")
def list_intake():
    return rpa_service.list_intake()


@router.post("/rpa/intake/promote")
def promote_intake(body: RpaPromoteRequest):
    try:
        return rpa_service.promote_intake(
            batch_id=body.batch_id,
            intake_id=body.intake_id,
            limit=body.limit,
            mode=body.mode,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc
