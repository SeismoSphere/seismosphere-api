from fastapi import APIRouter, HTTPException

from api.services.pipeline_service import PipelineService

router = APIRouter(tags=["pipeline"])
service = PipelineService()

@router.post("/pipeline/trigger")
def trigger_pipeline() -> dict:
    try:
        return service.trigger_dag_run()
    except (ConnectionError, RuntimeError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
