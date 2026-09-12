from fastapi import APIRouter, HTTPException, Query

from api.services.earthquake_service import EarthquakeService

router = APIRouter(tags=["earthquakes"])
service = EarthquakeService()

@router.get("/earthquakes/sources")
def list_data_sources() -> dict:
    return {"sources": service.list_available_sources()}

@router.get("/earthquakes/summary")
def get_earthquake_summary(dataset: str = Query(default="processed")) -> dict:
    try:
        return service.get_summary(dataset=dataset)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

@router.get("/earthquakes")
def list_earthquakes(
    dataset: str = Query(default="processed"),
    limit: int | None = Query(default=None, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict:
    try:
        return service.list_earthquakes(dataset=dataset, limit=limit, offset=offset)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/earthquakes/clusters")
def list_cluster_results(
    dataset: str = Query(default="hdbscan"),
    limit: int | None = Query(default=None, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict:
    try:
        return service.list_cluster_results(dataset=dataset, limit=limit, offset=offset)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/earthquakes/model-evaluations")
def list_model_evaluations() -> dict:
    try:
        return service.list_model_evaluations()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/earthquakes/clusters/summary")
def list_cluster_summaries() -> dict:
    try:
        return service.list_cluster_summaries()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

@router.get("/earthquakes/{earthquake_id}")
def get_earthquake(
    earthquake_id: str,
    dataset: str = Query(default="processed"),
) -> dict:
    try:
        earthquake = service.get_earthquake_by_id(earthquake_id, dataset=dataset)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if earthquake is None:
        raise HTTPException(status_code=404, detail="Earthquake not found")

    return earthquake
