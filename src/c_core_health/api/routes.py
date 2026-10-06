from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query, Response, status
from sqlalchemy import text

from c_core_health import prefect_api, purge, rerun, scenes
from c_core_health.api.deps import (
    ApiKeyHeader,
    ConnectionDep,
    PrefectDep,
    ResourcesDep,
    SettingsDep,
    authorize_mutation,
)
from c_core_health.lineage import resolve_lineage
from c_core_health.models import (
    Lineage,
    PurgePreview,
    PurgeRequest,
    PurgeResult,
    RerunRequest,
    RerunResponse,
    SceneDetail,
    ScenePage,
    Sensor,
)

router = APIRouter()

CollectionQuery = Annotated[
    str | None,
    Query(description="Required when the scene id exists in several SAR collections"),
]


@router.get("/healthz", tags=["health"])
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/readyz", tags=["health"])
def readyz(conn: ConnectionDep, prefect: PrefectDep, response: Response) -> dict[str, Any]:
    checks: dict[str, str] = {}
    try:
        conn.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as exc:
        checks["database"] = f"error: {type(exc).__name__}"
    try:
        checks["prefect"] = f"ok ({prefect_api.read_version(prefect)})"
    except Exception as exc:
        checks["prefect"] = f"error: {type(exc).__name__}"
    ready = all(v.startswith("ok") for v in checks.values())
    if not ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ok" if ready else "unavailable", "checks": checks}


@router.get("/scenes", response_model=ScenePage, tags=["scenes"])
def list_scenes(
    conn: ConnectionDep,
    settings: SettingsDep,
    since: Annotated[
        datetime | None,
        Query(description="Acquired at or after; defaults to CCH_DEFAULT_SINCE_DAYS ago"),
    ] = None,
    until: Annotated[datetime | None, Query(description="Acquired before")] = None,
    collection: str | None = None,
    sensor: Sensor | None = None,
    limit: Annotated[int, Query(ge=1)] = 50,
    cursor: str | None = None,
) -> ScenePage:
    since = since or datetime.now(UTC) - timedelta(days=settings.default_since_days)
    try:
        refs, next_cursor = scenes.list_scene_refs(
            conn,
            settings,
            since=since,
            until=until,
            collection=collection,
            sensor=sensor,
            limit=min(limit, settings.max_page_size),
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ScenePage(items=scenes.summarize_scenes(conn, refs), next_cursor=next_cursor)


@router.get("/scenes/{scene_id}", response_model=SceneDetail, tags=["scenes"])
def get_scene(
    scene_id: str,
    conn: ConnectionDep,
    settings: SettingsDep,
    prefect: PrefectDep,
    collection: CollectionQuery = None,
) -> SceneDetail:
    ref = scenes.resolve_scene(conn, settings, scene_id, collection)
    [summary] = scenes.summarize_scenes(conn, [ref])
    lineage = resolve_lineage(conn, prefect, settings, ref)
    return SceneDetail(**summary.model_dump(), lineage=lineage)


@router.get("/scenes/{scene_id}/lineage", response_model=Lineage, tags=["scenes"])
def get_lineage(
    scene_id: str,
    conn: ConnectionDep,
    settings: SettingsDep,
    prefect: PrefectDep,
    collection: CollectionQuery = None,
) -> Lineage:
    ref = scenes.resolve_scene(conn, settings, scene_id, collection)
    return resolve_lineage(conn, prefect, settings, ref)


@router.post("/scenes/{scene_id}/rerun", response_model=RerunResponse, tags=["actions"])
def rerun_scene(
    scene_id: str,
    body: RerunRequest,
    conn: ConnectionDep,
    resources: ResourcesDep,
    prefect: PrefectDep,
    api_key: ApiKeyHeader = None,
    collection: CollectionQuery = None,
) -> RerunResponse:
    """Re-run the scene's per-scene parent deployment. ``dry_run`` defaults to true."""
    if not body.dry_run:
        authorize_mutation(resources, api_key)
    ref = scenes.resolve_scene(conn, resources.settings, scene_id, collection)
    return rerun.rerun_scene(conn, prefect, resources.settings, ref, body)


@router.get(
    "/scenes/{scene_id}/purge-preview", response_model=PurgePreview, tags=["actions"]
)
def purge_preview(
    scene_id: str,
    conn: ConnectionDep,
    settings: SettingsDep,
    prefect: PrefectDep,
    collection: CollectionQuery = None,
    include_ais_messages: bool = False,
    allow_analyst_data_loss: bool = False,
) -> PurgePreview:
    ref = scenes.resolve_scene(conn, settings, scene_id, collection)
    lineage = resolve_lineage(conn, prefect, settings, ref)
    return purge.build_preview(
        conn,
        ref,
        include_ais_messages=include_ais_messages,
        allow_analyst_data_loss=allow_analyst_data_loss,
        active_run_ids=lineage.active_run_ids,
    )


@router.post("/scenes/{scene_id}/purge", response_model=PurgeResult, tags=["actions"])
def purge_scene(
    scene_id: str,
    body: PurgeRequest,
    conn: ConnectionDep,
    resources: ResourcesDep,
    prefect: PrefectDep,
    api_key: ApiKeyHeader = None,
    collection: CollectionQuery = None,
) -> PurgeResult:
    """Delete derived rows for the scene. ``dry_run`` defaults to true."""
    write_engine = None if body.dry_run else authorize_mutation(resources, api_key)
    settings = resources.settings
    ref = scenes.resolve_scene(conn, settings, scene_id, collection)
    purge.check_confirmation(ref, body)
    lineage = resolve_lineage(conn, prefect, settings, ref)
    if write_engine is None:
        preview = purge.build_preview(
            conn,
            ref,
            include_ais_messages=body.include_ais_messages,
            allow_analyst_data_loss=body.allow_analyst_data_loss,
            active_run_ids=lineage.active_run_ids,
        )
        return PurgeResult(
            preview=preview, dry_run=True, executed=False, backup_path=None, deleted={}
        )
    return purge.execute_purge(
        write_engine,
        settings.backup_dir,
        ref,
        body,
        active_run_ids=lineage.active_run_ids,
    )
