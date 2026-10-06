"""Re-run a scene's per-scene parent Prefect deployment with its original parameters."""

import copy
from typing import Any

import httpx
from loguru import logger
from sqlalchemy import Connection

from c_core_health import prefect_api
from c_core_health.errors import ConflictError
from c_core_health.lineage import (
    SERVICE_TAG,
    batch_tag,
    parameters_mention,
    resolve_lineage,
    scene_tag,
)
from c_core_health.models import (
    Lineage,
    PlannedRun,
    RerunRequest,
    RerunResponse,
    RunNode,
    SceneRef,
)
from c_core_health.settings import Settings


def build_rerun_parameters(
    original: dict[str, Any], declared: set[str], overrides: dict[str, Any]
) -> dict[str, Any]:
    """Original parameters plus overrides, but only overrides the deployment declares.

    Sending an undeclared parameter makes Prefect reject the run, so e.g. ``force`` is
    only set for flows that accept it.
    """
    parameters = copy.deepcopy(original)
    parameters.update({k: v for k, v in overrides.items() if k in declared})
    return parameters


def select_sources(
    lineage: Lineage, ref: SceneRef, request: RerunRequest
) -> list[RunNode]:
    if lineage.parent is None:
        raise ConflictError(
            "No per-scene parent flow run found for this scene",
            roots=[f"{r.flow_name}/{r.name}" for r in lineage.roots],
            unmatched_events=[
                f"{e.flow_name}/{e.run_name}" for e in lineage.unmatched_events
            ],
        )
    if lineage.active_run_ids:
        raise ConflictError(
            "Scene still has active flow runs; wait for them or cancel them in Prefect",
            active_run_ids=[str(i) for i in lineage.active_run_ids],
        )
    sources = [lineage.parent]
    if request.include_sibling_roots:
        sources.extend(lineage.sibling_roots)
    for source in sources:
        if not parameters_mention(source.parameters, ref.scene_id):
            raise ConflictError(
                f"Run {source.flow_name}/{source.name} does not reference the scene in "
                "its parameters; it may be a multi-scene batch flow and is not re-run",
                run_id=str(source.id),
            )
        if source.deployment_id is None:
            raise ConflictError(
                f"Run {source.flow_name}/{source.name} was not started from a deployment",
                run_id=str(source.id),
            )
    return sources


def plan_rerun(
    client: httpx.Client,
    settings: Settings,
    ref: SceneRef,
    source: RunNode,
    batch_key: str,
) -> PlannedRun:
    assert source.deployment_id is not None
    deployment = prefect_api.read_deployment(client, source.deployment_id)
    return PlannedRun(
        source_run_id=source.id,
        source_run_name=source.name,
        flow_name=source.flow_name,
        deployment_id=deployment.id,
        deployment_name=deployment.name,
        parameters=build_rerun_parameters(
            source.parameters,
            deployment.declared_parameters,
            settings.rerun_parameter_overrides,
        ),
        tags=[
            SERVICE_TAG,
            scene_tag(ref),
            batch_tag(batch_key),
            f"rerun-of:{source.name}",
        ],
        idempotency_key=f"{SERVICE_TAG}:rerun:{source.id}",
    )


def rerun_scene(
    conn: Connection,
    client: httpx.Client,
    settings: Settings,
    ref: SceneRef,
    request: RerunRequest,
) -> RerunResponse:
    lineage = resolve_lineage(conn, client, settings, ref)
    sources = select_sources(lineage, ref, request)
    batch_key = str(sources[0].id)
    planned = [plan_rerun(client, settings, ref, s, batch_key) for s in sources]
    if request.dry_run:
        return RerunResponse(scene=ref, dry_run=True, runs=planned)

    created: list[PlannedRun] = []
    for plan in planned:
        run = prefect_api.create_flow_run_from_deployment(
            client,
            plan.deployment_id,
            parameters=plan.parameters,
            tags=plan.tags,
            idempotency_key=plan.idempotency_key,
        )
        logger.bind(audit=True).info(
            "rerun scene={} collection={} flow={} source_run={} new_run={} ({})",
            ref.scene_id,
            ref.collection,
            plan.flow_name,
            plan.source_run_id,
            run.id,
            run.name,
        )
        created.append(
            plan.model_copy(
                update={
                    "created_run_id": run.id,
                    "created_run_name": run.name,
                    "created_state_type": run.state_type,
                }
            )
        )
    return RerunResponse(scene=ref, dry_run=False, runs=created)
