"""Thin typed wrapper over the Prefect 3 server REST API."""

from datetime import datetime
from typing import Any
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, TypeAdapter

from c_core_health.settings import Settings

PAGE_SIZE = 200


class PrefectModel(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)


class FlowRun(PrefectModel):
    id: UUID
    name: str
    flow_id: UUID
    deployment_id: UUID | None = None
    parent_task_run_id: UUID | None = None
    state_type: str | None = None
    state_name: str | None = None
    tags: list[str] = []
    parameters: dict[str, Any] = {}
    created: datetime
    start_time: datetime | None = None
    end_time: datetime | None = None
    idempotency_key: str | None = None


class TaskRun(PrefectModel):
    id: UUID
    name: str
    flow_run_id: UUID | None = None


class Flow(PrefectModel):
    id: UUID
    name: str


class Deployment(PrefectModel):
    id: UUID
    name: str
    flow_id: UUID
    paused: bool = False
    parameter_openapi_schema: dict[str, Any] | None = None

    @property
    def declared_parameters(self) -> set[str]:
        properties: dict[str, Any] = (self.parameter_openapi_schema or {}).get(
            "properties"
        ) or {}
        return set(properties)


_flow_runs = TypeAdapter(list[FlowRun])
_flows = TypeAdapter(list[Flow])
_task_runs = TypeAdapter(list[TaskRun])


def make_client(settings: Settings) -> httpx.Client:
    headers = {"Content-Type": "application/json"}
    if settings.prefect_api_key is not None:
        headers["Authorization"] = f"Bearer {settings.prefect_api_key.get_secret_value()}"
    return httpx.Client(
        base_url=settings.prefect_api_url.rstrip("/"),
        headers=headers,
        timeout=settings.prefect_timeout_s,
    )


def read_version(client: httpx.Client) -> str:
    response = client.get("/admin/version")
    response.raise_for_status()
    return str(response.json())


def filter_flow_runs(
    client: httpx.Client, criteria: dict[str, Any], *, max_results: int = 1000
) -> list[FlowRun]:
    """POST /flow_runs/filter, following offset pagination up to ``max_results``."""
    runs: list[FlowRun] = []
    while len(runs) < max_results:
        limit = min(PAGE_SIZE, max_results - len(runs))
        body = {**criteria, "limit": limit, "offset": len(runs), "sort": "START_TIME_DESC"}
        response = client.post("/flow_runs/filter", json=body)
        response.raise_for_status()
        page = _flow_runs.validate_python(response.json())
        runs.extend(page)
        if len(page) < limit:
            break
    return runs


def read_flow_run(client: httpx.Client, flow_run_id: UUID) -> FlowRun:
    response = client.get(f"/flow_runs/{flow_run_id}")
    response.raise_for_status()
    return FlowRun.model_validate(response.json())


def read_task_run(client: httpx.Client, task_run_id: UUID) -> TaskRun:
    response = client.get(f"/task_runs/{task_run_id}")
    response.raise_for_status()
    return TaskRun.model_validate(response.json())


def read_task_runs(client: httpx.Client, task_run_ids: set[UUID]) -> list[TaskRun]:
    if not task_run_ids:
        return []
    ids = sorted(str(i) for i in task_run_ids)
    task_runs: list[TaskRun] = []
    for start in range(0, len(ids), PAGE_SIZE):
        chunk = ids[start : start + PAGE_SIZE]
        body = {"task_runs": {"id": {"any_": chunk}}, "limit": PAGE_SIZE}
        response = client.post("/task_runs/filter", json=body)
        response.raise_for_status()
        task_runs.extend(_task_runs.validate_python(response.json()))
    return task_runs


def read_flow_names(client: httpx.Client, flow_ids: set[UUID]) -> dict[UUID, str]:
    if not flow_ids:
        return {}
    body = {"flows": {"id": {"any_": [str(i) for i in flow_ids]}}, "limit": PAGE_SIZE}
    response = client.post("/flows/filter", json=body)
    response.raise_for_status()
    return {flow.id: flow.name for flow in _flows.validate_python(response.json())}


def read_deployment(client: httpx.Client, deployment_id: UUID) -> Deployment:
    response = client.get(f"/deployments/{deployment_id}")
    response.raise_for_status()
    return Deployment.model_validate(response.json())


def create_flow_run_from_deployment(
    client: httpx.Client,
    deployment_id: UUID,
    *,
    parameters: dict[str, Any],
    tags: list[str],
    idempotency_key: str,
) -> FlowRun:
    """Prefect returns the existing run when ``idempotency_key`` was already used."""
    body = {"parameters": parameters, "tags": tags, "idempotency_key": idempotency_key}
    response = client.post(f"/deployments/{deployment_id}/create_flow_run", json=body)
    response.raise_for_status()
    return FlowRun.model_validate(response.json())
