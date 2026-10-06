"""In-memory stand-in for the Prefect REST endpoints this service calls, via respx."""

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import respx

BASE_URL = "http://prefect.test/api"
T0 = datetime(2026, 9, 28, 22, 0, tzinfo=UTC)


@dataclass
class FakePrefect:
    flows: dict[str, str] = field(default_factory=dict)  # flow id -> name
    runs: dict[str, dict[str, Any]] = field(default_factory=dict)
    task_runs: dict[str, dict[str, Any]] = field(default_factory=dict)
    deployments: dict[str, dict[str, Any]] = field(default_factory=dict)
    created: list[dict[str, Any]] = field(default_factory=list)

    def flow_id(self, name: str) -> str:
        for fid, fname in self.flows.items():
            if fname == name:
                return fid
        fid = str(uuid.uuid4())
        self.flows[fid] = name
        return fid

    def deployment(self, flow_name: str, parameters: list[str]) -> str:
        for did, dep in self.deployments.items():
            if dep["flow_id"] == self.flow_id(flow_name):
                return did
        did = str(uuid.uuid4())
        self.deployments[did] = {
            "id": did,
            "name": f"{flow_name}-deployment",
            "flow_id": self.flow_id(flow_name),
            "paused": False,
            "parameter_openapi_schema": {"properties": {p: {} for p in parameters}},
        }
        return did

    def add_run(
        self,
        flow_name: str,
        name: str,
        *,
        parent: str | None = None,
        minutes: int = 0,
        parameters: dict[str, Any] | None = None,
        state: str = "COMPLETED",
        tags: list[str] | None = None,
        declared: list[str] | None = None,
    ) -> str:
        run_id = str(uuid.uuid4())
        parent_task_run_id = None
        if parent is not None:
            parent_task_run_id = str(uuid.uuid4())
            self.task_runs[parent_task_run_id] = {
                "id": parent_task_run_id,
                "name": f"{name}-task",
                "flow_run_id": parent,
            }
        created = T0 + timedelta(minutes=minutes)
        params = parameters or {}
        self.runs[run_id] = {
            "id": run_id,
            "name": name,
            "flow_id": self.flow_id(flow_name),
            "deployment_id": self.deployment(flow_name, declared or [*params, "force"]),
            "parent_task_run_id": parent_task_run_id,
            "parent_flow_run_id": parent,
            "state_type": state,
            "state_name": state.title(),
            "tags": tags or [],
            "parameters": params,
            "created": created.isoformat(),
            "start_time": (created + timedelta(seconds=5)).isoformat(),
            "end_time": None,
            "idempotency_key": None,
        }
        return run_id

    def _filter_runs(self, body: dict[str, Any]) -> list[dict[str, Any]]:
        runs = list(self.runs.values())
        flow_names = body.get("flows", {}).get("name", {}).get("any_")
        if flow_names is not None:
            runs = [r for r in runs if self.flows[r["flow_id"]] in flow_names]
        criteria = body.get("flow_runs", {})
        if "name" in criteria:
            runs = [r for r in runs if r["name"] in criteria["name"]["any_"]]
        if "parent_flow_run_id" in criteria:
            parents = criteria["parent_flow_run_id"]["any_"]
            runs = [r for r in runs if r["parent_flow_run_id"] in parents]
        if "tags" in criteria:
            wanted = set(criteria["tags"]["all_"])
            runs = [r for r in runs if wanted <= set(r["tags"])]
        offset, limit = body.get("offset", 0), body.get("limit", 200)
        return runs[offset : offset + limit]

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/api")
        body = json.loads(request.content) if request.content else {}
        parts = path.strip("/").split("/")
        match request.method, parts:
            case "POST", ["flow_runs", "filter"]:
                return httpx.Response(200, json=self._filter_runs(body))
            case "GET", ["flow_runs", run_id]:
                return self._get(self.runs, run_id)
            case "GET", ["task_runs", task_run_id]:
                return self._get(self.task_runs, task_run_id)
            case "POST", ["task_runs", "filter"]:
                ids = body["task_runs"]["id"]["any_"]
                return httpx.Response(200, json=[self.task_runs[i] for i in ids])
            case "POST", ["flows", "filter"]:
                ids = body["flows"]["id"]["any_"]
                return httpx.Response(
                    200, json=[{"id": i, "name": self.flows[i]} for i in ids]
                )
            case "GET", ["deployments", deployment_id]:
                return self._get(self.deployments, deployment_id)
            case "POST", ["deployments", deployment_id, "create_flow_run"]:
                return self._create(deployment_id, body)
            case "GET", ["admin", "version"]:
                return httpx.Response(200, json="3.6.15")
            case _:
                return httpx.Response(404, json={"detail": f"unhandled {path}"})

    @staticmethod
    def _get(store: dict[str, dict[str, Any]], key: str) -> httpx.Response:
        if key not in store:
            return httpx.Response(404, json={"detail": "not found"})
        return httpx.Response(200, json=store[key])

    def _create(self, deployment_id: str, body: dict[str, Any]) -> httpx.Response:
        key = body.get("idempotency_key")
        for run in self.runs.values():
            if (
                key
                and run["idempotency_key"] == key
                and run["deployment_id"] == deployment_id
            ):
                return httpx.Response(200, json=run)
        flow_name = self.flows[self.deployments[deployment_id]["flow_id"]]
        run_id = self.add_run(
            flow_name,
            f"new-run-{len(self.created)}",
            minutes=600,
            parameters=body["parameters"],
            state="SCHEDULED",
            tags=body["tags"],
        )
        self.runs[run_id]["deployment_id"] = deployment_id
        self.runs[run_id]["idempotency_key"] = key
        self.created.append(body)
        return httpx.Response(201, json=self.runs[run_id])

    def mock(self) -> respx.MockRouter:
        router = respx.mock(base_url=BASE_URL, assert_all_called=False)
        router.route().mock(side_effect=self.handle)
        return router
