"""API response and request schemas."""

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ApiModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class Sensor(StrEnum):
    SENTINEL_1 = "sentinel-1"
    RCM = "rcm"
    OTHER = "other"


class SceneRef(ApiModel):
    scene_id: str
    collection: str
    sensor: Sensor
    acquired_at: datetime | None


class FlowStatus(ApiModel):
    """Latest event of the latest run of one flow, from ``features.flow_event``."""

    flow_name: str
    run_name: str
    last_event: str
    last_event_at: datetime

    @property
    def is_failed(self) -> bool:
        return self.last_event == "fail"

    @property
    def is_unfinished(self) -> bool:
        return self.last_event == "start"


class SceneStats(ApiModel):
    target_count: int = Field(
        description="Rows in features.targets (one per detection per polarization), "
        "excluding soft-deleted rows"
    )
    targets_by_type: dict[str, int]
    targets_by_polarization: dict[str, int]
    ais_correlated_target_count: int
    ais_message_count: int
    oil_spill_count: int


class SceneSummary(SceneRef):
    stats: SceneStats
    flows: list[FlowStatus]
    failed_flows: list[str]
    unfinished_flows: list[str] = Field(
        description="Flows whose latest event is 'start': still running, or crashed "
        "without a fail event"
    )
    last_flow_event_at: datetime | None


class ScenePage(ApiModel):
    items: list[SceneSummary]
    next_cursor: str | None


class FlowEventRun(ApiModel):
    """One (flow, run name) pair recorded for a scene in the event tables."""

    flow_name: str
    run_name: str
    first_event_at: datetime
    last_event_at: datetime
    last_event: str


class RunNode(ApiModel):
    id: UUID
    name: str
    flow_name: str
    state_type: str | None
    state_name: str | None
    deployment_id: UUID | None
    parent_flow_run_id: UUID | None
    created: datetime
    start_time: datetime | None
    end_time: datetime | None
    tags: list[str]
    parameters: dict[str, Any]


class Lineage(ApiModel):
    parent: RunNode | None = Field(
        description="Latest run of the preferred per-scene parent flow"
    )
    sibling_roots: list[RunNode] = Field(
        description="Other top-level per-scene runs of the latest attempt, e.g. RCM "
        "Kpler AIS acquisition started beside download-rcm-scene"
    )
    roots: list[RunNode] = Field(description="All top-level per-scene runs, newest first")
    runs: list[RunNode] = Field(description="Every run found for the scene")
    active_run_ids: list[UUID]
    unmatched_events: list[FlowEventRun] = Field(
        description="Event-table runs that could not be found in Prefect"
    )


class SceneDetail(SceneSummary):
    lineage: Lineage


class RerunRequest(ApiModel):
    dry_run: bool = True
    include_sibling_roots: bool = False


class PlannedRun(ApiModel):
    source_run_id: UUID
    source_run_name: str
    flow_name: str
    deployment_id: UUID
    deployment_name: str
    parameters: dict[str, Any]
    tags: list[str]
    idempotency_key: str
    created_run_id: UUID | None = None
    created_run_name: str | None = None
    created_state_type: str | None = None


class RerunResponse(ApiModel):
    scene: SceneRef
    dry_run: bool
    runs: list[PlannedRun]


class PurgePreview(ApiModel):
    scene: SceneRef
    include_ais_messages: bool
    deletes: dict[str, int] = Field(
        description="Rows that will be deleted per table, including FK cascades"
    )
    nullifies: dict[str, int] = Field(
        description="Rows in other tables whose FK to a deleted row becomes NULL"
    )
    analyst_data: dict[str, int] = Field(
        description="Analyst-authored rows that the purge would destroy"
    )
    cross_scene_duplicate_targets: int
    blockers: list[str]


class PurgeRequest(ApiModel):
    confirm_scene_id: str = Field(description="Must equal the scene id in the path")
    dry_run: bool = True
    include_ais_messages: bool = False
    allow_analyst_data_loss: bool = False
    expected_deletes: dict[str, int] | None = Field(
        default=None,
        description="Counts from a preview; the purge aborts if the data changed since",
    )


class PurgeResult(ApiModel):
    preview: PurgePreview
    dry_run: bool
    executed: bool
    backup_path: str | None
    deleted: dict[str, int]
