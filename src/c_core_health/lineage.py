"""Resolve which Prefect flow runs processed a scene and which run is its parent.

Holmes records ``(flow name, run name)`` per scene in ``features.flow_event``. Those
runs are looked up in Prefect, then the run tree is walked upward through
``parent_task_run_id`` until the next ancestor is a multi-scene scheduler flow, and
downward through ``parent_flow_run_id`` to catch runs that crashed before writing events.
"""

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

import httpx
from loguru import logger
from sqlalchemy import Connection

from c_core_health import prefect_api
from c_core_health.models import FlowEventRun, Lineage, RunNode, SceneRef
from c_core_health.prefect_api import FlowRun
from c_core_health.scenes import scene_flow_event_runs
from c_core_health.settings import Settings

SERVICE_TAG = "c-core-health"
MAX_ANCESTOR_HOPS = 25


def scene_tag(ref: SceneRef) -> str:
    return f"scene:{ref.collection}/{ref.scene_id}"


def batch_tag(batch_key: str) -> str:
    return f"c-core-health-batch:{batch_key}"


def parameters_mention(parameters: Any, scene_id: str) -> bool:
    """True if any string value, however nested, contains the scene id.

    Substring match because parents embed the id, e.g. S1 ``image_id`` is
    ``<scene_id>.SAFE`` and detect flows use a bucket path ending in the id.
    """
    if isinstance(parameters, str):
        return scene_id in parameters
    if isinstance(parameters, dict):
        return any(parameters_mention(v, scene_id) for v in parameters.values())  # pyright: ignore[reportUnknownVariableType]
    if isinstance(parameters, list | tuple):
        return any(parameters_mention(v, scene_id) for v in parameters)  # pyright: ignore[reportUnknownVariableType]
    return False


@dataclass
class RunGraph:
    """Mutable working state while exploring the Prefect run tree for one scene."""

    client: httpx.Client
    scheduler_flow_names: frozenset[str]
    max_runs: int
    runs: dict[UUID, FlowRun] = field(default_factory=dict[UUID, FlowRun])
    flow_names: dict[UUID, str] = field(default_factory=dict[UUID, str])
    parent_of: dict[UUID, UUID] = field(default_factory=dict[UUID, UUID])
    root_of: dict[UUID, UUID] = field(default_factory=dict[UUID, UUID])
    scheduler_of_root: dict[UUID, UUID] = field(default_factory=dict[UUID, UUID])
    truncated: bool = False

    def add(self, runs: Iterable[FlowRun]) -> None:
        new = [r for r in runs if r.id not in self.runs]
        for run in new:
            self.runs[run.id] = run
        missing = {r.flow_id for r in new} - set(self.flow_names)
        if missing:
            self.flow_names.update(prefect_api.read_flow_names(self.client, missing))

    def flow_name(self, run: FlowRun) -> str:
        return self.flow_names.get(run.flow_id, "<unknown flow>")


def match_event_runs(
    graph: RunGraph, event_runs: list[FlowEventRun], scene_id: str
) -> tuple[list[FlowRun], list[FlowEventRun]]:
    """Find the Prefect run behind each event-table ``(flow name, run name)`` pair.

    Run names are Prefect's random adjective-animal names and do repeat across the
    history of a flow, so ties go to runs whose parameters mention the scene and then
    to the run that started closest to the first event.
    """
    if not event_runs:
        return [], []
    candidates = prefect_api.filter_flow_runs(
        graph.client,
        {
            "flows": {"name": {"any_": sorted({e.flow_name for e in event_runs})}},
            "flow_runs": {"name": {"any_": sorted({e.run_name for e in event_runs})}},
        },
        max_results=graph.max_runs,
    )
    graph.add(candidates)
    by_pair: dict[tuple[str, str], list[FlowRun]] = defaultdict(list)
    for run in candidates:
        by_pair[(graph.flow_name(run), run.name)].append(run)

    matched: list[FlowRun] = []
    unmatched: list[FlowEventRun] = []
    for event_run in event_runs:
        options = by_pair.get((event_run.flow_name, event_run.run_name), [])
        if not options:
            unmatched.append(event_run)
            continue
        mentioning = [r for r in options if parameters_mention(r.parameters, scene_id)]
        pool = mentioning or options
        matched.append(min(pool, key=lambda r: _distance(r, event_run.first_event_at)))
    return matched, unmatched


def _distance(run: FlowRun, moment: datetime) -> float:
    return abs(((run.start_time or run.created) - moment).total_seconds())


def _parent_run(graph: RunGraph, run: FlowRun) -> FlowRun | None:
    if run.parent_task_run_id is None:
        return None
    task_run = prefect_api.read_task_run(graph.client, run.parent_task_run_id)
    if task_run.flow_run_id is None:
        return None
    parent = graph.runs.get(task_run.flow_run_id)
    if parent is None:
        parent = prefect_api.read_flow_run(graph.client, task_run.flow_run_id)
        graph.add([parent])
    return parent


def find_root(graph: RunGraph, run: FlowRun) -> UUID:
    """Walk upward to the highest per-scene ancestor, stopping below scheduler flows."""
    path = [run]
    current = run
    root: UUID | None = None
    for _ in range(MAX_ANCESTOR_HOPS):
        if current.id in graph.root_of:
            root = graph.root_of[current.id]
            break
        parent = _parent_run(graph, current)
        if parent is None:
            root = current.id
            break
        if graph.flow_name(parent) in graph.scheduler_flow_names:
            root = current.id
            graph.scheduler_of_root[current.id] = parent.id
            break
        graph.parent_of[current.id] = parent.id
        current = parent
        path.append(current)
    else:
        logger.warning(
            "Lineage walk exceeded {} hops from run {}", MAX_ANCESTOR_HOPS, run.id
        )
        root = current.id
    for node in path:
        graph.root_of[node.id] = root
    return root


def add_descendants(graph: RunGraph, root_id: UUID) -> None:
    frontier = [root_id]
    while frontier:
        if len(graph.runs) >= graph.max_runs:
            graph.truncated = True
            return
        children = prefect_api.filter_flow_runs(
            graph.client,
            {"flow_runs": {"parent_flow_run_id": {"any_": [str(i) for i in frontier]}}},
            max_results=graph.max_runs - len(graph.runs),
        )
        graph.add(children)
        children = [c for c in children if c.id not in graph.root_of]
        parent_by_task_run = _parent_flow_runs(graph, children, frontier)
        for child in children:
            graph.root_of[child.id] = root_id
            parent_id = parent_by_task_run.get(child.parent_task_run_id)
            if parent_id is not None:
                graph.parent_of[child.id] = parent_id
        frontier = [c.id for c in children]


def _parent_flow_runs(
    graph: RunGraph, children: list[FlowRun], frontier: list[UUID]
) -> dict[UUID | None, UUID]:
    """Map each child's ``parent_task_run_id`` to the flow run owning that task run."""
    if len(frontier) == 1:
        return {c.parent_task_run_id: frontier[0] for c in children}
    task_run_ids = {c.parent_task_run_id for c in children if c.parent_task_run_id}
    return {
        t.id: t.flow_run_id
        for t in prefect_api.read_task_runs(graph.client, task_run_ids)
        if t.flow_run_id is not None
    }


def _node(graph: RunGraph, run: FlowRun) -> RunNode:
    return RunNode(
        id=run.id,
        name=run.name,
        flow_name=graph.flow_name(run),
        state_type=run.state_type,
        state_name=run.state_name,
        deployment_id=run.deployment_id,
        parent_flow_run_id=graph.parent_of.get(run.id),
        created=run.created,
        start_time=run.start_time,
        end_time=run.end_time,
        tags=run.tags,
        parameters=run.parameters,
    )


def _group_key(graph: RunGraph, run: FlowRun) -> str:
    """Roots started together: same scheduler run, or same c-core-health re-run batch."""
    if run.id in graph.scheduler_of_root:
        return f"scheduler:{graph.scheduler_of_root[run.id]}"
    batch = next((t for t in run.tags if t.startswith(batch_tag(""))), None)
    return batch or f"run:{run.id}"


def choose_parent(
    graph: RunGraph, roots: list[FlowRun], scene_id: str, parent_flow_names: list[str]
) -> FlowRun | None:
    """Newest root of the highest-priority parent flow, else the newest scene root."""
    for flow_name in parent_flow_names:
        preferred = [r for r in roots if graph.flow_name(r) == flow_name]
        if preferred:
            return max(preferred, key=lambda r: r.created)
    about_scene = [r for r in roots if parameters_mention(r.parameters, scene_id)]
    return max(about_scene, key=lambda r: r.created) if about_scene else None


def resolve_lineage(
    conn: Connection, client: httpx.Client, settings: Settings, ref: SceneRef
) -> Lineage:
    graph = RunGraph(
        client=client,
        scheduler_flow_names=frozenset(settings.scheduler_flow_names),
        max_runs=settings.max_lineage_runs,
    )
    event_runs = scene_flow_event_runs(conn, ref)
    matched, unmatched = match_event_runs(graph, event_runs, ref.scene_id)

    reruns = prefect_api.filter_flow_runs(
        client,
        {"flow_runs": {"tags": {"all_": [SERVICE_TAG, scene_tag(ref)]}}},
        max_results=100,
    )
    graph.add(reruns)

    root_ids: set[UUID] = set()
    for run in [*reruns, *matched]:
        if run.id in graph.root_of:
            continue
        root_id = find_root(graph, run)
        if root_id not in root_ids:
            root_ids.add(root_id)
            add_descendants(graph, root_id)

    roots = sorted(
        (graph.runs[i] for i in root_ids), key=lambda r: r.created, reverse=True
    )
    parent = choose_parent(graph, roots, ref.scene_id, settings.parent_flow_names)
    siblings: list[FlowRun] = []
    if parent is not None:
        group = _group_key(graph, parent)
        siblings = [
            r
            for r in roots
            if r.id != parent.id
            and _group_key(graph, r) == group
            and parameters_mention(r.parameters, ref.scene_id)
        ]

    in_tree = [r for r in graph.runs.values() if r.id in graph.root_of]
    in_tree.sort(key=lambda r: r.created)
    active = [r.id for r in in_tree if r.state_type in settings.active_state_types]
    if graph.truncated:
        logger.warning("Lineage for {} truncated at {} runs", ref.scene_id, graph.max_runs)

    return Lineage(
        parent=_node(graph, parent) if parent is not None else None,
        sibling_roots=[_node(graph, r) for r in siblings],
        roots=[_node(graph, r) for r in roots],
        runs=[_node(graph, r) for r in in_tree],
        active_run_ids=active,
        unmatched_events=unmatched,
    )
