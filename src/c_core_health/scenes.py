"""Scene listing and per-scene summary statistics."""

import base64
import binascii
import json
from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import (
    ColumnElement,
    Connection,
    and_,
    exists,
    func,
    literal,
    or_,
    select,
    tuple_,
)
from sqlalchemy.dialects import postgresql

from c_core_health.errors import ConflictError, NotFoundError
from c_core_health.models import (
    FlowEventRun,
    FlowStatus,
    SceneRef,
    SceneStats,
    SceneSummary,
    Sensor,
)
from c_core_health.settings import Settings
from c_core_health.tables import (
    ais_message,
    ais_targets,
    event,
    flow_event,
    items,
    oil_spill_detection,
    scene_event,
    target_types,
    targets,
)

SceneKey = tuple[str, str]
"""(collection, scene_id)"""

SENSOR_ID_PREFIXES: dict[Sensor, tuple[str, ...]] = {
    Sensor.SENTINEL_1: ("S1",),
    Sensor.RCM: ("RCM",),
}


def as_utc(value: datetime) -> datetime:
    """Holmes stores several timestamps as naive UTC."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def sensor_for(scene_id: str) -> Sensor:
    for sensor, prefixes in SENSOR_ID_PREFIXES.items():
        if scene_id.startswith(prefixes):
            return sensor
    return Sensor.OTHER


def key_of(ref: SceneRef) -> SceneKey:
    return (ref.collection, ref.scene_id)


def encode_cursor(ref: SceneRef) -> str:
    assert ref.acquired_at is not None
    payload = {"t": ref.acquired_at.isoformat(), "c": ref.collection, "i": ref.scene_id}
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, str, str]:
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return datetime.fromisoformat(payload["t"]), payload["c"], payload["i"]
    except (binascii.Error, ValueError, KeyError, TypeError) as exc:
        raise ValueError("Invalid cursor") from exc


def sar_collection_filter(settings: Settings) -> ColumnElement[bool]:
    return or_(*(items.c.collection.like(p) for p in settings.sar_collection_patterns))


def _ref_from_row(
    scene_id: str, collection: str, acquired_at: datetime | None
) -> SceneRef:
    return SceneRef(
        scene_id=scene_id,
        collection=collection,
        sensor=sensor_for(scene_id),
        acquired_at=as_utc(acquired_at) if acquired_at else None,
    )


def list_scene_refs(
    conn: Connection,
    settings: Settings,
    *,
    since: datetime,
    until: datetime | None = None,
    collection: str | None = None,
    sensor: Sensor | None = None,
    limit: int,
    cursor: str | None = None,
) -> tuple[list[SceneRef], str | None]:
    """Recent SAR scenes, newest acquisition first, keyset-paginated."""
    conditions: list[ColumnElement[bool]] = [
        sar_collection_filter(settings),
        items.c.datetime >= since,
    ]
    if until is not None:
        conditions.append(items.c.datetime < until)
    if collection is not None:
        conditions.append(items.c.collection == collection)
    if sensor is Sensor.OTHER:
        known = [p for prefixes in SENSOR_ID_PREFIXES.values() for p in prefixes]
        conditions.extend(~items.c.id.startswith(p) for p in known)
    elif sensor is not None:
        conditions.append(
            or_(*(items.c.id.startswith(p) for p in SENSOR_ID_PREFIXES[sensor]))
        )
    if cursor is not None:
        cursor_time, cursor_collection, cursor_id = decode_cursor(cursor)
        conditions.append(
            tuple_(items.c.datetime, items.c.collection, items.c.id)
            < tuple_(literal(cursor_time), literal(cursor_collection), literal(cursor_id))
        )

    stmt = (
        select(items.c.id, items.c.collection, items.c.datetime)
        .where(and_(*conditions))
        .order_by(items.c.datetime.desc(), items.c.collection.desc(), items.c.id.desc())
        .limit(limit + 1)
    )
    rows = conn.execute(stmt).all()
    refs = [_ref_from_row(r.id, r.collection, r.datetime) for r in rows[:limit]]
    next_cursor = encode_cursor(refs[-1]) if len(rows) > limit else None
    return refs, next_cursor


def resolve_scene(
    conn: Connection, settings: Settings, scene_id: str, collection: str | None = None
) -> SceneRef:
    conditions = [items.c.id == scene_id, sar_collection_filter(settings)]
    if collection is not None:
        conditions.append(items.c.collection == collection)
    rows = conn.execute(
        select(items.c.id, items.c.collection, items.c.datetime).where(and_(*conditions))
    ).all()
    if not rows:
        raise NotFoundError(
            f"Scene {scene_id!r} not found in SAR collections",
            scene_id=scene_id,
            collection=collection,
        )
    if len(rows) > 1:
        raise ConflictError(
            f"Scene {scene_id!r} exists in several collections; pass ?collection=",
            collections=sorted(r.collection for r in rows),
        )
    row = rows[0]
    return _ref_from_row(row.id, row.collection, row.datetime)


def _scene_filter(
    collection_col: ColumnElement[str],
    item_col: ColumnElement[str],
    keys: Sequence[SceneKey],
) -> ColumnElement[bool]:
    """The plain ``IN`` on the item id lets Postgres use the scene_item_id indexes."""
    return and_(
        item_col.in_({scene_id for _, scene_id in keys}),
        tuple_(collection_col, item_col).in_(list(keys)),
    )


def scene_stats(conn: Connection, keys: Sequence[SceneKey]) -> dict[SceneKey, SceneStats]:
    if not keys:
        return {}
    ais_correlated = exists().where(
        ais_targets.c.target_id == targets.c.id, ais_targets.c.deleted_at.is_(None)
    )
    type_name = func.coalesce(target_types.c.target_type_name, "Unknown")
    polarization = func.coalesce(targets.c.detected_from_polarization, "unknown")
    target_rows = conn.execute(
        select(
            targets.c.scene_collection_id,
            targets.c.scene_item_id,
            type_name.label("type_name"),
            polarization.label("polarization"),
            func.count().label("n"),
            func.count().filter(ais_correlated).label("n_ais"),
        )
        .select_from(
            targets.outerjoin(target_types, target_types.c.id == targets.c.target_type_id)
        )
        .where(
            _scene_filter(targets.c.scene_collection_id, targets.c.scene_item_id, keys),
            targets.c.deleted_at.is_(None),
        )
        .group_by(
            targets.c.scene_collection_id, targets.c.scene_item_id, type_name, polarization
        )
    ).all()
    oil_rows = conn.execute(
        select(
            oil_spill_detection.c.scene_collection_id,
            oil_spill_detection.c.scene_item_id,
            func.count().label("n"),
        )
        .where(
            _scene_filter(
                oil_spill_detection.c.scene_collection_id,
                oil_spill_detection.c.scene_item_id,
                keys,
            ),
            oil_spill_detection.c.deleted_at.is_(None),
        )
        .group_by(
            oil_spill_detection.c.scene_collection_id, oil_spill_detection.c.scene_item_id
        )
    ).all()
    ais_rows = conn.execute(
        select(
            ais_message.c.scene_collection_id,
            ais_message.c.scene_item_id,
            func.count().label("n"),
        )
        .where(
            _scene_filter(
                ais_message.c.scene_collection_id, ais_message.c.scene_item_id, keys
            )
        )
        .group_by(ais_message.c.scene_collection_id, ais_message.c.scene_item_id)
    ).all()

    by_type: dict[SceneKey, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    by_pol: dict[SceneKey, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    ais_correlated_counts: dict[SceneKey, int] = defaultdict(int)
    for r in target_rows:
        key = (r.scene_collection_id, r.scene_item_id)
        by_type[key][r.type_name] += r.n
        by_pol[key][r.polarization] += r.n
        ais_correlated_counts[key] += r.n_ais
    oil = {(r.scene_collection_id, r.scene_item_id): r.n for r in oil_rows}
    ais = {(r.scene_collection_id, r.scene_item_id): r.n for r in ais_rows}

    return {
        key: SceneStats(
            target_count=sum(by_type[key].values()),
            targets_by_type=dict(sorted(by_type[key].items())),
            targets_by_polarization=dict(sorted(by_pol[key].items())),
            ais_correlated_target_count=ais_correlated_counts[key],
            ais_message_count=ais.get(key, 0),
            oil_spill_count=oil.get(key, 0),
        )
        for key in keys
    }


def latest_flow_statuses(
    conn: Connection, keys: Sequence[SceneKey]
) -> dict[SceneKey, list[FlowStatus]]:
    """Latest event per (scene, flow name) from the Holmes event tables."""
    if not keys:
        return {}
    stmt = (
        select(
            scene_event.c.collection_id,
            scene_event.c.item_id,
            flow_event.c.flow_name,
            flow_event.c.flow_id,
            flow_event.c.type,
            event.c.time,
        )
        .ext(
            postgresql.distinct_on(
                scene_event.c.collection_id, scene_event.c.item_id, flow_event.c.flow_name
            )
        )
        .select_from(
            scene_event.join(
                flow_event, flow_event.c.event_id == scene_event.c.event_id
            ).join(event, event.c.id == scene_event.c.event_id)
        )
        .where(_scene_filter(scene_event.c.collection_id, scene_event.c.item_id, keys))
        .order_by(
            scene_event.c.collection_id,
            scene_event.c.item_id,
            flow_event.c.flow_name,
            event.c.time.desc(),
        )
    )
    result: dict[SceneKey, list[FlowStatus]] = defaultdict(list)
    for r in conn.execute(stmt):
        result[(r.collection_id, r.item_id)].append(
            FlowStatus(
                flow_name=r.flow_name,
                run_name=r.flow_id,
                last_event=r.type,
                last_event_at=as_utc(r.time),
            )
        )
    return result


def summarize_scenes(conn: Connection, refs: Sequence[SceneRef]) -> list[SceneSummary]:
    keys = [key_of(ref) for ref in refs]
    stats = scene_stats(conn, keys)
    flows = latest_flow_statuses(conn, keys)
    summaries: list[SceneSummary] = []
    for ref in refs:
        scene_flows = sorted(flows.get(key_of(ref), []), key=lambda f: f.last_event_at)
        summaries.append(
            SceneSummary(
                **ref.model_dump(),
                stats=stats[key_of(ref)],
                flows=scene_flows,
                failed_flows=[f.flow_name for f in scene_flows if f.is_failed],
                unfinished_flows=[f.flow_name for f in scene_flows if f.is_unfinished],
                last_flow_event_at=scene_flows[-1].last_event_at if scene_flows else None,
            )
        )
    return summaries


def scene_flow_event_runs(conn: Connection, ref: SceneRef) -> list[FlowEventRun]:
    """Every (flow, run name) recorded for the scene, oldest first."""
    newest_first = postgresql.aggregate_order_by(flow_event.c.type, event.c.time.desc())
    last_type = postgresql.array_agg(newest_first)[1]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    stmt = (
        select(
            flow_event.c.flow_name,
            flow_event.c.flow_id,
            func.min(event.c.time).label("first_at"),
            func.max(event.c.time).label("last_at"),
            last_type.label("last_type"),
        )
        .select_from(
            scene_event.join(
                flow_event, flow_event.c.event_id == scene_event.c.event_id
            ).join(event, event.c.id == scene_event.c.event_id)
        )
        .where(
            scene_event.c.item_id == ref.scene_id,
            scene_event.c.collection_id == ref.collection,
        )
        .group_by(flow_event.c.flow_name, flow_event.c.flow_id)
        .order_by(func.min(event.c.time))
    )
    return [
        FlowEventRun(
            flow_name=r.flow_name,
            run_name=r.flow_id,
            first_event_at=as_utc(r.first_at),
            last_event_at=as_utc(r.last_at),
            last_event=r.last_type,
        )
        for r in conn.execute(stmt)
    ]
