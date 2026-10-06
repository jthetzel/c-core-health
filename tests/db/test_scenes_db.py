from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from c_core_health import scenes
from c_core_health.errors import ConflictError, NotFoundError
from c_core_health.models import Sensor
from tests.db.conftest import (
    NOW,
    RCM_DALO,
    S1_DALO,
    S1_JACMD,
    SCENE_A,
    SCENE_B,
    SCENE_C,
    SCENE_UL,
)

pytestmark = pytest.mark.db
SINCE = NOW - timedelta(days=7)


def keys(refs):
    return [(r.collection, r.scene_id) for r in refs]


def test_lists_recent_sar_scenes_newest_first(conn, db_settings):
    refs, cursor = scenes.list_scene_refs(conn, db_settings, since=SINCE, limit=50)
    assert keys(refs) == [
        (S1_JACMD, SCENE_A),
        (S1_DALO, SCENE_A),
        (RCM_DALO, SCENE_B),
        (S1_DALO, SCENE_C),
    ]
    assert cursor is None
    assert refs[2].sensor is Sensor.RCM


def test_keyset_pagination_covers_every_scene_once(conn, db_settings):
    everything, _ = scenes.list_scene_refs(conn, db_settings, since=SINCE, limit=50)
    seen, cursor = [], None
    while True:
        page, cursor = scenes.list_scene_refs(
            conn, db_settings, since=SINCE, limit=1, cursor=cursor
        )
        seen.extend(page)
        if cursor is None:
            break
    assert keys(seen) == keys(everything)


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        ({"sensor": Sensor.RCM}, [(RCM_DALO, SCENE_B)]),
        ({"sensor": Sensor.OTHER}, []),
        ({"collection": S1_DALO}, [(S1_DALO, SCENE_A), (S1_DALO, SCENE_C)]),
        (
            {"until": NOW - timedelta(minutes=30)},
            [(RCM_DALO, SCENE_B), (S1_DALO, SCENE_C)],
        ),
    ],
)
def test_filters(conn, db_settings, filters, expected):
    refs, _ = scenes.list_scene_refs(conn, db_settings, since=SINCE, limit=50, **filters)
    assert keys(refs) == expected


def test_invalid_cursor(conn, db_settings):
    with pytest.raises(ValueError, match="cursor"):
        scenes.list_scene_refs(conn, db_settings, since=SINCE, limit=5, cursor="nope")


def test_resolve_scene(conn, db_settings):
    with pytest.raises(ConflictError) as exc:
        scenes.resolve_scene(conn, db_settings, SCENE_A)
    assert exc.value.context["collections"] == [S1_DALO, S1_JACMD]
    assert scenes.resolve_scene(conn, db_settings, SCENE_A, S1_DALO).collection == S1_DALO
    with pytest.raises(NotFoundError):
        scenes.resolve_scene(conn, db_settings, SCENE_UL)
    with pytest.raises(NotFoundError):
        scenes.resolve_scene(conn, db_settings, "NOPE")


def test_stats_exclude_soft_deleted_and_stay_per_collection(conn):
    stats = scenes.scene_stats(
        conn, [(S1_DALO, SCENE_A), (S1_JACMD, SCENE_A), (S1_DALO, SCENE_C)]
    )

    a = stats[(S1_DALO, SCENE_A)]
    assert a.target_count == 3
    assert a.targets_by_type == {"Iceberg": 1, "Ship": 2}
    assert a.targets_by_polarization == {"hh": 1, "hv": 2}
    assert a.ais_correlated_target_count == 1
    assert a.ais_message_count == 2
    assert a.oil_spill_count == 2

    assert stats[(S1_JACMD, SCENE_A)].target_count == 0
    assert stats[(S1_DALO, SCENE_C)].target_count == 1
    assert stats[(S1_DALO, SCENE_C)].oil_spill_count == 1


def test_summary_flags_failed_and_unfinished_flows(conn, db_settings):
    ref = scenes.resolve_scene(conn, db_settings, SCENE_A, S1_DALO)
    [summary] = scenes.summarize_scenes(conn, [ref])

    assert summary.failed_flows == ["detect-oil-spill"]
    assert summary.unfinished_flows == ["acquire-ais-data-from-kpler-for-scene"]
    detect = next(f for f in summary.flows if f.flow_name == "fast-sar-detect-objects")
    assert (detect.run_name, detect.last_event) == ("clever-crab", "end")
    assert summary.last_flow_event_at == max(f.last_event_at for f in summary.flows)
    assert summary.last_flow_event_at is not None
    assert summary.last_flow_event_at.tzinfo is not None


def test_flow_event_runs_keep_every_attempt(conn, db_settings):
    ref = scenes.resolve_scene(conn, db_settings, SCENE_A, S1_DALO)
    runs = scenes.scene_flow_event_runs(conn, ref)

    assert runs[0].run_name == "old-run"
    by_name = {r.run_name: r for r in runs}
    assert by_name["almond-narwhal"].last_event == "fail"
    assert by_name["clever-crab"].first_event_at < by_name["clever-crab"].last_event_at


def test_read_engine_cannot_write(conn):
    with pytest.raises(DBAPIError, match="read-only"):
        conn.execute(text("DELETE FROM features.targets"))
