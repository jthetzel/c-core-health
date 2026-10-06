import json
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, text

from c_core_health import purge
from c_core_health.db import create_db_engine
from c_core_health.errors import ConflictError
from c_core_health.models import PurgeRequest, SceneRef, Sensor
from tests.db.conftest import NOW, S1_DALO, S1_JACMD, SCENE_A, SCENE_C, Seed

pytestmark = pytest.mark.db

REF = SceneRef(
    scene_id=SCENE_A, collection=S1_DALO, sensor=Sensor.SENTINEL_1, acquired_at=NOW
)


@pytest.fixture
def write_engine(db_settings, seed: Seed) -> Iterator[Engine]:
    engine = create_db_engine(db_settings, read_only=False)
    yield engine
    engine.dispose()


def count(engine: Engine, table: str, where: str = "TRUE", **params: object) -> int:
    sql = f"SELECT count(*) FROM {table} WHERE {where}"
    with engine.connect() as conn:
        return conn.execute(text(sql), params).scalar_one()


def preview(conn, **kwargs):
    options = {"include_ais_messages": False, "allow_analyst_data_loss": False, **kwargs}
    return purge.build_preview(conn, REF, **options)


def request(**kwargs) -> PurgeRequest:
    return PurgeRequest(confirm_scene_id=SCENE_A, dry_run=False, **kwargs)


def test_preview_counts(conn):
    result = preview(conn)
    assert result.deletes == {
        "features.ais_targets": 1,
        "features.iceberg_drift_forecasting": 1,
        "features.targets": 4,
        "features.grouped_targets": 0,
        "features.oil_spill_detection": 2,
        "features.oil_spill_alert": 0,
        "features.scene_qa": 0,
        "features.stac_pmtiles": 0,
    }
    assert result.nullifies == {
        "features.unseenlabs_target_information.targets_table_id": 0,
        "features.ais_vessels.targets_table_id": 1,
    }
    assert result.blockers == []


def test_preview_with_ais_messages(conn):
    result = preview(conn, include_ais_messages=True)
    assert result.deletes["features.ais_message"] == 2
    assert result.deletes["features.ais_targets"] == 1
    assert result.nullifies["features.unseenlabs_target_information.ais_message_id"] == 1


def test_purge_deletes_only_this_scenes_derived_rows(write_engine, db_settings, seed):
    result = purge.execute_purge(write_engine, db_settings.backup_dir, REF, request())

    assert result.executed
    assert result.deleted == {**result.preview.deletes, **result.preview.nullifies}
    assert count(write_engine, "features.targets", "scene_item_id = :v", v=SCENE_A) == 0
    assert count(write_engine, "features.targets", "scene_item_id = :v", v=SCENE_C) == 1
    assert (
        count(
            write_engine, "features.oil_spill_detection", "scene_item_id = :v", v=SCENE_C
        )
        == 1
    )
    assert count(write_engine, "features.ais_targets") == 0
    assert count(write_engine, "features.iceberg_drift_forecasting") == 0
    assert count(write_engine, "features.ais_vessels", "targets_table_id IS NULL") == 1
    # Kept: scene items (both collections), event history and AIS messages.
    assert count(write_engine, "pgstac.items", "id = :v", v=SCENE_A) == 2
    assert count(write_engine, "features.flow_event") == 8
    assert count(write_engine, "features.ais_message") == 2


def test_purge_writes_complete_backup(write_engine, db_settings, seed):
    result = purge.execute_purge(write_engine, db_settings.backup_dir, REF, request())

    assert result.backup_path is not None
    backup = Path(result.backup_path)
    lines = (backup / "features.targets.jsonl").read_text().splitlines()
    assert sorted(json.loads(line)["id"] for line in lines) == sorted(
        [seed.ship_hh, seed.iceberg_hv, seed.ship_hv_duplicate, seed.deleted_target]
    )
    manifest = json.loads((backup / "manifest.json").read_text())
    assert manifest["scene"]["scene_id"] == SCENE_A
    assert not (backup / "ROLLED_BACK").exists()


def test_purge_with_ais_messages(write_engine, db_settings):
    purge.execute_purge(
        write_engine, db_settings.backup_dir, REF, request(include_ais_messages=True)
    )
    assert count(write_engine, "features.ais_message") == 0
    assert (
        count(
            write_engine,
            "features.unseenlabs_target_information",
            "ais_message_id IS NULL",
        )
        == 1
    )


def test_analyst_data_blocks_unless_allowed(write_engine, db_settings, seed):
    with write_engine.begin() as conn:
        conn.execute(
            text("UPDATE features.targets SET analyst_confirmed = true WHERE id = :id"),
            {"id": seed.iceberg_hv},
        )
        conn.execute(
            text(
                "INSERT INTO features.annotations (subject_type, subject_key) "
                "VALUES ('detection', :k)"
            ),
            {"k": str(seed.iceberg_hv)},
        )
    with pytest.raises(ConflictError) as exc:
        purge.execute_purge(write_engine, db_settings.backup_dir, REF, request())
    assert "analyst" in exc.value.context["blockers"][0]
    assert count(write_engine, "features.targets") == 5

    result = purge.execute_purge(
        write_engine, db_settings.backup_dir, REF, request(allow_analyst_data_loss=True)
    )
    assert (
        result.preview.analyst_data["features.annotations (detections, kept but orphaned)"]
        == 1
    )
    assert count(write_engine, "features.annotations") == 1


def test_cross_scene_duplicate_blocks(write_engine, db_settings, seed):
    with write_engine.begin() as conn:
        conn.execute(
            text("UPDATE features.targets SET duplicate_of = :a WHERE id = :c"),
            {"a": seed.ship_hh, "c": seed.scene_c_target},
        )
    with pytest.raises(ConflictError):
        purge.execute_purge(write_engine, db_settings.backup_dir, REF, request())
    assert count(write_engine, "features.targets", "scene_item_id = :v", v=SCENE_C) == 1


def test_active_runs_block(write_engine, db_settings):
    with pytest.raises(ConflictError):
        purge.execute_purge(
            write_engine,
            db_settings.backup_dir,
            REF,
            request(),
            active_run_ids=[uuid.uuid4()],
        )


def test_stale_preview_is_rejected(write_engine, db_settings):
    with pytest.raises(ConflictError, match="changed since the preview"):
        purge.execute_purge(
            write_engine,
            db_settings.backup_dir,
            REF,
            request(expected_deletes={"features.targets": 1}),
        )
    assert count(write_engine, "features.targets") == 5


def test_confirmation_must_match(write_engine, db_settings):
    bad = PurgeRequest(confirm_scene_id=SCENE_C, dry_run=False)
    with pytest.raises(ConflictError, match="confirm_scene_id"):
        purge.execute_purge(write_engine, db_settings.backup_dir, REF, bad)


def test_count_mismatch_rolls_back(write_engine, db_settings, monkeypatch):
    real_step = purge._execute_step

    def miscounting(conn, step):
        n = real_step(conn, step)
        return n + 1 if step.label == "features.targets" else n

    monkeypatch.setattr(purge, "_execute_step", miscounting)
    with pytest.raises(ConflictError, match="mismatch"):
        purge.execute_purge(write_engine, db_settings.backup_dir, REF, request())

    assert count(write_engine, "features.targets") == 5
    assert count(write_engine, "features.ais_targets") == 1
    [backup] = db_settings.backup_dir.iterdir()
    assert (backup / "ROLLED_BACK").exists()


def test_other_collection_with_same_scene_id_untouched(write_engine, db_settings):
    with write_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO features.targets (scene_item_id, scene_collection_id) "
                "VALUES (:s, :c)"
            ),
            {"s": SCENE_A, "c": S1_JACMD},
        )
    purge.execute_purge(write_engine, db_settings.backup_dir, REF, request())
    assert (
        count(write_engine, "features.targets", "scene_collection_id = :v", v=S1_JACMD)
        == 1
    )
