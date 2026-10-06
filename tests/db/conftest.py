"""Fixtures backed by a disposable Postgres container loaded with ``schema.sql``.

Needs a Docker-compatible socket. With colima, export
``DOCKER_HOST=unix://$HOME/.colima/default/docker.sock`` and
``TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock``.
"""

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import Connection, Engine, create_engine, text

from c_core_health.db import create_db_engine
from c_core_health.settings import Settings
from tests.conftest import make_settings

SCHEMA = (Path(__file__).parent / "schema.sql").read_text()
NOW = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)

S1_DALO = "dalo-greenland-eez-s1"
S1_JACMD = "jacmd-greenland-eez-s1"
RCM_DALO = "dalo-greenland-rcm"
UNSEENLABS = "dalo-greenland-eez-unseenlabs"

SCENE_A = "S1C_EW_GRDM_1SDH_20260929T120000_A_COG"
SCENE_B = "RCM1_OK1_PK1_1_SC50MB_20260929_110000_HH_HV_GRD"
SCENE_C = "S1C_EW_GRDM_1SDH_20260929T100000_C_COG"
SCENE_OLD = "S1A_EW_GRDM_1SDH_20260801T100000_OLD_COG"
SCENE_UL = "UNSEENLABS_SURMAR_20260929T120000Z"


@dataclass(frozen=True)
class Seed:
    ship_hh: int
    iceberg_hv: int
    ship_hv_duplicate: int
    deleted_target: int
    scene_c_target: int
    ais_msg_1: int
    ais_msg_2: int


@pytest.fixture(scope="session")
def pg_url() -> Iterator[str]:
    try:
        from testcontainers.community.postgres import PostgresContainer

        container = PostgresContainer("postgres:16-alpine", driver="psycopg")
        container.start()
    except Exception as exc:
        pytest.skip(f"Postgres container unavailable: {exc}")
    try:
        yield container.get_connection_url()
    finally:
        container.stop()


@pytest.fixture
def admin_engine(pg_url: str) -> Iterator[Engine]:
    engine = create_engine(pg_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS pgstac CASCADE"))
        conn.execute(text("DROP SCHEMA IF EXISTS features CASCADE"))
        conn.exec_driver_sql(SCHEMA)
    yield engine
    engine.dispose()


@pytest.fixture
def db_settings(pg_url: str, tmp_path: Path) -> Settings:
    return make_settings(tmp_path, pg_dsn=pg_url)


@pytest.fixture
def read_engine(db_settings: Settings, admin_engine: Engine) -> Iterator[Engine]:
    engine = create_db_engine(db_settings, read_only=True)
    yield engine
    engine.dispose()


@pytest.fixture
def conn(read_engine: Engine, seed: Seed) -> Iterator[Connection]:
    with read_engine.connect() as connection:
        yield connection
        connection.rollback()


def _insert(conn: Connection, sql: str, **params: object) -> int:
    return conn.execute(text(sql), params).scalar_one()


def _target(conn: Connection, scene: str, collection: str, **values: object) -> int:
    columns = ["scene_item_id", "scene_collection_id", *values]
    placeholders = ", ".join(f":{c}" for c in columns)
    return _insert(
        conn,
        f"INSERT INTO features.targets ({', '.join(columns)}) VALUES ({placeholders}) "
        "RETURNING id",
        scene_item_id=scene,
        scene_collection_id=collection,
        **values,
    )


def _event(
    conn: Connection,
    scene: str,
    collection: str,
    flow: str,
    run: str,
    kind: str,
    at: datetime,
) -> None:
    event_id = uuid.uuid4()
    conn.execute(
        text("INSERT INTO features.event (id, time) VALUES (:id, :time)"),
        {"id": event_id, "time": at.replace(tzinfo=None)},
    )
    conn.execute(
        text("INSERT INTO features.scene_event VALUES (:id, :collection, :item)"),
        {"id": event_id, "collection": collection, "item": scene},
    )
    conn.execute(
        text("INSERT INTO features.flow_event VALUES (:id, :type, :flow, :run, NULL)"),
        {"id": event_id, "type": kind, "flow": flow, "run": run},
    )


@pytest.fixture
def seed(admin_engine: Engine) -> Seed:
    with admin_engine.begin() as conn:
        for collection in (S1_DALO, S1_JACMD, RCM_DALO, UNSEENLABS):
            conn.execute(
                text("INSERT INTO pgstac.collections VALUES (:c)"), {"c": collection}
            )
        for scene, collection, hours in [
            (SCENE_A, S1_DALO, 0),
            (SCENE_A, S1_JACMD, 0),
            (SCENE_B, RCM_DALO, 1),
            (SCENE_C, S1_DALO, 2),
            (SCENE_UL, UNSEENLABS, 0),
            (SCENE_OLD, S1_DALO, 24 * 59),
        ]:
            conn.execute(
                text("INSERT INTO pgstac.items VALUES (:id, :c, :t, '{}')"),
                {"id": scene, "c": collection, "t": NOW - timedelta(hours=hours)},
            )
        conn.execute(
            text(
                "INSERT INTO features.target_types VALUES "
                "(1, 'Unclassified'), (2, 'Ship'), (3, 'Ship - AIS'), (4, 'Iceberg')"
            )
        )
        ship_hh = _target(
            conn, SCENE_A, S1_DALO, target_type_id=2, detected_from_polarization="hh"
        )
        iceberg_hv = _target(
            conn, SCENE_A, S1_DALO, target_type_id=4, detected_from_polarization="hv"
        )
        ship_hv_duplicate = _target(
            conn,
            SCENE_A,
            S1_DALO,
            target_type_id=2,
            detected_from_polarization="hv",
            duplicate_of=ship_hh,
        )
        deleted_target = _target(
            conn, SCENE_A, S1_DALO, target_type_id=1, deleted_at=NOW.replace(tzinfo=None)
        )
        scene_c_target = _target(
            conn, SCENE_C, S1_DALO, target_type_id=2, detected_from_polarization="hh"
        )
        ais_msg_1 = _insert(
            conn,
            "INSERT INTO features.ais_message (scene_item_id, scene_collection_id) "
            "VALUES (:s, :c) RETURNING id",
            s=SCENE_A,
            c=S1_DALO,
        )
        ais_msg_2 = _insert(
            conn,
            "INSERT INTO features.ais_message (scene_item_id, scene_collection_id) "
            "VALUES (:s, :c) RETURNING id",
            s=SCENE_A,
            c=S1_DALO,
        )
        conn.execute(
            text(
                "INSERT INTO features.ais_targets (target_id, ais_point_id) "
                "VALUES (:t, :m)"
            ),
            {"t": ship_hh, "m": ais_msg_1},
        )
        conn.execute(
            text("INSERT INTO features.iceberg_drift_forecasting (target_id) VALUES (:t)"),
            {"t": iceberg_hv},
        )
        conn.execute(
            text("INSERT INTO features.ais_vessels (targets_table_id) VALUES (:t)"),
            {"t": ship_hh},
        )
        conn.execute(
            text(
                "INSERT INTO features.unseenlabs_target_information (ais_message_id) "
                "VALUES (:m)"
            ),
            {"m": ais_msg_2},
        )
        for scene in (SCENE_A, SCENE_A, SCENE_C):
            conn.execute(
                text(
                    "INSERT INTO features.oil_spill_detection "
                    "(scene_item_id, scene_collection_id) VALUES (:s, :c)"
                ),
                {"s": scene, "c": S1_DALO},
            )

        start = NOW + timedelta(minutes=30)
        chain = [
            (
                "fast-sar-detector-processing-chain",
                "overjoyed-chimpanzee",
                ["start", "end"],
            ),
            ("fast-sar-detect-objects", "clever-crab", ["start", "end"]),
            ("detect-oil-spill", "almond-narwhal", ["start", "fail"]),
            ("acquire-ais-data-from-kpler-for-scene", "petite-ant", ["start"]),
        ]
        for offset, (flow, run, kinds) in enumerate(chain):
            for step, kind in enumerate(kinds):
                at = start + timedelta(minutes=offset * 5 + step)
                _event(conn, SCENE_A, S1_DALO, flow, run, kind, at)
        # An earlier attempt of the detect flow; the later run must win.
        _event(
            conn,
            SCENE_A,
            S1_DALO,
            "fast-sar-detect-objects",
            "old-run",
            "fail",
            start - timedelta(hours=3),
        )
    return Seed(
        ship_hh=ship_hh,
        iceberg_hv=iceberg_hv,
        ship_hv_duplicate=ship_hv_duplicate,
        deleted_target=deleted_target,
        scene_c_target=scene_c_target,
        ais_msg_1=ais_msg_1,
        ais_msg_2=ais_msg_2,
    )
