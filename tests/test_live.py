"""Read-only smoke tests against the port-forwarded Holmes DB and Prefect server.

Run with ``uv run pytest -m live`` after starting the port-forwards in README.md and
configuring ``.env``. Nothing here writes: the DB connection is read-only and only
dry-run re-runs and purge previews are exercised.
"""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import Connection

from c_core_health import lineage, prefect_api, purge, rerun, scenes
from c_core_health.db import create_db_engine, read_connection
from c_core_health.models import RerunRequest, SceneRef
from c_core_health.settings import Settings

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def live_settings() -> Settings:
    try:
        return Settings()  # pyright: ignore[reportCallIssue]
    except Exception as exc:
        pytest.skip(f"CCH_* settings not configured: {exc}")


@pytest.fixture(scope="module")
def live_conn(live_settings: Settings) -> Iterator[Connection]:
    engine = create_db_engine(live_settings, read_only=True)
    with read_connection(engine) as conn:
        yield conn
    engine.dispose()


@pytest.fixture(scope="module")
def live_prefect(live_settings: Settings) -> Iterator[httpx.Client]:
    with prefect_api.make_client(live_settings) as client:
        yield client


@pytest.fixture(scope="module")
def processed_scene(live_conn: Connection, live_settings: Settings) -> SceneRef:
    refs, _ = scenes.list_scene_refs(
        live_conn, live_settings, since=datetime.now(UTC) - timedelta(days=3), limit=50
    )
    for summary in scenes.summarize_scenes(live_conn, refs):
        if summary.stats.target_count and not summary.unfinished_flows:
            return SceneRef.model_validate(summary.model_dump())
    pytest.skip("No fully processed scene in the last 3 days")


def test_prefect_reachable(live_prefect: httpx.Client):
    assert prefect_api.read_version(live_prefect).startswith("3.")


def test_lineage_finds_parent(live_conn, live_prefect, live_settings, processed_scene):
    result = lineage.resolve_lineage(
        live_conn, live_prefect, live_settings, processed_scene
    )
    assert result.parent is not None
    assert result.parent.flow_name in live_settings.parent_flow_names
    assert lineage.parameters_mention(result.parent.parameters, processed_scene.scene_id)


def test_rerun_dry_run(live_conn, live_prefect, live_settings, processed_scene):
    result = rerun.rerun_scene(
        live_conn, live_prefect, live_settings, processed_scene, RerunRequest()
    )
    assert result.dry_run
    assert all(p.created_run_id is None for p in result.runs)


def test_purge_preview(live_conn, processed_scene):
    preview = purge.build_preview(
        live_conn,
        processed_scene,
        include_ais_messages=False,
        allow_analyst_data_loss=False,
    )
    assert preview.deletes["features.targets"] > 0
