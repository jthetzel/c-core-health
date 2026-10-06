from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from c_core_health.models import FlowEventRun, SceneRef, Sensor
from c_core_health.settings import Settings
from tests.fake_prefect import BASE_URL, T0, FakePrefect


def make_settings(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "pg_dsn": "postgresql+psycopg://user:pass@localhost:1/none",
        "prefect_api_url": BASE_URL,
        "backup_dir": tmp_path / "backups",
        **overrides,
    }
    return Settings(_env_file=None, **values)  # pyright: ignore[reportCallIssue]


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return make_settings(tmp_path)


@pytest.fixture
def rcm_ref() -> SceneRef:
    return SceneRef(
        scene_id="RCM1_OK1_PK1_1_SC50MB_20260928_221225_HH_HV_GRD",
        collection="dalo-greenland-rcm",
        sensor=Sensor.RCM,
        acquired_at=T0,
    )


@pytest.fixture(autouse=True)
def _no_dotenv(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in ("CCH_ENABLE_MUTATIONS", "CCH_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    yield


def event_run(flow: str, name: str, minutes: int, last: str = "end") -> FlowEventRun:
    at = T0 + timedelta(minutes=minutes, seconds=6)
    return FlowEventRun(
        flow_name=flow, run_name=name, first_event_at=at, last_event_at=at, last_event=last
    )


@pytest.fixture
def rcm_world(rcm_ref: SceneRef) -> tuple[FakePrefect, dict[str, str], list[FlowEventRun]]:
    """RCM topology observed live: two schedulers, then download and Kpler side by side."""
    fake = FakePrefect()
    sid, col = rcm_ref.scene_id, rcm_ref.collection
    ids: dict[str, str] = {}
    ids["aoi"] = fake.add_run("aoi-triggers", "mysterious-myna")
    ids["search"] = fake.add_run(
        "search-rcm-scene",
        "bald-smilodon",
        parent=ids["aoi"],
        parameters={"params": {"rcm_stac_collection_id": col}},
    )
    ids["download"] = fake.add_run(
        "download-rcm-scene",
        "auburn-pegasus",
        parent=ids["search"],
        minutes=1,
        parameters={"scene_item_id": sid, "stac_collection_id": col, "download_url": "x"},
    )
    ids["kpler"] = fake.add_run(
        "acquire-ais-data-from-kpler-for-scene",
        "magic-peccary",
        parent=ids["search"],
        minutes=1,
        parameters={"params": {"stac_item_id": sid, "stac_collection_id": col}},
    )
    ids["detect"] = fake.add_run(
        "fast-sar-detect-objects",
        "tan-wombat",
        parent=ids["download"],
        minutes=2,
        parameters={"image_package_root_path": f"s3://bucket/{col}/{sid}"},
    )
    ids["chips"] = fake.add_run(
        "generate-image-chips",
        "mature-angelfish",
        parent=ids["detect"],
        minutes=3,
        parameters={"input_model": {"output_stac_item_id": sid}},
        state="CRASHED",
    )
    ids["classify"] = fake.add_run(
        "classify-and-update-targets",
        "warm-salamander",
        parent=ids["detect"],
        minutes=4,
        parameters={"image_scene_id": sid, "stac_collection_id": col},
    )
    ids["classify_other_scene"] = fake.add_run(
        "classify-and-update-targets",
        "warm-salamander",
        minutes=-5000,
        parameters={"image_scene_id": "RCM2_SOMETHING_ELSE", "stac_collection_id": col},
    )
    events = [
        event_run("download-rcm-scene", "auburn-pegasus", 1),
        event_run("acquire-ais-data-from-kpler-for-scene", "magic-peccary", 1),
        event_run("fast-sar-detect-objects", "tan-wombat", 2),
        event_run("classify-and-update-targets", "warm-salamander", 4),
        event_run("detect-oil-spill", "ghost-run", 5, last="start"),
    ]
    return fake, ids, events
