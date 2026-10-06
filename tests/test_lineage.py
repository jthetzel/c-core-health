import pytest

from c_core_health import lineage, prefect_api
from c_core_health.models import FlowEventRun, SceneRef
from c_core_health.settings import Settings
from tests.fake_prefect import FakePrefect


def resolve(
    fake: FakePrefect,
    events: list[FlowEventRun],
    settings: Settings,
    ref: SceneRef,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(lineage, "scene_flow_event_runs", lambda _conn, _ref: events)
    with fake.mock(), prefect_api.make_client(settings) as client:
        return lineage.resolve_lineage(None, client, settings, ref)  # pyright: ignore[reportArgumentType]


def test_parent_is_download_below_schedulers(rcm_world, settings, rcm_ref, monkeypatch):
    fake, ids, events = rcm_world
    result = resolve(fake, events, settings, rcm_ref, monkeypatch)

    assert result.parent is not None
    assert str(result.parent.id) == ids["download"]
    assert {str(r.id) for r in result.roots} == {ids["download"], ids["kpler"]}
    assert [str(r.id) for r in result.sibling_roots] == [ids["kpler"]]
    run_ids = {str(r.id) for r in result.runs}
    assert ids["aoi"] not in run_ids and ids["search"] not in run_ids


def test_descendants_without_events_are_found(rcm_world, settings, rcm_ref, monkeypatch):
    fake, ids, events = rcm_world
    result = resolve(fake, events, settings, rcm_ref, monkeypatch)

    by_id = {str(r.id): r for r in result.runs}
    assert ids["chips"] in by_id
    assert by_id[ids["chips"]].state_type == "CRASHED"
    assert str(by_id[ids["classify"]].parent_flow_run_id) == ids["detect"]
    assert str(by_id[ids["detect"]].parent_flow_run_id) == ids["download"]


def test_reused_run_name_resolves_to_run_about_this_scene(
    rcm_world, settings, rcm_ref, monkeypatch
):
    fake, ids, events = rcm_world
    result = resolve(fake, events, settings, rcm_ref, monkeypatch)

    run_ids = {str(r.id) for r in result.runs}
    assert ids["classify"] in run_ids
    assert ids["classify_other_scene"] not in run_ids


def test_unmatched_events_and_active_runs_are_reported(
    rcm_world, settings, rcm_ref, monkeypatch
):
    fake, ids, events = rcm_world
    fake.runs[ids["chips"]]["state_type"] = "RUNNING"
    result = resolve(fake, events, settings, rcm_ref, monkeypatch)

    assert [(u.flow_name, u.run_name) for u in result.unmatched_events] == [
        ("detect-oil-spill", "ghost-run")
    ]
    assert [str(i) for i in result.active_run_ids] == [ids["chips"]]


def test_tagged_rerun_becomes_parent_with_batch_sibling(
    rcm_world, settings, rcm_ref, monkeypatch
):
    fake, ids, events = rcm_world
    tags = [lineage.SERVICE_TAG, lineage.scene_tag(rcm_ref), lineage.batch_tag("b1")]
    new_download = fake.add_run(
        "download-rcm-scene",
        "new-download",
        minutes=100,
        parameters=fake.runs[ids["download"]]["parameters"],
        tags=tags,
    )
    new_kpler = fake.add_run(
        "acquire-ais-data-from-kpler-for-scene",
        "new-kpler",
        minutes=100,
        parameters=fake.runs[ids["kpler"]]["parameters"],
        tags=tags,
    )
    result = resolve(fake, events, settings, rcm_ref, monkeypatch)

    assert result.parent is not None
    assert str(result.parent.id) == new_download
    assert [str(r.id) for r in result.sibling_roots] == [new_kpler]


def test_scene_without_runs(settings, rcm_ref, monkeypatch):
    result = resolve(FakePrefect(), [], settings, rcm_ref, monkeypatch)
    assert result.parent is None
    assert result.runs == []


@pytest.mark.parametrize(
    ("parameters", "expected"),
    [
        ({"image_id": "SCENE_A.SAFE"}, True),
        ({"params": {"stac_item_id": "SCENE_A"}}, True),
        ({"paths": ["s3://b/c/SCENE_A/x.tif"]}, True),
        ({"image_id": "SCENE_B"}, False),
        ({"count": 3, "flag": None}, False),
    ],
)
def test_parameters_mention(parameters, expected):
    assert lineage.parameters_mention(parameters, "SCENE_A") is expected
