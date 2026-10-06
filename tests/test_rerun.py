import pytest

from c_core_health import lineage, prefect_api, rerun
from c_core_health.errors import ConflictError
from c_core_health.models import RerunRequest
from tests.fake_prefect import FakePrefect


def test_build_rerun_parameters_only_applies_declared_overrides():
    original = {"scene_item_id": "S", "nested": {"a": 1}}
    result = rerun.build_rerun_parameters(
        original, {"scene_item_id", "force"}, {"force": True, "check_flow_status": False}
    )
    assert result == {"scene_item_id": "S", "nested": {"a": 1}, "force": True}
    result["nested"]["a"] = 2
    assert original["nested"]["a"] == 1


def run_rerun(fake, events, settings, ref, monkeypatch, request):
    monkeypatch.setattr(lineage, "scene_flow_event_runs", lambda _conn, _ref: events)
    with fake.mock(), prefect_api.make_client(settings) as client:
        return rerun.rerun_scene(None, client, settings, ref, request)  # pyright: ignore[reportArgumentType]


def test_dry_run_plans_parent_with_force_and_creates_nothing(
    rcm_world, settings, rcm_ref, monkeypatch
):
    fake, ids, events = rcm_world
    result = run_rerun(fake, events, settings, rcm_ref, monkeypatch, RerunRequest())

    assert result.dry_run
    [plan] = result.runs
    assert str(plan.source_run_id) == ids["download"]
    assert plan.flow_name == "download-rcm-scene"
    assert plan.parameters["force"] is True
    assert plan.parameters["scene_item_id"] == rcm_ref.scene_id
    assert lineage.scene_tag(rcm_ref) in plan.tags
    assert fake.created == []


def test_rerun_creates_runs_idempotently(rcm_world, settings, rcm_ref, monkeypatch):
    fake, ids, events = rcm_world
    request = RerunRequest(dry_run=False, include_sibling_roots=True)
    first = run_rerun(fake, events, settings, rcm_ref, monkeypatch, request)

    assert [p.flow_name for p in first.runs] == [
        "download-rcm-scene",
        "acquire-ais-data-from-kpler-for-scene",
    ]
    assert all(p.created_run_id is not None for p in first.runs)
    assert len(fake.created) == 2
    assert fake.created[0]["idempotency_key"] == f"c-core-health:rerun:{ids['download']}"

    # The new runs are SCHEDULED, so a second click is refused rather than duplicated.
    with pytest.raises(ConflictError, match="active"):
        run_rerun(fake, events, settings, rcm_ref, monkeypatch, request)
    assert len(fake.created) == 2


def test_refuses_when_runs_are_active(rcm_world, settings, rcm_ref, monkeypatch):
    fake, ids, events = rcm_world
    fake.runs[ids["classify"]]["state_type"] = "RUNNING"
    with pytest.raises(ConflictError, match="active"):
        run_rerun(fake, events, settings, rcm_ref, monkeypatch, RerunRequest())


def test_refuses_parent_that_does_not_reference_scene(
    rcm_world, settings, rcm_ref, monkeypatch
):
    fake, ids, events = rcm_world
    fake.runs[ids["download"]]["parameters"] = {"scene_item_id": "SOME_OTHER_SCENE"}
    with pytest.raises(ConflictError, match="does not reference the scene"):
        run_rerun(fake, events, settings, rcm_ref, monkeypatch, RerunRequest())


def test_refuses_when_no_parent_found(settings, rcm_ref, monkeypatch):
    with pytest.raises(ConflictError, match="No per-scene parent"):
        run_rerun(FakePrefect(), [], settings, rcm_ref, monkeypatch, RerunRequest())
