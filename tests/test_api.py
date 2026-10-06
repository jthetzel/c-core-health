from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from c_core_health import prefect_api
from c_core_health.api.app import create_app
from c_core_health.api.deps import Resources, get_connection
from tests.conftest import make_settings

SCENE = "RCM1_OK1_PK1_1_SC50MB_20260928_221225_HH_HV_GRD"


def client_for(tmp_path: Path, **overrides: Any) -> Iterator[tuple[TestClient, MagicMock]]:
    settings = make_settings(tmp_path, **overrides)
    lazy_engine = create_engine(settings.pg_dsn.get_secret_value())  # never connects
    resources = Resources(
        settings=settings,
        read_engine=lazy_engine,
        write_engine=lazy_engine if settings.enable_mutations else None,
        prefect=prefect_api.make_client(settings),
    )
    app = create_app(resources)
    conn = MagicMock(name="connection")
    app.dependency_overrides[get_connection] = lambda: conn
    with TestClient(app) as client:
        yield client, conn


@pytest.fixture
def api(tmp_path: Path) -> Iterator[tuple[TestClient, MagicMock]]:
    yield from client_for(tmp_path)


@pytest.fixture
def api_with_mutations(tmp_path: Path) -> Iterator[tuple[TestClient, MagicMock]]:
    yield from client_for(
        tmp_path,
        enable_mutations=True,
        api_key="s3cret",
        cors_origins=["http://dash.test"],
    )


def test_healthz(api):
    client, _ = api
    assert client.get("/healthz").json() == {"status": "ok"}


@pytest.mark.parametrize(
    ("path", "body"),
    [
        (f"/scenes/{SCENE}/rerun", {"dry_run": False}),
        (f"/scenes/{SCENE}/purge", {"dry_run": False, "confirm_scene_id": SCENE}),
    ],
)
def test_mutations_disabled_by_default(api, path, body):
    client, conn = api
    response = client.post(path, json=body, headers={"X-Api-Key": "anything"})
    assert response.status_code == 403
    conn.execute.assert_not_called()


@pytest.mark.parametrize("headers", [{}, {"X-Api-Key": "wrong"}])
def test_mutations_require_valid_api_key(api_with_mutations, headers):
    client, conn = api_with_mutations
    response = client.post(
        f"/scenes/{SCENE}/rerun", json={"dry_run": False}, headers=headers
    )
    assert response.status_code == 401
    conn.execute.assert_not_called()


def test_mutations_refused_without_configured_key(tmp_path):
    for client, _ in client_for(tmp_path, enable_mutations=True):
        response = client.post(
            f"/scenes/{SCENE}/rerun", json={"dry_run": False}, headers={"X-Api-Key": "x"}
        )
        assert response.status_code == 403


def test_cors_allows_configured_origin_only(api_with_mutations):
    client, _ = api_with_mutations
    allowed = client.options(
        "/scenes",
        headers={"Origin": "http://dash.test", "Access-Control-Request-Method": "GET"},
    )
    assert allowed.headers["access-control-allow-origin"] == "http://dash.test"
    denied = client.options(
        "/scenes",
        headers={"Origin": "http://evil.test", "Access-Control-Request-Method": "GET"},
    )
    assert "access-control-allow-origin" not in denied.headers


def test_limit_validation(api):
    client, _ = api
    assert client.get("/scenes?limit=0").status_code == 422
