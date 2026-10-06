import secrets
from collections.abc import Generator
from dataclasses import dataclass
from typing import Annotated

import httpx
from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy import Connection, Engine

from c_core_health.db import read_connection
from c_core_health.errors import MutationsDisabledError
from c_core_health.settings import Settings


@dataclass(frozen=True)
class Resources:
    settings: Settings
    read_engine: Engine
    write_engine: Engine | None
    prefect: httpx.Client


def get_resources(request: Request) -> Resources:
    return request.app.state.resources


ResourcesDep = Annotated[Resources, Depends(get_resources)]


def get_settings(resources: ResourcesDep) -> Settings:
    return resources.settings


def get_connection(resources: ResourcesDep) -> Generator[Connection]:
    with read_connection(resources.read_engine) as conn:
        yield conn


def get_prefect(resources: ResourcesDep) -> httpx.Client:
    return resources.prefect


SettingsDep = Annotated[Settings, Depends(get_settings)]
ConnectionDep = Annotated[Connection, Depends(get_connection)]
PrefectDep = Annotated[httpx.Client, Depends(get_prefect)]
ApiKeyHeader = Annotated[str | None, Header(alias="X-Api-Key")]


def authorize_mutation(resources: Resources, api_key: str | None) -> Engine:
    """Gate for anything that writes to Postgres or creates Prefect runs.

    Returns the write engine so callers cannot obtain it without passing the gate.
    """
    settings = resources.settings
    if not settings.enable_mutations or resources.write_engine is None:
        raise MutationsDisabledError(
            "Mutations are disabled; set CCH_ENABLE_MUTATIONS=true and CCH_API_KEY"
        )
    if settings.api_key is None:
        raise MutationsDisabledError("Mutations need CCH_API_KEY to be configured")
    if api_key is None or not secrets.compare_digest(
        api_key.encode(), settings.api_key.get_secret_value().encode()
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing or invalid X-Api-Key"
        )
    return resources.write_engine
