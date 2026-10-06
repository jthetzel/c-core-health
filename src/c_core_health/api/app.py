import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger

from c_core_health import prefect_api
from c_core_health.api.deps import Resources
from c_core_health.api.routes import router
from c_core_health.db import create_db_engine
from c_core_health.errors import ServiceError
from c_core_health.settings import Settings, get_settings


def configure_logging(settings: Settings) -> None:
    logger.remove()
    logger.add(sys.stderr, level=settings.log_level, serialize=settings.log_json)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    logger.add(
        settings.backup_dir / "audit.jsonl",
        level="INFO",
        serialize=True,
        filter=lambda record: bool(record["extra"].get("audit")),
    )


def build_resources(settings: Settings) -> Resources:
    return Resources(
        settings=settings,
        read_engine=create_db_engine(settings, read_only=True),
        write_engine=(
            create_db_engine(settings, read_only=False)
            if settings.enable_mutations
            else None
        ),
        prefect=prefect_api.make_client(settings),
    )


def create_app(resources: Resources | None = None) -> FastAPI:
    resources = resources or build_resources(get_settings())
    settings = resources.settings

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        configure_logging(settings)
        app.state.resources = resources
        logger.info(
            "c-core-health starting; mutations {}",
            "ENABLED" if settings.enable_mutations else "disabled",
        )
        yield
        resources.prefect.close()
        resources.read_engine.dispose()
        if resources.write_engine is not None:
            resources.write_engine.dispose()

    app = FastAPI(
        title="c-core-health",
        description="Recent SAR scenes, their Prefect run lineage, re-runs and purges.",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", "X-Api-Key", "Authorization"],
    )
    app.include_router(router)

    @app.exception_handler(ServiceError)
    async def service_error(_: Request, exc: ServiceError) -> JSONResponse:  # pyright: ignore[reportUnusedFunction]
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.message, "context": exc.context},
        )

    @app.exception_handler(httpx.HTTPError)
    async def prefect_error(_: Request, exc: httpx.HTTPError) -> JSONResponse:  # pyright: ignore[reportUnusedFunction]
        logger.exception("Prefect API call failed")
        return JSONResponse(
            status_code=502, content={"detail": f"Prefect API error: {type(exc).__name__}"}
        )

    return app


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        create_app(build_resources(settings)), host=settings.host, port=settings.port
    )
