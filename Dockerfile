FROM python:3.13-slim AS build
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.13-slim
RUN useradd --create-home --uid 10001 app \
    && mkdir -p /data/backups \
    && chown -R app /data
COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" \
    CCH_HOST=0.0.0.0 \
    CCH_PORT=8000 \
    CCH_LOG_JSON=true \
    CCH_BACKUP_DIR=/data/backups
USER app
EXPOSE 8000
CMD ["c-core-health"]
