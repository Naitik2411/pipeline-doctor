FROM python:3.11-slim-bookworm

# Install uv (fast, reproducible installs from uv.lock)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

# Install deps first (better layer caching)
COPY pyproject.toml uv.lock ./
COPY services ./services

RUN uv sync --frozen --no-dev

# Default: mock commerce API
EXPOSE 8000
CMD ["uv", "run", "uvicorn", "services.mock_api.app:app", "--host", "0.0.0.0", "--port", "8000"]