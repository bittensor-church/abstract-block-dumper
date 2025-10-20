FROM python:3.12-slim-bookworm

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install minimal build dependencies
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set uv environment variables and fallback version for setuptools-scm
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_CACHE_DIR=/tmp/.uv-cache \
    SETUPTOOLS_SCM_PRETEND_VERSION_FOR_ABSTRACT_BLOCK_DUMPER=0.1.0

WORKDIR /app

# Copy workspace files and source code (needed for hatch-vcs to write _version.py)
COPY uv.lock pyproject.toml README.md ./
COPY example_project/pyproject.toml ./example_project/
COPY example_project ./example_project
COPY src ./src
COPY .git .git

# Install workspace dependencies
RUN --mount=type=cache,target=/tmp/.uv-cache \
    uv sync --frozen --no-dev

# Install the project (workspace member)
RUN --mount=type=cache,target=/tmp/.uv-cache \
    uv sync --frozen --no-dev

EXPOSE 8000

# Set working directory to example_project
WORKDIR /app/example_project

# Use uv to run the application
CMD ["uv", "run", "python", "manage.py", "runserver", "0.0.0.0:8000"]
