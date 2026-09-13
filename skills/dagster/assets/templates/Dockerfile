# Code-location image for a uv-managed Dagster project (create-dagster layout).
# OSS: the container serves definitions over gRPC to the webserver and daemon.
# Dagster+ Hybrid: reference the same image from dagster_cloud.yaml / build.yaml instead of using CMD.
FROM python:3.12-slim

# Install uv (pin the tag in production, e.g. ghcr.io/astral-sh/uv:0.8.0).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /opt/dagster/app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv PATH="/opt/venv/bin:$PATH"

# Dependencies first for layer caching, then the project itself.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev
COPY . .
RUN uv sync --frozen --no-dev

# Serve the code location (module = [tool.dg.project].code_location_target_module).
# Point workspace.yaml at this host/port:  load_from: [{grpc_server: {host: ..., port: 4000, location_name: my_project}}]
EXPOSE 4000
CMD ["dagster", "api", "grpc", "--module-name", "my_project.definitions", "--host", "0.0.0.0", "--port", "4000"]
