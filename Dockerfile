# Образ сервиса cadastre-etl: `docker compose up -d etl` (см. compose.yaml).
#
# Колёса shapely, pyproj и psycopg[binary] готовые — системные библиотеки не нужны.
# Зависимости ставятся отдельным слоем до кода, чтобы правка кода не пересобирала их.

FROM python:3.12-slim

# uv — из PyPI: не нужен второй реестр (ghcr.io доступен не из всякой сети).
RUN pip install --no-cache-dir "uv>=0.8,<0.9"

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev

# Непривилегированный пользователь; кэш НСПД — на томе /app/.cache.
RUN useradd --system --uid 10001 etl && mkdir -p /app/.cache && chown etl /app/.cache
USER etl

ENV PATH="/app/.venv/bin:$PATH" \
    NSPD_CACHE_PATH=/app/.cache/nspd.sqlite

ENTRYPOINT ["python", "-m", "cadastre_etl"]
CMD ["run", "--loop"]
