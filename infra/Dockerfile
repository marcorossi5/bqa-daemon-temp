###############
# builder stage
# prepares python production environment
###############
FROM python:3.12-slim AS builder

WORKDIR /app/

SHELL ["/bin/bash", "-c"]
ENV VENV=/app/.venv \
    UV_CACHE_DIR=/app/uv_cache

RUN python3 -m venv $VENV
ENV PATH="${VENV}/bin:${PATH}" \
    VIRTUAL_ENV="${VENV}"

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install uv

COPY ["pyproject.toml", "uv.lock", "./"]
RUN --mount=type=cache,target=$UV_CACHE_DIR \
        uv sync --no-install-project --no-dev


###############
# production stage
# loads the production environment, copy source files and sets image entrypoint
###############
FROM python:3.12-slim AS prod

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        openssh-client \
        curl \
        bc; \
    rm -rf /var/lib/apt/lists/*

ARG UID
ARG GID

RUN groupadd -g "${GID}" dockerUser \
  && useradd --no-log-init -u "${UID}" -g "${GID}" dockerUser

USER dockerUser

# load production environment
ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app/

# copy lib files
COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

# copy source files
COPY src/ src/

# launch production server
CMD ["python", "-m", "src.main"]

###############
# builder dev stage
# loads the production environment, add local group packages
###############
FROM builder AS builder_dev

RUN --mount=type=cache,target=$UV_CACHE_DIR \
        uv sync --no-install-project --group local

###############
# dev stage
# loads the production environment, copy source files and sets image entrypoint
###############
FROM python:3.12-slim AS dev

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        openssh-client \
        curl \
        bc; \
    rm -rf /var/lib/apt/lists/*

# load production environment
ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app/

# copy lib files
COPY --from=builder_dev ${VIRTUAL_ENV} ${VIRTUAL_ENV}

# copy source files
COPY src/ src/

# launch production server
CMD ["python", "-m", "src.main"]
