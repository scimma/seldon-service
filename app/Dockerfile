# syntax=docker/dockerfile:1
#
# Two images from one file:
#
# - service (the default target): the Django service for `app` and
#   `consumer`. No torch, no seldon_core, no model package; it carries only
#   the standard-library contract package so it can read what models return.
# - worker: the service plus exactly one model package, chosen by the
#   MODEL_PACKAGE build argument (a directory under packages/), and the
#   runtime that package needs. Run it as `python manage.py forecast_worker`.
#
# app/ is the build context; packages/ is read through the BuildKit named
# context `packages`. run/seldonctl and docker/docker-compose.dev.yaml set
# both. Directly:
#   docker buildx build --target service --build-context packages=packages \
#     -t seldon-service app/
#   GH_BUILD_TOKEN=$(gh auth token) docker buildx build --target worker \
#     --build-context packages=packages \
#     --build-arg MODEL_PACKAGE=seldon-reference-model \
#     --secret id=gh_token,env=GH_BUILD_TOKEN \
#     -t seldon-forecaster-reference app/
# The private seldon_core repository is reached through that BuildKit secret,
# never a build arg or a credential baked into a layer.

FROM python:3.13-slim AS base

RUN pip install --no-cache-dir --upgrade pip

COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# The contract package is standard-library only.
COPY --from=packages seldon-model-contract /src/seldon-model-contract
RUN pip install --no-cache-dir /src/seldon-model-contract && rm -rf /src


FROM base AS worker-deps

ARG TORCH_VERSION=2.9.0+cpu
ARG LIGHTNING_VERSION=2.5.2
ARG SELDON_CORE_REF=v1.2.0
ARG MODEL_PACKAGE=seldon-reference-model
# Fixes the timestamps inside the model package's wheel, so rebuilding an
# unchanged package gives the same wheel and the same digest.
ARG SOURCE_DATE_EPOCH=315532800

RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# CPU-only torch from the CPU index, at an exact version. torch is kept out of
# requirements.txt so nothing later can resolve a CUDA build over it.
RUN pip install --no-cache-dir "torch==${TORCH_VERSION}" \
    --index-url https://download.pytorch.org/whl/cpu \
    && pip install --no-cache-dir "lightning==${LIGHTNING_VERSION}"

# The token reaches git through a URL rewrite held in this command's
# environment only, so the URL pip prints in the build log carries no
# credential and nothing is written to a file.
RUN --mount=type=secret,id=gh_token,required=true \
    GIT_CONFIG_COUNT=1 \
    GIT_CONFIG_KEY_0="url.https://x-access-token:$(cat /run/secrets/gh_token)@github.com/skai-institute/.insteadOf" \
    GIT_CONFIG_VALUE_0="https://github.com/skai-institute/" \
    pip install --no-cache-dir \
    "seldon_core @ git+https://github.com/skai-institute/Transient-Foundation-Model.git@${SELDON_CORE_REF}"

# Install the model package from its wheel and record the wheel's sha256: the
# worker registers its deployment under that digest. The package's own tests
# go into the image so they can run against the installed wheel.
COPY --from=packages ${MODEL_PACKAGE} /src/model
RUN mkdir -p /opt/seldon/model \
    && SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH} \
       pip wheel --no-cache-dir --no-deps --wheel-dir /wheels /src/model \
    && pip install --no-cache-dir /wheels/*.whl \
    && printf 'sha256:%s\n' "$(sha256sum /wheels/*.whl | cut -d' ' -f1)" \
       > /opt/seldon/model/wheel.sha256 \
    && if [ -d /src/model/tests ]; then cp -r /src/model/tests /opt/seldon/model/tests; fi \
    && rm -rf /src /wheels


FROM python:3.13-slim AS worker

# TORCH_COMPILE_DISABLE and MPLBACKEND must be set before the library is
# imported: it compiles at construction and imports matplotlib.pyplot at module
# scope. OMP_NUM_THREADS bounds torch's thread pool before Django starts;
# keep it equal to SELDON_TORCH_THREADS and the container's CPU limit.
ENV PYTHONUNBUFFERED=1 \
    SELDON_IMAGE_TARGET=worker \
    TORCH_COMPILE_DISABLE=1 \
    MPLBACKEND=Agg \
    OMP_NUM_THREADS=1 \
    SELDON_TORCH_THREADS=1

COPY --from=worker-deps /usr/local/lib/python3.13/site-packages/ /usr/local/lib/python3.13/site-packages/
COPY --from=worker-deps /usr/local/bin/ /usr/local/bin/
COPY --from=worker-deps /opt/seldon/ /opt/seldon/

RUN mkdir /app
COPY . /app
WORKDIR /app

CMD ["python", "manage.py", "forecast_worker"]


FROM base AS service

ENV PYTHONUNBUFFERED=1 \
    SELDON_IMAGE_TARGET=service

RUN mkdir /app
COPY . /app
WORKDIR /app

CMD ["bash", "entrypoints/docker-entrypoint.app.sh"]
