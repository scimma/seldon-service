# Build:
#   run/seldonctl slim_dev up
# or directly, with the private seldon_core repository reached through a
# BuildKit secret (never a build arg or a credential baked into a layer):
#   GH_BUILD_TOKEN=$(gh auth token) docker build \
#     --secret id=gh_token,env=GH_BUILD_TOKEN -t seldon-service app/
FROM python:3.13-slim AS deps

ARG TORCH_VERSION=2.9.0+cpu
ARG SELDON_CORE_REF=v1.2.0

RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip

# CPU-only torch first, from the CPU index, at an exact version. torch is kept
# out of requirements.txt so nothing later can resolve a CUDA build over it.
RUN pip install --no-cache-dir "torch==${TORCH_VERSION}" \
    --index-url https://download.pytorch.org/whl/cpu

COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN --mount=type=secret,id=gh_token,required=true \
    pip install --no-cache-dir \
    "seldon_core @ git+https://x-access-token:$(cat /run/secrets/gh_token)@github.com/skai-institute/Transient-Foundation-Model.git@${SELDON_CORE_REF}"

FROM python:3.13-slim

# TORCH_COMPILE_DISABLE and MPLBACKEND must be set before the library is
# imported: it compiles at construction and imports matplotlib.pyplot at module
# scope. OMP_NUM_THREADS bounds torch's thread pool before Django starts;
# keep it equal to SELDON_TORCH_THREADS and the container's CPU limit.
ENV PYTHONUNBUFFERED=1 \
    TORCH_COMPILE_DISABLE=1 \
    MPLBACKEND=Agg \
    OMP_NUM_THREADS=1 \
    SELDON_TORCH_THREADS=1

COPY --from=deps /usr/local/lib/python3.13/site-packages/ /usr/local/lib/python3.13/site-packages/
COPY --from=deps /usr/local/bin/ /usr/local/bin/

RUN mkdir /app
COPY . /app
WORKDIR /app

CMD ["bash", "entrypoints/docker-entrypoint.app.sh"]
