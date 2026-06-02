FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYARROW_IGNORE_TIMEZONE=1 \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python \
    HADOOP_HOME=/opt/hadoop

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    tini \
    curl \
    git \
    procps \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/hadoop

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /workspace/air-quality-ml

COPY requirements.txt pyproject.toml /workspace/air-quality-ml/
COPY src /workspace/air-quality-ml/src

RUN python -m pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pip install -e .

COPY . /workspace/air-quality-ml

ENTRYPOINT ["tini", "--"]
CMD ["python", "jobs/build_gold_features_targets.py"]
