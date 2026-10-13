FROM python:3.12.13-slim-trixie

WORKDIR /app

COPY services/kafka-tools/requirements.txt /tmp/requirements.txt
RUN python -m pip install --upgrade pip==26.0.1 && \
    python -m pip install --no-cache-dir -r /tmp/requirements.txt
