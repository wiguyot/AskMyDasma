FROM python:3.12.13-slim-trixie

WORKDIR /app

RUN python -m pip install --upgrade pip==26.0.1 && \
    python -m pip install --no-cache-dir \
      confluent-kafka==2.14.0 \
      fastavro==1.12.1 \
      pandas==3.0.2 \
      attrs==26.1.0 \
      certifi==2026.2.25 \
      httpx==0.28.1 \
      cachetools==7.0.5 \
      authlib==1.6.9

ENTRYPOINT ["python", "/app/producer/producer.py"]
