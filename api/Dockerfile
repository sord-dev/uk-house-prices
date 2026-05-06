FROM python:3.12-slim

WORKDIR /

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY api/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ingest/ /app/
COPY api/ /api/

ENV PYTHONPATH=/app

RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app /api
USER app

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--app-dir", "/"]
