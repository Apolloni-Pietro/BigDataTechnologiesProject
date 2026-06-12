# Pipeline service: the medallion orchestrator (bronze -> silver -> gold + ML).
FROM python:3.12-slim

# DuckDB httpfs and psycopg2-binary need no system libs, but keeping the image
# lean and predictable: install only what pip needs.
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# The scheduler runs forever; there is no port to expose.
CMD ["python", "main.py"]
