# python:3.12-slim is a minimal Debian image with Python 3.12 pre-installed.
# It is ~50 MB vs ~1 GB for the full python:3.12 image.
# "slim" removes documentation, test files, and package manager caches.
FROM python:3.12-slim

WORKDIR /app
# Sets the working directory inside the container. Every subsequent
# COPY, RUN, and CMD uses this as the current directory.
# The directory is created if it does not exist.

# Install system-level dependencies first.
# These are C libraries that some Python packages compile against.
# gcc:       required to compile psycopg2 from source
# libpq-dev: PostgreSQL client headers, required by psycopg2
# curl:      used by the Docker healthcheck for the api and dashboard services
# --no-install-recommends: do not install suggested packages (keeps image small)
# rm -rf /var/lib/apt/lists/*: delete the package list cache after installing
# (it is not needed at runtime and wastes ~30 MB)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy ONLY requirements.txt before copying the rest of the source.
# Why? Docker builds images in layers. Each instruction is a layer.
# If you copy everything first and then run pip install, Docker
# re-runs pip install every time ANY source file changes.
# By copying requirements.txt first and installing, that layer is
# cached and only invalidated when requirements.txt itself changes.
# This makes rebuilds after code changes much faster.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# --no-cache-dir: do not write pip's download cache to the image.
# Saves ~50-100 MB of cache files that are useless at runtime.

# Now copy the actual source code.
# This layer is invalidated on every code change, but since the
# pip install layer above is cached, rebuilds are fast.
COPY . .

# Create a non-root user and switch to it.
# Running as root inside a container is a security risk — if the
# container is ever compromised, the attacker has root inside it.
# This is standard practice for production containers.
RUN useradd --create-home appuser && chown -R appuser /app
USER appuser

# The CMD is overridden per-service. See each service section below.
# The ingestion, consumer, and api Dockerfiles all have a CMD here.

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
# headless=true disables the browser auto-open prompt,
# which is useless inside a container.