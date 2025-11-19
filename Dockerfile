# Multi-stage Dockerfile for dbt + data ingestion
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies using uv
RUN uv sync --frozen --no-dev

# Final stage
FROM python:3.11-slim-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy uv virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY . .

# Add virtual environment to PATH
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app:$PYTHONPATH"

# Set dbt profiles directory
ENV DBT_PROFILES_DIR=/app/dbt

# Create entrypoint script
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
echo "Starting UMKM Analytics Pipeline"\n\
\n\
# Run ingestion + dbt pipeline\n\
python ingestion/main.py\n\
\n\
echo "Pipeline completed successfully"\n\
' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Default command
CMD ["/app/entrypoint.sh"]
