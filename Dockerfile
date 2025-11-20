# Single-stage Dockerfile for dbt + data ingestion
FROM python:3.11-slim-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml ./

# Install dependencies using pip
RUN pip install --no-cache-dir \
    "dbt-core>=1.10.15" \
    "dbt-postgres>=1.9.1" \
    "psycopg2-binary>=2.9.9" \
    "python-dotenv>=1.0.0" \
    "pandas>=2.1.0" \
    "sqlalchemy>=2.0.0" \
    "requests>=2.31.0" \
    "google-auth>=2.23.0" \
    "google-auth-oauthlib>=1.1.0" \
    "google-api-python-client>=2.100.0"

# Copy application code
COPY . .

# Set PYTHONPATH to include app directory
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
