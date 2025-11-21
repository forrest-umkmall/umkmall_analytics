# UMKM Mall Analytics Platform

A modern, cost-effective analytics platform built with dbt and Lightdash, designed for small teams (up to 20 users).

Command to run sample ingestion source:
`uv run --env-file .env.local ingestion/sources/eduqat_enrollments.py`

Command to run db pipeline locally (connected to railway db):
`uv run --env-file .env.local dbt run --project-dir ./dbt --profiles-dir ./dbt`

Command to run single model in pipeline locally (connected to railway db):
`uv run --env-file .env.local dbt run --project-dir ./dbt --profiles-dir ./dbt --select stg_eduqat_enrollments`


## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Railway Platform                         │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐      ┌──────────────┐      ┌───────────┐   │
│  │  PostgreSQL  │ ───► │ dbt Pipeline │ ───► │ Lightdash │   │
│  │   Database   │      │  + Ingestion │      │  Web UI   │   │
│  │              │ ◄─── │   (Cron)     │      │           │   │
│  └──────────────┘      └──────────────┘      └───────────┘   │
│         │                     │                     │        │
│         ▼                     ▼                     ▼        │
│    raw schema            dbt models            Analytics     │
│    staging schema        transformations      Dashboards     │
│    marts schema                                              │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Features

- **Data Ingestion**: Python scripts to pull data from various sources
- **Transformation**: dbt for SQL-based data modeling and transformations
- **Visualization**: Lightdash for self-service analytics and dashboards
- **Orchestration**: Simple Python orchestrator (main.py) for scheduled runs
- **Package Management**: uv for fast, modern Python dependency management
- **Deployment**: Railway for easy, cost-effective cloud hosting

## Project Structure

```
umkmall_analytics/
├── dbt/                          # dbt project
│   ├── models/
│   │   ├── staging/             # Clean, standardized data
│   │   ├── marts/               # Business logic & analytics models
│   │   └── sources.yml          # Source definitions
│   ├── dbt_project.yml
│   └── profiles.yml             # Database connection config
│
├── ingestion/                   # Data ingestion pipeline
│   ├── sources/                 # Individual source ingestion scripts
│   │   └── example_source.py
│   ├── utils/                   # Shared utilities
│   │   └── db.py               # Database helpers
│   └── main.py                 # Orchestrator script
│
├── lightdash/                   # Lightdash configuration
│   ├── docker-compose.yml      # Local development
│   ├── Dockerfile              # Production build
│   └── README.md
│
├── railway/                     # Railway deployment config
│   └── README.md               # Deployment guide
│
├── docker-compose.yml          # Local development environment
├── Dockerfile                  # dbt + ingestion container
├── pyproject.toml             # Python dependencies (uv)
├── .env.example               # Environment variables template
└── README.md                  # This file
```

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager
- Docker & Docker Compose (for local development)
- PostgreSQL (or use Docker)

### Local Development Setup

1. **Clone and install dependencies**

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository
git clone <your-repo-url>
cd umkmall_analytics

# Install dependencies
uv sync
```

2. **Configure environment variables**

```bash
# Copy the example env file
cp .env.example .env

# Edit .env with your settings
nano .env
```

3. **Start the local environment**

```bash
# Start PostgreSQL and Lightdash
docker-compose up -d

# Or start everything including running the pipeline once
docker-compose up
```

4. **Run the data pipeline manually**

```bash
# Activate the virtual environment
source .venv/bin/activate

# Run ingestion + dbt
python ingestion/main.py

# Or run dbt commands directly
cd dbt
dbt deps
dbt build
```

5. **Access Lightdash**

Open http://localhost:8080 in your browser and complete the setup wizard.

## Development Workflow

### Adding a New Data Source

1. Create a new Python file in `ingestion/sources/`:

```python
# ingestion/sources/my_source.py
from utils.db import get_db_connection
import logging

logger = logging.getLogger(__name__)

def ingest_my_data():
    logger.info("Ingesting data from my source")
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Create table in raw schema
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.my_table (
                id SERIAL PRIMARY KEY,
                data VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Insert your data
        # ... your ingestion logic here ...

        conn.commit()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    ingest_my_data()
```

2. The script will automatically be picked up by `main.py`

### Adding dbt Models

1. **Define the source** in `dbt/models/sources.yml`:

```yaml
sources:
  - name: raw
    tables:
      - name: my_table
        columns:
          - name: id
            tests:
              - unique
              - not_null
```

2. **Create a staging model** in `dbt/models/staging/`:

```sql
-- dbt/models/staging/stg_my_table.sql
{{ config(materialized='view') }}

select
    id,
    data,
    created_at
from {{ source('raw', 'my_table') }}
```

3. **Create a mart model** in `dbt/models/marts/`:

```sql
-- dbt/models/marts/mart_my_analysis.sql
{{ config(materialized='table') }}

select
    id,
    data,
    count(*) as record_count
from {{ ref('stg_my_table') }}
group by id, data
```

4. **Run dbt**:

```bash
cd dbt
dbt build
```

## Deployment to Railway

See the detailed [Railway Deployment Guide](railway/README.md) for step-by-step instructions.

**Quick Steps:**

1. Create a Railway project
2. Add PostgreSQL database
3. Deploy dbt + Ingestion service (with cron)
4. Deploy Lightdash service
5. Configure environment variables
6. Set up cron schedule for pipeline

**Estimated Cost:** $10-20/month for 20 users

## Common Commands

```bash
# Install/update dependencies
uv sync

# Run the full pipeline
python ingestion/main.py

# Run dbt commands
cd dbt
dbt deps          # Install dbt dependencies
dbt build         # Run all models and tests
dbt test          # Run tests only
dbt docs generate # Generate documentation
dbt docs serve    # Serve documentation

# Docker commands
docker-compose up -d              # Start services
docker-compose down               # Stop services
docker-compose logs -f lightdash  # View Lightdash logs
docker-compose run pipeline       # Run pipeline once
```

## Database Schemas

- **raw**: Raw data from source systems (managed by ingestion scripts)
- **staging**: Cleaned and standardized data (dbt views)
- **marts**: Business logic and analytics tables (dbt tables)
- **dbt_dev** / **dbt_prod**: dbt metadata and temporary tables

## Environment Variables

See `.env.example` for all available variables.

**Required:**
- `POSTGRES_HOST` - Database host
- `POSTGRES_PORT` - Database port (default: 5432)
- `POSTGRES_USER` - Database user
- `POSTGRES_PASSWORD` - Database password
- `POSTGRES_DB` - Database name
- `LIGHTDASH_SECRET` - Secret key for Lightdash

**Optional:**
- `DBT_TARGET` - dbt target environment (dev/prod)
- `DBT_PROFILES_DIR` - Path to dbt profiles (default: ./dbt)

## Monitoring & Logs

### Local Development
```bash
# View all logs
docker-compose logs -f

# View specific service
docker-compose logs -f lightdash
docker-compose logs -f postgres
```

### Railway Production
- Check Railway dashboard for service logs
- Monitor cron job execution
- Set up alerts for failures

## Troubleshooting

### dbt can't connect to database
- Verify `POSTGRES_*` environment variables
- Check that database is running
- Test connection: `psql -h $POSTGRES_HOST -U $POSTGRES_USER $POSTGRES_DB`

### Lightdash shows "Project not found"
- Ensure dbt project is mounted correctly
- Check `DBT_PROJECT_DIR` environment variable
- Verify dbt project structure is valid

### Ingestion script fails
- Check database connection
- Verify raw schema exists
- Check logs for specific error messages

## Contributing

1. Create a new branch for your feature
2. Add your data sources and dbt models
3. Test locally with `docker-compose`
4. Submit a pull request

## License

[Your License Here]

## Support

For questions or issues, please [open an issue](your-repo-issues-url).