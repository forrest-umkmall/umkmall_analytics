# Lightdash Configuration

This directory contains the Lightdash configuration for the UMKM Mall Analytics platform.

## Local Development

To run Lightdash locally with PostgreSQL:

```bash
cd lightdash
docker-compose up -d
```

This will start:
- PostgreSQL database on port 5432
- Lightdash on port 8080

Access Lightdash at: http://localhost:8080

## Production Deployment (Railway)

For Railway deployment, the Dockerfile is used to build a container that includes the dbt project.

The Lightdash service will connect to the Railway PostgreSQL database using environment variables.

## Environment Variables

Required environment variables (set in Railway or .env):

- `POSTGRES_HOST` - Database host
- `POSTGRES_PORT` - Database port (default: 5432)
- `POSTGRES_USER` - Database user
- `POSTGRES_PASSWORD` - Database password
- `POSTGRES_DB` - Database name
- `LIGHTDASH_SECRET` - Secret key for Lightdash (generate a random string)

## First Time Setup

1. Access Lightdash web UI
2. Create an admin account
3. Connect to your dbt project (already mounted at /usr/app/dbt)
4. Sync your dbt models
5. Start creating charts and dashboards!

## Notes

- The dbt project is mounted as read-only in the container
- Lightdash reads the dbt models but doesn't modify them
- Run dbt transformations separately (via the ingestion pipeline)
- Lightdash will automatically detect new models after dbt runs
