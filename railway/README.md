# Railway Deployment Guide

This guide explains how to deploy the UMKM Analytics platform to Railway.

## Architecture

The deployment consists of three Railway services:

1. **PostgreSQL Database** - Railway built-in Postgres
2. **dbt + Ingestion Pipeline** - Cron job service
3. **Lightdash** - Web service for analytics UI

## Deployment Steps

### 1. Create Railway Project

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login to Railway
railway login

# Create a new project
railway init
```

### 2. Add PostgreSQL Database

In the Railway dashboard:
1. Click "New Service"
2. Select "Database" → "PostgreSQL"
3. Note the connection details (automatically set as environment variables)

### 3. Deploy dbt + Ingestion Service

This service runs on a cron schedule to ingest data and run dbt transformations.

```bash
# Deploy from root directory
railway up
```

**Environment Variables to Set:**
- `POSTGRES_HOST` (from Railway Postgres)
- `POSTGRES_PORT` (from Railway Postgres)
- `POSTGRES_USER` (from Railway Postgres)
- `POSTGRES_PASSWORD` (from Railway Postgres)
- `POSTGRES_DB` (from Railway Postgres)
- `DBT_TARGET=prod`

**Cron Configuration:**
- In Railway dashboard, go to Settings → Cron
- Set schedule (e.g., `0 */6 * * *` for every 6 hours)
- This will run the ingestion + dbt pipeline on schedule

### 4. Deploy Lightdash Service

Create a new service for Lightdash:

```bash
# From the lightdash directory
cd lightdash
railway up
```

**Environment Variables to Set:**
- `POSTGRES_HOST` (same as above)
- `POSTGRES_PORT` (same as above)
- `POSTGRES_USER` (same as above)
- `POSTGRES_PASSWORD` (same as above)
- `POSTGRES_DB` (same as above)
- `LIGHTDASH_SECRET` (generate a random secret: `openssl rand -hex 32`)
- `SECURE_COOKIES=true` (for production)
- `TRUST_PROXY=true` (for Railway)
- `PORT=8080`

**Service Settings:**
- Type: Web Service
- Port: 8080
- Always running (not cron)

## Cost Optimization Tips

For 20 users, you can keep costs low:

1. **Database**: Use Railway's Starter plan ($5/month with 1GB storage)
2. **dbt + Ingestion**: Runs on cron, only active during scheduled runs (minimal cost)
3. **Lightdash**: Small instance (512MB RAM should be enough)

Estimated monthly cost: ~$10-20

## Environment Variables Reference

### Database (Set by Railway automatically)
- `DATABASE_URL` - Full connection string

### Application
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `DBT_TARGET` - `prod` or `dev`

### Lightdash
- `LIGHTDASH_SECRET` - Random secret key
- `SECURE_COOKIES` - `true` for production
- `TRUST_PROXY` - `true` for Railway
- `DBT_PROJECT_DIR` - `/usr/app/dbt` (set in Dockerfile)

## Monitoring

- Railway provides logs for all services
- Check logs after cron runs to ensure pipeline succeeds
- Monitor Lightdash service for any errors

## Troubleshooting

### dbt fails to connect to database
- Verify all `POSTGRES_*` environment variables are set correctly
- Check that database service is running

### Lightdash can't find dbt project
- Ensure `DBT_PROJECT_DIR` is set to `/usr/app/dbt`
- Verify dbt files are copied in Dockerfile

### Cron job not running
- Check Railway cron configuration in service settings
- Verify entrypoint.sh is executable
- Check service logs for errors
