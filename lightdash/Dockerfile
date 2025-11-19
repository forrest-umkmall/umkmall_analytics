# Dockerfile for Lightdash with embedded dbt project
# This is for production deployment (Railway)

FROM lightdash/lightdash:latest

# Copy dbt project into the container
COPY ../dbt /usr/app/dbt

# Set environment variable for dbt project location
ENV DBT_PROJECT_DIR=/usr/app/dbt

# Expose the Lightdash port
EXPOSE 8080

# The base image already has the correct entrypoint
