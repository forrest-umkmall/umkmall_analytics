# Dockerfile for Lightdash with embedded dbt project
# This is for production deployment (Railway)
#
# IMPORTANT: Build context must be the PROJECT ROOT, not lightdash/
# In Railway: Set "Root Directory" to "/" (or leave empty) and "Dockerfile Path" to "lightdash/Dockerfile"

FROM lightdash/lightdash:latest

# Set working directory for dbt project
WORKDIR /usr/app/dbt

# Copy dbt project files from project root
# This requires build context to be project root
COPY dbt/ ./

# Install dbt to compile the project and generate manifest.json
USER root
RUN pip install --no-cache-dir dbt-core dbt-postgres

# Generate manifest.json at build time
# dbt parse generates the manifest without needing database connection
RUN dbt deps --profiles-dir . && \
    dbt parse --profiles-dir .

# Set environment variables for Lightdash
ENV DBT_PROJECT_DIR=/usr/app/dbt

# Switch back to lightdash user for security
USER lightdash

# Expose the Lightdash port
EXPOSE 8080

# The base image already has the correct entrypoint