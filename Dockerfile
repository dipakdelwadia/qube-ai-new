# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container
COPY . .

# Create a non-root user
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

# Expose the port the app runs on
EXPOSE 8502

# Define environment variables for the database and API keys
# These will be passed in when you run the container
ENV GOOGLE_API_KEY=""

# Default database connection
ENV MSSQL_HOST=""
ENV MSSQL_USER=""
ENV MSSQL_PASSWORD=""
ENV MSSQL_DB=""
ENV MSSQL_PORT="1433"

# DEV environment database connection
ENV DEV_MSSQL_HOST=""
ENV DEV_MSSQL_USER=""
ENV DEV_MSSQL_PASSWORD=""
ENV DEV_MSSQL_DB="EV1_WEB_OPRS_DEMO_DEV"
ENV DEV_MSSQL_PORT="1433"

# UAT environment database connection
ENV UAT_MSSQL_HOST=""
ENV UAT_MSSQL_USER=""
ENV UAT_MSSQL_PASSWORD=""
ENV UAT_MSSQL_DB="EV1_WEB_OPRS_DEMO_UAT"
ENV UAT_MSSQL_PORT="1433"

# QA environment database connection
ENV QA_MSSQL_HOST=""
ENV QA_MSSQL_USER=""
ENV QA_MSSQL_PASSWORD=""
ENV QA_MSSQL_DB="EV1_WEB_OPRS_DEMO_QA"
ENV QA_MSSQL_PORT="1433"

# DEMO environment database connection
ENV DEMO_MSSQL_HOST=""
ENV DEMO_MSSQL_USER=""
ENV DEMO_MSSQL_PASSWORD=""
ENV DEMO_MSSQL_DB="EV1_WEB_OPRS_DEMO_PROD"
ENV DEMO_MSSQL_PORT="1433"

# NEWDEMO environment database connection
ENV NEWDEMO_MSSQL_HOST=""
ENV NEWDEMO_MSSQL_USER=""
ENV NEWDEMO_MSSQL_PASSWORD=""
ENV NEWDEMO_MSSQL_DB="ETest_PRID"
ENV NEWDEMO_MSSQL_PORT="1433"

# PRID-QA environment database connection
ENV PRID-QA_MSSQL_HOST=""
ENV PRID-QA_MSSQL_USER=""
ENV PRID-QA_MSSQL_PASSWORD=""
ENV PRID-QA_MSSQL_DB="ETest_PRID"
ENV PRID-QA_MSSQL_PORT="1433"

ENV PRID-UAT_MSSQL_HOST=""
ENV PRID-UAT_MSSQL_USER=""
ENV PRID-UAT_MSSQL_PASSWORD=""
ENV PRID-UAT_MSSQL_DB="EUAT_PRID"
ENV PRID-UAT_MSSQL_PORT="1433"

ENV PYTHONUNBUFFERED=1

# Command to run the application using the Gunicorn production server
CMD ["gunicorn", "--log-level", "info", "-w", "8", "-k", "uvicorn.workers.UvicornWorker", "main:app", "-b", "0.0.0.0:8502"] 