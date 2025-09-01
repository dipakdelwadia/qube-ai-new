#!/bin/bash

# Startup script for Azure App Service
echo "Starting AI Assistant API..."

# Install dependencies if not already installed
echo "Installing dependencies..."
python -m pip install --upgrade pip
pip install -r requirements.txt

# Start the FastAPI application with Gunicorn
echo "Starting Gunicorn server..."
gunicorn --bind 0.0.0.0:8000 --workers 4 --timeout 120 --keep-alive 2 --max-requests 1000 --max-requests-jitter 50 main:app
