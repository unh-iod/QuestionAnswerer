# Use official lightweight Python image
FROM python:3.13-slim

# Environment settings
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install system deps — R is required by pyreadr
RUN apt-get update && apt-get install -y \
    build-essential \
    r-base \
    libssl-dev \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependencies first (cache friendly)
COPY requirements.txt .

# Install Python deps
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Cloud Run port
ENV PORT=8080

# Start Flask using Gunicorn with tuning for Cloud Run
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 120 main:app