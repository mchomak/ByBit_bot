# ==============================================================================
# Bybit Trading Bot - Production Dockerfile
# ==============================================================================
FROM python:3.12-slim

LABEL maintainer="ByBit Bot Team"
LABEL description="Automated trading bot for Bybit exchange"
LABEL version="1.0.0"

# Set working directory
WORKDIR /app

# Environment variables for Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy requirements first (for Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY config/ ./config/
COPY core/ ./core/
COPY db/ ./db/
COPY services/ ./services/
COPY bot/ ./bot/
COPY trade/ ./trade/

# Create directories for logs and data
RUN mkdir -p /app/logs /app/data

# Create non-root user for security
RUN useradd -m -u 1000 -s /bin/bash botuser && \
    chown -R botuser:botuser /app
USER botuser

# Default command - run the bot
CMD ["python", "-m", "core.main"]
