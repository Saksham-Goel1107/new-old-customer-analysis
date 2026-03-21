# Multi-stage build for optimized image size
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY cohort_analysis.py .
COPY app.py .

# Create a non-root user for security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Health check: ensure credentials file is present (mounted) — lightweight and reliable
# On Kubernetes or Docker orchestration, missing credentials often indicates a misconfiguration.
HEALTHCHECK --interval=300s --timeout=10s --start-period=10s --retries=3 \
    CMD test -f /app/credentials.json || exit 1

# Default to scheduled mode (can be overridden)
ENV RUN_MODE=scheduled
ENV GOOGLE_CREDENTIALS_FILE=/app/credentials.json

# Run the application
CMD ["python", "app.py"]
