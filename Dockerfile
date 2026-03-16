FROM python:3.14-slim

# Install uv
RUN pip install --no-cache-dir uv

# Set working directory
WORKDIR /app

# Copy dependency files first (better caching)
COPY pyproject.toml uv.lock ./

# Install dependencies (no dev packages for production)
RUN uv sync --no-dev

# Copy source code and config
COPY src/ ./src/
COPY pipeline.py logging_config.yaml ./
COPY queries/ ./queries/
COPY data/raw/ ./data/raw/

# Create logs directory with proper permissions
RUN mkdir -p logs && chmod 777 logs

# Create a non-root user
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Run pipeline
CMD ["uv", "run", "pipeline.py"]
