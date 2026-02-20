FROM python:3.13-slim

# Install uv
RUN pip install uv

WORKDIR /app

COPY pyproject.toml .
RUN uv sync

COPY . .

# Create a non-root user
RUN useradd -m appuser
USER appuser

CMD ["python", "pipeline.py"]
