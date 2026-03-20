FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first for layer caching
COPY pyproject.toml README.md ./
RUN uv pip install --system --no-cache ".[server,s3]"

# Copy application code
COPY src/ src/
RUN uv pip install --system --no-cache --no-deps .

RUN useradd --create-home cascadq
USER cascadq

EXPOSE 8000

ENTRYPOINT ["python", "-m", "cascadq"]
CMD ["--config", "/etc/cascadq/config.yaml"]
