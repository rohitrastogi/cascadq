FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir ".[server,s3]"

EXPOSE 8000

ENTRYPOINT ["python", "-m", "cascadq"]
CMD ["--storage", "s3"]
