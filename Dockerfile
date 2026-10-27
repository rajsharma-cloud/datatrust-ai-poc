# DataTrust AI web app (dashboard + AI agent + Data Estate portal)
# Build:  docker build -t datatrust-web .
# Run:    docker run -p 8000:8000 --env-file .env -e PG_HOST=<db host> datatrust-web
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    WEB_HOST=0.0.0.0 \
    WEB_PORT=8000

WORKDIR /app
COPY requirements-web.txt .
RUN pip install --no-cache-dir -r requirements-web.txt

# application code only: config.py + web/ (no data, no .env, no secrets)
COPY config.py .
COPY web/ web/

# run as a non-root user
RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request,os; urllib.request.urlopen(f'http://127.0.0.1:{os.getenv(\"PORT\") or os.getenv(\"WEB_PORT\",\"8000\")}/health', timeout=4)"
CMD ["python", "web/server.py"]
