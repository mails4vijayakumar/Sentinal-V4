FROM python:3.12-slim

WORKDIR /app

# Install shared requirements first (cached layer)
COPY agents/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy shared library
COPY shared/ /app/shared/

# Copy agent source
COPY agents/Agent-splunk/main.py /app/main.py

# Non-root user
RUN addgroup --system sentinel && adduser --system --ingroup sentinel sentinel
USER sentinel

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import httpx, asyncio; asyncio.run(httpx.AsyncClient().get('http://localhost:8002/health'))"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002", "--no-access-log"]
