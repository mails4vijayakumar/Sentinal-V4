FROM python:3.12-slim

WORKDIR /app

COPY routing-db/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy shared lib (routing_client, models needed)
COPY shared/ /app/shared/

# Copy routing-db app
COPY routing-db/ /app/routing_db/

# Module alias so imports work
RUN ln -s /app/routing_db /app/routing-db

RUN addgroup --system sentinel && adduser --system --ingroup sentinel sentinel
USER sentinel

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "routing_db.app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
