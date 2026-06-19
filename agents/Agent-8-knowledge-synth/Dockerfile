FROM python:3.12-slim

WORKDIR /app
ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# System deps for hdbscan / scikit-learn native extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && rm -rf /var/lib/apt/lists/*

# Install shared agent deps first (cached layer), then Agent 8 deps.
COPY agents/requirements.txt /app/agents/requirements.txt
COPY agents/Agent-8-knowledge-synth/requirements.txt /app/agents/Agent-8-knowledge-synth/requirements.txt
RUN pip install --no-cache-dir -r /app/agents/requirements.txt \
    && pip install --no-cache-dir -r /app/agents/Agent-8-knowledge-synth/requirements.txt

# Copy shared library and the agent source. Agent 8's code imports via the
# dotted path `agents.Agent_8_knowledge_synth.*` (underscores) — the directory
# uses hyphens. The sitecustomize.py shim below aliases the hyphenated dir
# under the underscore module name at interpreter startup, mirroring the trick
# used in tests/conftest.py so Python imports resolve correctly in-container.
COPY shared /app/shared
COPY agents/Agent-8-knowledge-synth /app/agents/Agent-8-knowledge-synth

RUN printf '%s\n' \
    'import sys, importlib.util, importlib.machinery' \
    'from pathlib import Path' \
    '' \
    'root = Path("/app")' \
    'agent_dir = root / "agents" / "Agent-8-knowledge-synth"' \
    'if agent_dir.exists() and "agents.Agent_8_knowledge_synth" not in sys.modules:' \
    '    if "agents" not in sys.modules:' \
    '        spec = importlib.machinery.ModuleSpec("agents", loader=None, is_package=True)' \
    '        m = importlib.util.module_from_spec(spec)' \
    '        m.__path__ = [str(root / "agents")]' \
    '        sys.modules["agents"] = m' \
    '    spec = importlib.machinery.ModuleSpec("agents.Agent_8_knowledge_synth", loader=None, is_package=True)' \
    '    m = importlib.util.module_from_spec(spec)' \
    '    m.__path__ = [str(agent_dir)]' \
    '    sys.modules["agents.Agent_8_knowledge_synth"] = m' \
    > /app/sitecustomize.py

# Non-root user (matches Agent 1 pattern)
RUN addgroup --system sentinel && adduser --system --ingroup sentinel sentinel
USER sentinel

EXPOSE 8008

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8008/health').read()"

CMD ["uvicorn", "agents.Agent_8_knowledge_synth.main:app", "--host", "0.0.0.0", "--port", "8008"]
