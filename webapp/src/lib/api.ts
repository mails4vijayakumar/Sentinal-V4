const API_BASE     = import.meta.env.VITE_API_BASE      || 'http://localhost:8001'
const ROUTING_BASE = import.meta.env.VITE_ROUTING_DB_BASE || 'http://localhost:8000'

async function _fetch<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, { ...init, headers: { 'Content-Type': 'application/json', ...init?.headers } })
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`)
  return res.json() as Promise<T>
}

// ── Pipeline runs ──────────────────────────────────────────────────────────
export interface RunSummary {
  run_id: string
  incident_id: string
  status: 'running' | 'completed' | 'failed' | 'cancelled'
  flow: 'primary' | 'secondary'
  started_at: string
  completed_at: string | null
  duration_ms: number | null
  steps?: StepSummary[]
}

export interface StepSummary {
  agent_num: number
  agent_name: string
  status: 'pending' | 'running' | 'completed' | 'failed' | 'skipped'
  started_at: string | null
  completed_at: string | null
  duration_ms: number | null
  summary: string | null
  error: string | null
}

export interface DashboardMetrics {
  active: number
  completed_today: number
  avg_duration_ms: number
  p1_open: number
  success_rate: number
}

export const api = {
  async getMetrics(): Promise<DashboardMetrics> {
    return _fetch<DashboardMetrics>(`${ROUTING_BASE}/reads/metrics`)
  },
  async getActiveRuns(): Promise<RunSummary[]> {
    return _fetch<RunSummary[]>(`${ROUTING_BASE}/reads/runs?status=running&limit=50`)
  },
  async getRecentRuns(hours = 24, limit = 100): Promise<RunSummary[]> {
    return _fetch<RunSummary[]>(`${ROUTING_BASE}/reads/runs?hours=${hours}&limit=${limit}`)
  },
  async getRun(runId: string): Promise<RunSummary> {
    return _fetch<RunSummary>(`${ROUTING_BASE}/reads/runs/${runId}`)
  },
  async getIncident(externalId: string) {
    return _fetch(`${ROUTING_BASE}/reads/incidents/${externalId}`)
  },
  agentHealth: (agentNum: number) =>
    _fetch<{ status: string }>(`${API_BASE.replace('8001', `800${agentNum}`)}/health`),
}

// ── Chat API ───────────────────────────────────────────────────────────────
export interface ChatMessage { role: 'user' | 'assistant'; content: string; sources?: Citation[] }
export interface Citation    { title: string; url: string; score: number; excerpt?: string }

export async function* streamChat(
  messages: ChatMessage[],
  signal?: AbortSignal,
): AsyncGenerator<string> {
  const res = await fetch(`${API_BASE}/api/chat`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ messages }),
    signal,
  })
  if (!res.ok || !res.body) throw new Error(`Chat error: ${res.status}`)
  const reader  = res.body.getReader()
  const decoder = new TextDecoder()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    const text = decoder.decode(value, { stream: true })
    for (const line of text.split('\n')) {
      if (line.startsWith('data: ')) {
        const payload = line.slice(6)
        if (payload === '[DONE]') return
        try {
          const j = JSON.parse(payload)
          if (j.token) yield j.token as string
        } catch { /* ignore parse errors */ }
      }
    }
  }
}
