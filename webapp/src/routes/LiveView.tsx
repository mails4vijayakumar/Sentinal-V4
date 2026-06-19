import { useState, useEffect, useCallback } from 'react'
import { Activity, Wifi, WifiOff, AlertTriangle, CheckCircle, Clock } from 'lucide-react'
import { useSSE } from '../hooks/useSSE'
import { api, type RunSummary } from '../lib/api'
import type { SSEEvent } from '../lib/events'
import { fmtDuration, fmtRelative, severityColor, truncate } from '../lib/formatters'
import { AGENT_LABELS } from '../lib/events'
import { PipelineCard } from '../components/pipeline/PipelineCard'
import { MetricTile } from '../components/shared/MetricTile'

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8001'

interface LiveRun extends RunSummary {
  _agentState: Record<number, 'idle' | 'running' | 'done' | 'error'>
}

export function LiveView() {
  const [runs,    setRuns]    = useState<LiveRun[]>([])
  const [metrics, setMetrics] = useState({ active: 0, completed_today: 0, avg_duration_ms: 0, p1_open: 0, success_rate: 100 })
  const [selected, setSelected] = useState<string | null>(null)

  // Fetch initial state
  useEffect(() => {
    api.getMetrics().then(setMetrics).catch(() => null)
    api.getActiveRuns().then(runs => setRuns(runs.map(toAlive))).catch(() => null)
    const interval = setInterval(() => {
      api.getMetrics().then(setMetrics).catch(() => null)
    }, 10_000)
    return () => clearInterval(interval)
  }, [])

  const handleSSEEvent = useCallback((e: SSEEvent) => {
    if (e.event === 'pipeline_started') {
      const newRun: LiveRun = {
        run_id: e.run_id!, incident_id: '', status: 'running',
        flow:   (e.data?.flow as string) || 'primary',
        started_at: e.timestamp!, completed_at: null, duration_ms: null,
        _agentState: {},
      }
      setRuns(prev => [newRun, ...prev.filter(r => r.run_id !== e.run_id)].slice(0, 30))
    }
    if (e.event === 'agent_start' && e.run_id && e.agent_num) {
      setRuns(prev => prev.map(r => r.run_id === e.run_id
        ? { ...r, _agentState: { ...r._agentState, [e.agent_num!]: 'running' } } : r))
    }
    if (e.event === 'agent_done' && e.run_id && e.agent_num) {
      setRuns(prev => prev.map(r => r.run_id === e.run_id
        ? { ...r, _agentState: { ...r._agentState, [e.agent_num!]: 'done' } } : r))
    }
    if (e.event === 'agent_error' && e.run_id && e.agent_num) {
      setRuns(prev => prev.map(r => r.run_id === e.run_id
        ? { ...r, _agentState: { ...r._agentState, [e.agent_num!]: 'error' } } : r))
    }
    if (e.event === 'pipeline_complete' && e.run_id) {
      setRuns(prev => prev.map(r => r.run_id === e.run_id
        ? { ...r, status: 'completed', duration_ms: (e.data?.duration_ms as number) || null } : r))
      setMetrics(m => ({ ...m, completed_today: m.completed_today + 1, active: Math.max(0, m.active - 1) }))
    }
    if (e.event === 'pipeline_error' && e.run_id) {
      setRuns(prev => prev.map(r => r.run_id === e.run_id ? { ...r, status: 'failed' } : r))
      setMetrics(m => ({ ...m, active: Math.max(0, m.active - 1) }))
    }
  }, [])

  const { connected } = useSSE(`${API_BASE}/sse/dashboard`, { onEvent: handleSSEEvent })

  const activeRuns    = runs.filter(r => r.status === 'running')
  const recentDone    = runs.filter(r => r.status !== 'running').slice(0, 15)

  return (
    <div className="live-view">
      {/* Header row */}
      <div className="live-view__header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <Activity size={16} color="var(--amber)" />
          <span style={{ fontFamily: 'var(--font-mono)', fontSize: 12, color: 'var(--text-secondary)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>
            Live Pipeline Monitor
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          {connected
            ? <><Wifi size={13} color="var(--green)" /> <span style={{ fontSize: 11, color: 'var(--green)', fontFamily: 'var(--font-mono)' }}>LIVE</span></>
            : <><WifiOff size={13} color="var(--red)" /> <span style={{ fontSize: 11, color: 'var(--red)', fontFamily: 'var(--font-mono)' }}>RECONNECTING</span></>
          }
        </div>
      </div>

      {/* Metrics row */}
      <div className="metrics-row">
        <MetricTile label="Active" value={metrics.active} accent="amber" pulse={metrics.active > 0} />
        <MetricTile label="Completed Today" value={metrics.completed_today} accent="green" />
        <MetricTile label="Avg Duration" value={fmtDuration(metrics.avg_duration_ms)} accent="blue" />
        <MetricTile label="P1 Open" value={metrics.p1_open} accent={metrics.p1_open > 0 ? 'red' : 'muted'} />
        <MetricTile label="Success Rate" value={`${metrics.success_rate}%`} accent="green" />
      </div>

      {/* Active pipelines */}
      {activeRuns.length > 0 && (
        <section className="live-section">
          <h3 className="section-label">
            <span className="dot dot--running" /> Running ({activeRuns.length})
          </h3>
          <div className="pipeline-grid">
            {activeRuns.map(run => (
              <PipelineCard
                key={run.run_id}
                run={run}
                agentState={run._agentState}
                selected={selected === run.run_id}
                onClick={() => setSelected(s => s === run.run_id ? null : run.run_id)}
              />
            ))}
          </div>
        </section>
      )}

      {/* Recent */}
      <section className="live-section">
        <h3 className="section-label">Recent</h3>
        <div className="run-list">
          {recentDone.length === 0 && (
            <div style={{ color: 'var(--text-muted)', fontSize: 13, padding: '32px 0', textAlign: 'center' }}>
              No completed runs yet
            </div>
          )}
          {recentDone.map(run => (
            <div key={run.run_id} className={`run-row run-row--${run.status}`}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, flex: 1 }}>
                {run.status === 'completed'
                  ? <CheckCircle size={13} color="var(--green)" />
                  : <AlertTriangle size={13} color="var(--red)" />}
                <span className="mono" style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                  {run.run_id.slice(0, 8)}
                </span>
                <span className="badge" style={{ background: run.flow === 'primary' ? 'var(--blue-dim)' : 'rgba(74,85,104,.2)', color: run.flow === 'primary' ? 'var(--blue)' : 'var(--text-muted)' }}>
                  {run.flow}
                </span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 16, color: 'var(--text-muted)', fontSize: 12 }}>
                <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                  <Clock size={11} /> {fmtDuration(run.duration_ms)}
                </span>
                <span>{fmtRelative(run.completed_at || run.started_at)}</span>
              </div>
            </div>
          ))}
        </div>
      </section>

      <style>{`
        .live-view { padding: 24px; display: flex; flex-direction: column; gap: 24px; }
        .live-view__header { display: flex; align-items: center; justify-content: space-between; }
        .metrics-row { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; }
        .live-section { display: flex; flex-direction: column; gap: 12px; }
        .section-label { font-size: 11px; font-family: var(--font-mono); color: var(--text-muted);
          text-transform: uppercase; letter-spacing: 0.1em; display: flex; align-items: center; gap: 8px; }
        .dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
        .dot--running { background: var(--amber); animation: pulse-glow 1.6s infinite; }
        .pipeline-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr)); gap: 12px; }
        .run-list { display: flex; flex-direction: column; gap: 2px; }
        .run-row { display: flex; align-items: center; justify-content: space-between;
          padding: 8px 12px; border-radius: var(--radius); transition: background var(--t-fast); }
        .run-row:hover { background: var(--bg-hover); }
        .run-row--completed { border-left: 2px solid var(--green); }
        .run-row--failed    { border-left: 2px solid var(--red); }
      `}</style>
    </div>
  )
}

function toAlive(r: RunSummary): LiveRun {
  const agentState: Record<number, 'idle' | 'running' | 'done' | 'error'> = {}
  for (const step of r.steps || []) {
    agentState[step.agent_num] =
      step.status === 'completed' ? 'done'
      : step.status === 'failed'  ? 'error'
      : step.status === 'running' ? 'running'
      : 'idle'
  }
  return { ...r, _agentState: agentState }
}
