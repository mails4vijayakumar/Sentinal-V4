import { CheckCircle, AlertTriangle, Loader, Circle } from 'lucide-react'
import { AGENT_LABELS } from '../../lib/events'
import { fmtDuration } from '../../lib/formatters'
import type { RunSummary } from '../../lib/api'

interface Props {
  run:        RunSummary & { _agentState?: Record<number, 'idle'|'running'|'done'|'error'> }
  agentState?: Record<number, 'idle'|'running'|'done'|'error'>
  selected?:  boolean
  onClick?:   () => void
}

export function PipelineCard({ run, agentState = {}, selected, onClick }: Props) {
  const state = agentState || run._agentState || {}
  const activeAgent = Object.entries(state).find(([,s]) => s === 'running')?.[0]

  return (
    <div className={`pipeline-card ${selected ? 'pipeline-card--selected' : ''}`} onClick={onClick}>
      {/* Header */}
      <div className="pc-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span className={`badge badge--${run.status}`}>{run.status}</span>
          <span className={`badge ${run.flow === 'primary' ? '' : ''}`}
            style={{ background: run.flow === 'primary' ? 'var(--blue-dim)' : 'rgba(74,85,104,.2)', color: run.flow === 'primary' ? 'var(--blue)' : 'var(--text-muted)' }}>
            {run.flow}
          </span>
        </div>
        <span className="mono" style={{ fontSize: 10, color: 'var(--text-muted)' }}>
          {run.run_id.slice(0, 8)}
        </span>
      </div>

      {/* Agent chain */}
      <div className="pc-chain">
        {[1,2,3,4,5,6,7].map((n, i) => {
          const s = state[n] || 'idle'
          return (
            <div key={n} style={{ display: 'flex', alignItems: 'center' }}>
              <div className={`pc-node pc-node--${s}`} title={`Agent ${n}: ${AGENT_LABELS[n]}`}>
                <span className="pc-node__num">{n}</span>
                {s === 'running' && <div className="pc-node__ring" />}
                {s === 'done'    && <CheckCircle size={8} style={{ position: 'absolute', bottom: -2, right: -2, color: 'var(--green)' }} />}
                {s === 'error'   && <AlertTriangle size={8} style={{ position: 'absolute', bottom: -2, right: -2, color: 'var(--red)' }} />}
              </div>
              {i < 6 && (
                <svg width="16" height="2" style={{ flexShrink: 0 }}>
                  <line x1="0" y1="1" x2="16" y2="1" strokeWidth="1"
                    stroke={s === 'done' ? 'var(--green)' : s === 'running' ? 'var(--amber)' : 'var(--border)'}
                    strokeDasharray={s === 'running' ? '3 2' : undefined}
                  />
                </svg>
              )}
            </div>
          )
        })}
      </div>

      {/* Footer */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
          {activeAgent ? `Agent ${activeAgent} running…` : run.status === 'completed' ? 'Complete' : ''}
        </span>
        {run.duration_ms && (
          <span className="mono" style={{ fontSize: 11, color: 'var(--amber)' }}>
            {fmtDuration(run.duration_ms)}
          </span>
        )}
      </div>

      <style>{`
        .pipeline-card { background: var(--bg-surface); border: 1px solid var(--border);
          border-radius: var(--radius-lg); padding: 14px; cursor: pointer; transition: all var(--t-base);
          display: flex; flex-direction: column; gap: 12px; }
        .pipeline-card:hover { border-color: var(--border-active); background: var(--bg-raised); }
        .pipeline-card--selected { border-color: var(--border-focus); box-shadow: 0 0 0 1px var(--border-focus); }
        .pc-header { display: flex; align-items: center; justify-content: space-between; }
        .pc-chain  { display: flex; align-items: center; }
        .pc-node   { width: 24px; height: 24px; border-radius: 50%; position: relative;
          display: flex; align-items: center; justify-content: center;
          border: 1.5px solid var(--border); transition: all var(--t-base); flex-shrink: 0; }
        .pc-node--running { border-color: var(--amber); background: var(--amber-dim); box-shadow: var(--amber-glow); }
        .pc-node--done    { border-color: var(--green); background: var(--green-dim); }
        .pc-node--error   { border-color: var(--red);   background: var(--red-dim); }
        .pc-node--idle    { border-color: var(--border); }
        .pc-node__num     { font-family: var(--font-mono); font-size: 9px; color: var(--text-secondary); }
        .pc-node--running .pc-node__num { color: var(--amber); }
        .pc-node--done    .pc-node__num { color: var(--green); }
        .pc-node__ring    { position: absolute; inset: -4px; border-radius: 50%;
          border: 1px solid var(--amber); animation: pulse-glow 1.4s infinite; opacity: 0.5; }
      `}</style>
    </div>
  )
}
