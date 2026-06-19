import { CheckCircle, AlertTriangle, Clock } from 'lucide-react'
import type { RunSummary } from '../../lib/api'
import { fmtDuration, fmtDateTime } from '../../lib/formatters'

interface Props {
  runs:     RunSummary[]
  limit?:   number
  onSelect?: (runId: string) => void
}

export function RunTable({ runs, limit = 50, onSelect }: Props) {
  return (
    <table className="run-table">
      <thead>
        <tr>
          <th>Run ID</th>
          <th>Status</th>
          <th>Flow</th>
          <th>Duration</th>
          <th>Started</th>
        </tr>
      </thead>
      <tbody>
        {runs.slice(0, limit).map(r => (
          <tr key={r.run_id} onClick={() => onSelect?.(r.run_id)} style={{ cursor: onSelect ? 'pointer' : 'default' }}>
            <td className="mono" style={{ fontSize: 11, color: 'var(--text-muted)' }}>{r.run_id.slice(0, 12)}…</td>
            <td>
              <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                {r.status === 'completed' ? <CheckCircle size={12} color="var(--green)" />
                  : r.status === 'failed' ? <AlertTriangle size={12} color="var(--red)" />
                  : <Clock size={12} color="var(--amber)" />}
                <span style={{ fontSize: 12, color: statusColor(r.status) }}>{r.status}</span>
              </div>
            </td>
            <td>
              <span className="badge" style={{
                background: r.flow === 'primary' ? 'var(--blue-dim)' : 'rgba(74,85,104,.2)',
                color: r.flow === 'primary' ? 'var(--blue)' : 'var(--text-muted)',
              }}>{r.flow}</span>
            </td>
            <td className="mono" style={{ fontSize: 12 }}>{fmtDuration(r.duration_ms)}</td>
            <td style={{ fontSize: 12, color: 'var(--text-muted)' }}>{fmtDateTime(r.started_at)}</td>
          </tr>
        ))}
        {runs.length === 0 && (
          <tr><td colSpan={5} style={{ textAlign: 'center', padding: '32px 0', color: 'var(--text-muted)' }}>
            No runs in this window
          </td></tr>
        )}
      </tbody>
      <style>{`
        .run-table { width: 100%; border-collapse: collapse; }
        .run-table th { font-family: var(--font-mono); font-size: 10px; color: var(--text-muted);
          text-transform: uppercase; letter-spacing: 0.08em; padding: 6px 12px;
          border-bottom: 1px solid var(--border); text-align: left; }
        .run-table td { padding: 8px 12px; border-bottom: 1px solid rgba(255,255,255,0.04); }
        .run-table tr:hover td { background: var(--bg-hover); }
      `}</style>
    </table>
  )
}

function statusColor(s: string): string {
  return s === 'completed' ? 'var(--green)' : s === 'failed' ? 'var(--red)' : 'var(--amber)'
}
