import { useState } from 'react'
import { XAxis, YAxis, Tooltip, ResponsiveContainer, LineChart, Line } from 'recharts'
import { RefreshCw } from 'lucide-react'
import { useReports } from '../hooks/useReports'
import { fmtDuration } from '../lib/formatters'
import { MetricTile } from '../components/shared/MetricTile'
import { RunTable } from '../components/reports/RunTable'
import { VolumeChart } from '../components/reports/VolumeChart'

const WINDOWS = [
  { label: '6h', value: 6 },
  { label: '24h', value: 24 },
  { label: '7d', value: 168 },
]

export function ReportsView() {
  const [hours, setHours] = useState(24)
  const { runs, loading, error, refresh, stats } = useReports(hours)

  // Build hourly bucketed chart data
  const buckets = buildBuckets(runs, hours)

  const successRate = runs.length > 0
    ? Math.round(100 * stats.completed / runs.length)
    : 100

  // P1/P2 breakdown
  const bySeverity = runs.reduce<Record<string, number>>((acc, r) => {
    const sev = (r as any).severity || 'unknown'
    acc[sev] = (acc[sev] || 0) + 1
    return acc
  }, {})

  return (
    <div className="reports-view">
      {/* Toolbar */}
      <div className="reports-toolbar">
        <div style={{ display: 'flex', gap: 4 }}>
          {WINDOWS.map(w => (
            <button
              key={w.value}
              className={hours === w.value ? 'btn-amber' : 'btn-ghost'}
              style={{ padding: '5px 12px', fontSize: 12 }}
              onClick={() => setHours(w.value)}
            >{w.label}</button>
          ))}
        </div>
        <button className="btn-ghost" onClick={refresh} disabled={loading}
          style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12 }}>
          <RefreshCw size={12} className={loading ? 'spin' : ''} /> Refresh
        </button>
      </div>

      {/* KPI row */}
      <div className="metrics-row">
        <MetricTile label="Total Runs"   value={runs.length}         accent="blue" />
        <MetricTile label="Completed"    value={stats.completed}     accent="green" />
        <MetricTile label="Failed"       value={stats.failed}        accent={stats.failed > 0 ? 'red' : 'muted'} />
        <MetricTile label="Avg Duration" value={fmtDuration(stats.avgDuration)} accent="amber" />
        <MetricTile label="Success Rate" value={`${successRate}%`}   accent={successRate > 95 ? 'green' : 'red'} />
      </div>

      {/* Charts row */}
      <div className="charts-row">
        {/* Volume over time */}
        <div className="card" style={{ flex: 2 }}>
          <h4 className="chart-title">Pipeline Volume</h4>
          <VolumeChart data={buckets} barSize={hours <= 24 ? 8 : 4} />
        </div>

        {/* Duration trend */}
        <div className="card" style={{ flex: 1 }}>
          <h4 className="chart-title">Avg Duration (s)</h4>
          <ResponsiveContainer width="100%" height={160}>
            <LineChart data={buckets}>
              <XAxis dataKey="label" tick={{ fill: 'var(--text-muted)', fontSize: 10 }} axisLine={false} tickLine={false} />
              <YAxis hide />
              <Tooltip
                contentStyle={{ background: 'var(--bg-raised)', border: '1px solid var(--border)', borderRadius: 6, fontSize: 12 }}
                formatter={(v: number) => [`${v}s`, 'avg']}
              />
              <Line dataKey="avgDuration" stroke="var(--amber)" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Run table */}
      <div className="card" style={{ overflow: 'auto' }}>
        <h4 className="chart-title" style={{ marginBottom: 14 }}>Run History</h4>
        {error && <div style={{ color: 'var(--red)', fontSize: 13, marginBottom: 12 }}>{error}</div>}
        <RunTable runs={runs} limit={50} />
      </div>

      <style>{`
        .reports-view { padding: 24px; display: flex; flex-direction: column; gap: 20px; }
        .reports-toolbar { display: flex; align-items: center; justify-content: space-between; }
        .metrics-row { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; }
        .charts-row  { display: flex; gap: 12px; }
        .chart-title { font-size: 11px; font-family: var(--font-mono); color: var(--text-secondary);
          text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 12px; }
        .run-table { width: 100%; border-collapse: collapse; }
        .run-table th { font-family: var(--font-mono); font-size: 10px; color: var(--text-muted);
          text-transform: uppercase; letter-spacing: 0.08em; padding: 6px 12px;
          border-bottom: 1px solid var(--border); text-align: left; }
        .run-table td { padding: 8px 12px; border-bottom: 1px solid rgba(255,255,255,0.04); }
        .run-table tr:hover td { background: var(--bg-hover); }
        .spin { animation: spin 1s linear infinite; }
        @keyframes spin { to { transform: rotate(360deg); } }
      `}</style>
    </div>
  )
}

function buildBuckets(runs: any[], hours: number) {
  const bucketCount = hours <= 6 ? 6 : hours <= 24 ? 24 : 14
  const bucketMs    = (hours * 3_600_000) / bucketCount
  const now         = Date.now()
  const buckets = Array.from({ length: bucketCount }, (_, i) => ({
    label:       formatBucketLabel(now - (bucketCount - 1 - i) * bucketMs, hours),
    completed:   0,
    failed:      0,
    avgDuration: 0,
    _durs:       [] as number[],
  }))
  for (const r of runs) {
    const t = new Date(r.started_at).getTime()
    const idx = Math.floor((now - t) / bucketMs)
    const b = buckets[bucketCount - 1 - idx]
    if (!b) continue
    if (r.status === 'completed') { b.completed++; if (r.duration_ms) b._durs.push(r.duration_ms) }
    if (r.status === 'failed')    b.failed++
  }
  return buckets.map(b => ({
    ...b,
    avgDuration: b._durs.length ? Math.round(b._durs.reduce((s, d) => s + d, 0) / b._durs.length / 1000) : 0,
  }))
}

function formatBucketLabel(ts: number, hours: number) {
  const d = new Date(ts)
  if (hours <= 24) return `${d.getHours().toString().padStart(2,'0')}h`
  return `${d.getMonth()+1}/${d.getDate()}`
}
