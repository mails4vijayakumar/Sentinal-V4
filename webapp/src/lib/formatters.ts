import { formatDistanceToNow, format } from 'date-fns'

export function fmtDuration(ms: number | null | undefined): string {
  if (!ms) return '—'
  if (ms < 1000) return `${ms}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
  return `${(ms / 60_000).toFixed(1)}m`
}

export function fmtRelative(iso: string | null | undefined): string {
  if (!iso) return '—'
  try { return formatDistanceToNow(new Date(iso), { addSuffix: true }) }
  catch { return iso }
}

export function fmtTime(iso: string | null | undefined): string {
  if (!iso) return '—'
  try { return format(new Date(iso), 'HH:mm:ss') }
  catch { return iso }
}

export function fmtDateTime(iso: string | null | undefined): string {
  if (!iso) return '—'
  try { return format(new Date(iso), 'MMM d HH:mm') }
  catch { return iso }
}

export function severityColor(sev: string): string {
  const map: Record<string, string> = {
    P1: 'var(--sev-p1)', P2: 'var(--sev-p2)', P3: 'var(--sev-p3)',
    P4: 'var(--sev-p4)', P5: 'var(--sev-p5)',
  }
  return map[sev?.toUpperCase()] || 'var(--text-muted)'
}

export function statusColor(status: string): string {
  const map: Record<string, string> = {
    running:   'var(--amber)',
    completed: 'var(--green)',
    failed:    'var(--red)',
    cancelled: 'var(--text-muted)',
    pending:   'var(--text-muted)',
    skipped:   'var(--text-muted)',
  }
  return map[status] || 'var(--text-muted)'
}

export function truncate(s: string, n = 60): string {
  return s.length > n ? s.slice(0, n) + '…' : s
}
