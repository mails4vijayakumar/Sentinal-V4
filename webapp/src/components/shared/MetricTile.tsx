interface Props {
  label:   string
  value:   string | number
  accent?: 'amber' | 'green' | 'red' | 'blue' | 'muted'
  pulse?:  boolean
}

const ACCENT_COLOR: Record<string, string> = {
  amber: 'var(--amber)',
  green: 'var(--green)',
  red:   'var(--red)',
  blue:  'var(--blue)',
  muted: 'var(--text-muted)',
}

export function MetricTile({ label, value, accent = 'muted', pulse = false }: Props) {
  const color = ACCENT_COLOR[accent]
  return (
    <div className="metric-tile">
      <span className="metric-tile__label">{label}</span>
      <span className="metric-tile__value" style={{
        color,
        textShadow: pulse ? `0 0 12px ${color}` : undefined,
        animation:  pulse ? 'pulse-glow 1.6s infinite' : undefined,
      }}>
        {value}
      </span>
      <style>{`
        .metric-tile { background: var(--bg-surface); border: 1px solid var(--border);
          border-radius: var(--radius-lg); padding: 14px 16px;
          display: flex; flex-direction: column; gap: 6px; }
        .metric-tile__label { font-size: 10px; font-family: var(--font-mono); color: var(--text-muted);
          text-transform: uppercase; letter-spacing: 0.1em; }
        .metric-tile__value { font-family: var(--font-mono); font-size: 24px; font-weight: 600; line-height: 1; }
      `}</style>
    </div>
  )
}
