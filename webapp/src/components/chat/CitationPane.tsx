import { X, ChevronRight } from 'lucide-react'
import type { Citation } from '../../lib/api'
import { scoreLabel } from '../../lib/chat'

interface Props {
  citations: Citation[]
  selected:  Citation | null
  onSelect:  (c: Citation) => void
  onClose:   () => void
}

export function CitationPane({ citations, selected, onSelect, onClose }: Props) {
  return (
    <div className="citation-pane">
      <div className="citation-pane__header">
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-secondary)',
          textTransform: 'uppercase', letterSpacing: '0.08em' }}>
          Sources ({citations.length})
        </span>
        <button onClick={onClose} style={{ background: 'none', border: 'none', color: 'var(--text-muted)', cursor: 'pointer' }}>
          <X size={14} />
        </button>
      </div>
      <div className="citation-list">
        {citations.map((c, i) => {
          const { label, tone } = scoreLabel(c.score)
          const active = selected === c
          return (
            <button key={i} className={`citation-item ${active ? 'citation-item--active' : ''}`} onClick={() => onSelect(c)}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 }}>
                <span className="citation-item__title">{c.title}</span>
                <span className="citation-item__score" style={{ color: toneColor(tone) }}>
                  {(c.score * 100).toFixed(0)}%
                </span>
              </div>
              <span className="citation-item__label" style={{ color: toneColor(tone) }}>{label}</span>
              {active && c.excerpt && <p className="citation-item__excerpt">{c.excerpt}</p>}
              {active && c.url && (
                <a href={c.url} target="_blank" rel="noopener noreferrer" className="citation-item__link">
                  Open <ChevronRight size={10} />
                </a>
              )}
            </button>
          )
        })}
      </div>
      <style>{`
        .citation-pane { width: 320px; flex-shrink: 0; border-left: 1px solid var(--border);
          display: flex; flex-direction: column; background: var(--bg-surface); }
        .citation-pane__header { display: flex; align-items: center; justify-content: space-between;
          padding: 14px 16px; border-bottom: 1px solid var(--border); }
        .citation-list { flex: 1; overflow-y: auto; padding: 12px; display: flex; flex-direction: column; gap: 6px; }
        .citation-item { background: var(--bg-raised); border: 1px solid var(--border); border-radius: var(--radius);
          padding: 10px 12px; text-align: left; cursor: pointer; transition: all var(--t-fast);
          display: flex; flex-direction: column; gap: 4px; }
        .citation-item:hover { border-color: var(--border-active); }
        .citation-item--active { border-color: var(--border-focus); background: var(--bg-overlay); }
        .citation-item__title { font-size: 13px; color: var(--text-primary); font-weight: 500; }
        .citation-item__score { font-family: var(--font-mono); font-size: 11px; flex-shrink: 0; }
        .citation-item__label { font-size: 10px; font-family: var(--font-mono); text-transform: uppercase; letter-spacing: 0.06em; }
        .citation-item__excerpt { font-size: 12px; color: var(--text-secondary); line-height: 1.5; margin-top: 4px; }
        .citation-item__link { display: inline-flex; align-items: center; gap: 3px; font-size: 11px;
          color: var(--blue); text-decoration: none; font-family: var(--font-mono); margin-top: 4px; }
        .citation-item__link:hover { text-decoration: underline; }
      `}</style>
    </div>
  )
}

function toneColor(tone: 'high' | 'med' | 'low'): string {
  return tone === 'high' ? 'var(--green)' : tone === 'med' ? 'var(--amber)' : 'var(--text-muted)'
}
