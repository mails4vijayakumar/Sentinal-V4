import { BookOpen } from 'lucide-react'
import type { ChatMessage } from '../../lib/api'

interface Props {
  message:     ChatMessage
  streaming?:  boolean
  onShowSources?: (msg: ChatMessage) => void
}

export function MessageBubble({ message, streaming, onShowSources }: Props) {
  const isUser = message.role === 'user'
  return (
    <div className={`msg msg--${message.role}`}>
      <div className="msg__avatar">{isUser ? 'U' : 'AI'}</div>
      <div className="msg__bubble">
        <div className="msg__content">
          {message.content || (streaming ? <span className="cursor-blink">▋</span> : null)}
        </div>
        {message.sources && message.sources.length > 0 && (
          <button className="citations-btn" onClick={() => onShowSources?.(message)}>
            <BookOpen size={11} /> {message.sources.length} sources
          </button>
        )}
      </div>
      <style>{`
        .msg { display: flex; gap: 12px; animation: fade-in-up 0.2s ease; }
        .msg--user { flex-direction: row-reverse; }
        .msg__avatar { width: 30px; height: 30px; border-radius: 50%; flex-shrink: 0;
          display: flex; align-items: center; justify-content: center; font-size: 11px; font-weight: 600;
          background: var(--bg-overlay); border: 1px solid var(--border); color: var(--text-secondary); }
        .msg--user .msg__avatar { background: var(--amber-dim); border-color: var(--amber); color: var(--amber); }
        .msg__bubble { max-width: 72%; display: flex; flex-direction: column; gap: 6px; }
        .msg--user .msg__bubble { align-items: flex-end; }
        .msg__content { background: var(--bg-surface); border: 1px solid var(--border); border-radius: var(--radius-lg);
          padding: 12px 16px; font-size: 14px; line-height: 1.6; white-space: pre-wrap; word-break: break-word; }
        .msg--user .msg__content { background: var(--amber-dim); border-color: var(--border-active); }
        .cursor-blink { display: inline-block; animation: blink-cursor 0.7s infinite; color: var(--amber); }
        .citations-btn { display: inline-flex; align-items: center; gap: 5px; padding: 4px 9px;
          background: var(--blue-dim); border: 1px solid rgba(61,170,255,.2); border-radius: 4px;
          color: var(--blue); font-size: 11px; cursor: pointer; transition: all var(--t-fast); font-family: var(--font-mono); }
        .citations-btn:hover { border-color: var(--blue); }
      `}</style>
    </div>
  )
}
