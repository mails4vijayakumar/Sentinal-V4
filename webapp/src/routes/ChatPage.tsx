import { useRef, useEffect, useState, KeyboardEvent } from 'react'
import { Send, Square, RotateCcw } from 'lucide-react'
import { useChatStream } from '../hooks/useChatStream'
import { useCitationPane } from '../hooks/useCitationPane'
import { MessageBubble } from '../components/chat/MessageBubble'
import { CitationPane } from '../components/chat/CitationPane'
import { STARTER_PROMPTS } from '../lib/chat'

export function ChatPage() {
  const { messages, streaming, send, stop, reset } = useChatStream()
  const { open: paneOpen, citations, selected, show, hide, select } = useCitationPane()
  const [input, setInput] = useState('')
  const endRef            = useRef<HTMLDivElement>(null)

  useEffect(() => { endRef.current?.scrollIntoView({ behavior: 'smooth' }) }, [messages])

  const handleSend = () => { if (input.trim()) { send(input); setInput('') } }
  const handleKey  = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSend() }
  }

  return (
    <div className="chat-page">
      <div className="chat-column" style={{ flex: paneOpen ? '0 0 55%' : '1' }}>
        <div className="chat-thread">
          {messages.length === 0 && (
            <div className="chat-empty">
              <div className="chat-empty__icon">⚡</div>
              <h2 className="chat-empty__title">Sentinel Intelligence</h2>
              <p className="chat-empty__sub">Ask about past incidents, runbooks, and resolutions</p>
              <div className="starters">
                {STARTER_PROMPTS.map(s => (
                  <button key={s} className="starter" onClick={() => setInput(s)}>{s}</button>
                ))}
              </div>
            </div>
          )}
          {messages.map((m, i) => (
            <MessageBubble
              key={i}
              message={m}
              streaming={streaming && i === messages.length - 1}
              onShowSources={(msg) => show(msg.sources || [])}
            />
          ))}
          <div ref={endRef} />
        </div>

        <div className="chat-input-area">
          <textarea
            className="chat-textarea"
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKey}
            placeholder="Ask about incidents, runbooks, root causes…"
            rows={1}
            disabled={streaming}
          />
          <div className="chat-actions">
            {messages.length > 0 && (
              <button className="btn-ghost" onClick={reset} title="Clear" style={{ padding: '7px' }}>
                <RotateCcw size={13} />
              </button>
            )}
            {streaming
              ? <button className="btn-ghost" onClick={stop} style={{ padding: '7px', borderColor: 'var(--red)', color: 'var(--red)' }}>
                  <Square size={13} />
                </button>
              : <button className="btn-amber" onClick={handleSend} disabled={!input.trim()}>
                  <Send size={13} />
                </button>
            }
          </div>
        </div>
      </div>

      {paneOpen && (
        <CitationPane citations={citations} selected={selected} onSelect={select} onClose={hide} />
      )}

      <style>{`
        .chat-page { display: flex; height: 100%; overflow: hidden; }
        .chat-column { display: flex; flex-direction: column; min-width: 0; }
        .chat-thread { flex: 1; overflow-y: auto; padding: 32px 24px; display: flex; flex-direction: column; gap: 20px; }
        .chat-empty  { display: flex; flex-direction: column; align-items: center; justify-content: center;
          height: 100%; gap: 12px; animation: fade-in-up 0.4s ease; }
        .chat-empty__icon  { font-size: 40px; filter: drop-shadow(0 0 20px rgba(245,166,35,.4)); }
        .chat-empty__title { font-size: 22px; font-weight: 600; color: var(--text-primary); }
        .chat-empty__sub   { color: var(--text-muted); font-size: 14px; }
        .starters { display: flex; flex-direction: column; gap: 8px; width: 100%; max-width: 500px; margin-top: 8px; }
        .starter { background: var(--bg-surface); border: 1px solid var(--border); border-radius: var(--radius);
          padding: 10px 14px; text-align: left; color: var(--text-secondary); font-size: 13px; cursor: pointer;
          transition: all var(--t-fast); }
        .starter:hover { border-color: var(--border-active); color: var(--text-primary); background: var(--bg-hover); }
        .chat-input-area { padding: 16px 24px 24px; display: flex; gap: 10px; align-items: flex-end;
          border-top: 1px solid var(--border); background: var(--bg-base); }
        .chat-textarea { flex: 1; background: var(--bg-raised); border: 1px solid var(--border); border-radius: var(--radius-lg);
          color: var(--text-primary); font-family: var(--font-ui); font-size: 14px; padding: 12px 16px;
          resize: none; outline: none; min-height: 44px; max-height: 160px; overflow-y: auto;
          transition: border-color var(--t-fast); }
        .chat-textarea:focus { border-color: var(--border-focus); }
        .chat-textarea::placeholder { color: var(--text-muted); }
        .chat-actions { display: flex; gap: 6px; align-items: flex-end; }
      `}</style>
    </div>
  )
}
