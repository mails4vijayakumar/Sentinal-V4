import type { ChatMessage, Citation } from './api'

// ── Suggested starter prompts shown on the empty chat screen ─────────────────
export const STARTER_PROMPTS: string[] = [
  'What were the most common P1 root causes this week?',
  'How do I restart the HL7 interface engine?',
  'What runbooks exist for DB connection pool exhaustion?',
  'Summarise the last 5 resolved incidents',
  'Which services breached SLA in the last 24 hours?',
]

// ── Local session persistence (in-memory only; survives route changes) ───────
// NOTE: artifacts/browser-storage APIs are intentionally avoided. Sessions live
// in module state for the lifetime of the tab.
interface ChatSession {
  id:        string
  title:     string
  messages:  ChatMessage[]
  createdAt: number
}

const _sessions = new Map<string, ChatSession>()

export function newSessionId(): string {
  return `sess_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
}

export function saveSession(session: ChatSession): void {
  _sessions.set(session.id, session)
}

export function getSession(id: string): ChatSession | undefined {
  return _sessions.get(id)
}

export function listSessions(): ChatSession[] {
  return [..._sessions.values()].sort((a, b) => b.createdAt - a.createdAt)
}

// ── Derive a session title from the first user message ───────────────────────
export function deriveTitle(messages: ChatMessage[]): string {
  const first = messages.find(m => m.role === 'user')
  if (!first) return 'New conversation'
  return first.content.length > 48 ? first.content.slice(0, 48) + '…' : first.content
}

// ── Citation helpers ─────────────────────────────────────────────────────────

/** Sort citations by descending score and dedupe by title. */
export function rankCitations(citations: Citation[]): Citation[] {
  const seen = new Set<string>()
  return [...citations]
    .sort((a, b) => b.score - a.score)
    .filter(c => {
      if (seen.has(c.title)) return false
      seen.add(c.title)
      return true
    })
}

/** Render a confidence label from a cosine score. */
export function scoreLabel(score: number): { label: string; tone: 'high' | 'med' | 'low' } {
  if (score >= 0.85) return { label: 'Strong match', tone: 'high' }
  if (score >= 0.70) return { label: 'Likely match', tone: 'med' }
  return { label: 'Weak match', tone: 'low' }
}

// ── Token estimation (rough — 4 chars ≈ 1 token) ─────────────────────────────
export function estimateTokens(text: string): number {
  return Math.ceil(text.length / 4)
}

/** Trim a message history to fit within a rough token budget (keeps most recent). */
export function trimHistory(messages: ChatMessage[], maxTokens = 8000): ChatMessage[] {
  let budget = maxTokens
  const kept: ChatMessage[] = []
  for (let i = messages.length - 1; i >= 0; i--) {
    const cost = estimateTokens(messages[i].content)
    if (budget - cost < 0) break
    budget -= cost
    kept.unshift(messages[i])
  }
  return kept
}
