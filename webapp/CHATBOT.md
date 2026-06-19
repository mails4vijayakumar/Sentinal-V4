# Chatbot — CHATBOT.md

## Architecture

Full-page `/chat` route with streaming LLM responses and KB citation pane.

### Current (v1)
- `POST /api/chat` on Agent 1 — proxied to configured LLM provider
- KB RAG: Agent 1 queries pgvector, attaches citations to response
- `useChatStream.ts` — streams tokens via SSE `data: {"token":"..."}` format
- `useCitationPane.ts` — manages sliding citation panel

### v2 Sketch
- Dedicated chat service (`:8010`) with session persistence in `chat.sessions`
- Multi-turn context window management (rolling 32k tokens)
- Tool use: can trigger `demo_p1_incidents.py` and surface routing-db metrics inline
- Citation hover-preview with excerpt highlight

## Prompt Design

System prompt context window includes:
1. Last 24h incident summary from routing-db
2. Top 5 KB documents by recent access
3. Active pipeline runs (if any)
