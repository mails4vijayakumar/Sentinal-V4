# Webapp — AGENTS.md

## Three Routes

| Path      | Component         | Description |
|-----------|-------------------|-------------|
| /live     | LiveView.tsx      | Real-time pipeline monitor over SSE |
| /reports  | ReportsView.tsx   | Historical runs, volume charts, duration trends |
| /chat     | ChatPage.tsx      | Streaming chatbot with KB citation pane |

## SSE Connection

Agent 1 (:8001) exposes two SSE streams:
- `/sse/dashboard` — global fan-out (all runs)
- `/sse/run/{run_id}` — per-run with replay (`?last_id=0`)

`useSSE.ts` handles auto-reconnect on disconnect.

## Design Tokens

`src/styles/tokens.css` defines all CSS variables. Key accents:
- `--amber` #F5A623 — primary brand / running state
- `--green` #00D4A8 — completed / success
- `--red`   #FF4136 — failed / critical alerts

## Build

```bash
cd webapp && npm install && npm run dev    # dev server :3000
npm run build                              # production dist/
```
