# Agent 5 — Multi-Channel Notifications

**Port:** `8005` · **Queue:** `agent:5:queue` · **Enqueues:** none (terminal for its branch)

## Role

Delivers human-facing alerts across multiple channels for primary-flow
incidents. Runs in parallel with Agent 6 after Agent 4's fan-out. Secondary
flows and P4/P5 are skipped (no notification noise for low-priority tickets).

## Channels

| Channel | Transport | Enabled when |
|---------|-----------|--------------|
| Microsoft Teams | Incoming webhook (MessageCard) | `TEAMS_WEBHOOK_URL` set |
| Email | SMTP + STARTTLS | `SMTP_HOST` and `NOTIFY_EMAIL_TO` set |
| SMS | (placeholder for future provider) | — |

## Teams Card

Posts an actionable `MessageCard` with a severity-coloured theme and facts:
SNOW number, PagerDuty incident ID, on-call engineer, run ID.

| Severity | Theme colour |
|----------|--------------|
| P1 | `#FF0000` |
| P2 | `#FF6B00` |
| P3 | `#FFC300` |
| P4 | `#00C176` |
| P5 | `#666666` |

## Email

Plain-text summary including severity, title, SNOW incident, on-call, Splunk
classification, and run ID. Subject: `[{severity}] Sentinel: {title}`.

## Outputs

- Writes `NotificationEnrichment` (`channels_notified`, `teams_ok`, `email_ok`)
- Records step in routing-db
- Publishes `agent_start` / `agent_done`
- Does **not** enqueue Agent 6 (already enqueued by Agent 4's fan-out)

## Skip Logic

If flow is secondary or severity is P4/P5, returns an empty
`NotificationEnrichment` and emits `agent_done` immediately.

## Failure Behaviour

Each channel is independent; a failed Teams post does not block email. Channel
failures are logged and surface in `channels_notified` (the channel is omitted).

## Key Env Vars

`TEAMS_WEBHOOK_URL`, `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`,
`NOTIFY_EMAIL_TO`, `AGENT_5_PORT`
