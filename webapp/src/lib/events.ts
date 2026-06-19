export type SSEEventType =
  | 'pipeline_started'
  | 'agent_start'
  | 'agent_done'
  | 'agent_error'
  | 'pipeline_complete'
  | 'pipeline_error'
  | 'heartbeat'

export interface SSEEvent {
  event:      SSEEventType
  run_id?:    string
  agent_num?: number
  agent_name?: string
  timestamp?: string
  data?:       Record<string, unknown>
}

export const AGENT_LABELS: Record<number, string> = {
  1: 'Ingest',
  2: 'Splunk',
  3: 'ServiceNow',
  4: 'PagerDuty',
  5: 'Notify',
  6: 'Confluence',
  7: 'RCA',
}

export const AGENT_NAMES: Record<number, string> = {
  1: 'dynatrace',
  2: 'splunk',
  3: 'servicenow',
  4: 'pagerduty',
  5: 'notifications',
  6: 'confluence',
  7: 'rca',
}
