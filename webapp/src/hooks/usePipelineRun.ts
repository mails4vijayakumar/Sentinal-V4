import { useEffect, useReducer } from 'react'
import type { RunSummary, StepSummary } from '../lib/api'
import type { SSEEvent } from '../lib/events'
import { useSSE } from './useSSE'

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8001'

interface RunState {
  run: RunSummary | null
  loading: boolean
  error: string | null
}
type Action =
  | { type: 'SET_RUN'; run: RunSummary }
  | { type: 'PATCH_STEP'; step: Partial<StepSummary> & { agent_num: number } }
  | { type: 'SET_STATUS'; status: string }
  | { type: 'ERROR'; error: string }

function reducer(state: RunState, action: Action): RunState {
  switch (action.type) {
    case 'SET_RUN':
      return { run: action.run, loading: false, error: null }
    case 'PATCH_STEP': {
      if (!state.run) return state
      const steps = (state.run.steps || []).map(s =>
        s.agent_num === action.step.agent_num ? { ...s, ...action.step } : s
      )
      // Insert if not present
      if (!steps.find(s => s.agent_num === action.step.agent_num)) {
        steps.push(action.step as StepSummary)
        steps.sort((a, b) => a.agent_num - b.agent_num)
      }
      return { ...state, run: { ...state.run, steps } }
    }
    case 'SET_STATUS':
      return state.run ? { ...state, run: { ...state.run, status: action.status as RunSummary['status'] } } : state
    case 'ERROR':
      return { ...state, loading: false, error: action.error }
    default:
      return state
  }
}

export function usePipelineRun(runId: string | null) {
  const [state, dispatch] = useReducer(reducer, { run: null, loading: true, error: null })

  // Initial fetch
  useEffect(() => {
    if (!runId) return
    const ROUTING_BASE = import.meta.env.VITE_ROUTING_DB_BASE || 'http://localhost:8000'
    fetch(`${ROUTING_BASE}/reads/runs/${runId}`)
      .then(r => r.json())
      .then(run => dispatch({ type: 'SET_RUN', run }))
      .catch(e => dispatch({ type: 'ERROR', error: String(e) }))
  }, [runId])

  // SSE live updates
  const sseUrl = runId ? `${API_BASE}/sse/run/${runId}` : ''
  useSSE(sseUrl, {
    enabled: !!runId,
    onEvent: (e: SSEEvent) => {
      if (!e.run_id || e.run_id !== runId) return
      if (e.event === 'agent_start' && e.agent_num != null) {
        dispatch({ type: 'PATCH_STEP', step: { agent_num: e.agent_num, agent_name: e.agent_name || '', status: 'running', started_at: e.timestamp || null } })
      }
      if (e.event === 'agent_done' && e.agent_num != null) {
        dispatch({ type: 'PATCH_STEP', step: { agent_num: e.agent_num, status: 'completed', completed_at: e.timestamp || null } })
      }
      if (e.event === 'agent_error' && e.agent_num != null) {
        dispatch({ type: 'PATCH_STEP', step: { agent_num: e.agent_num, status: 'failed', error: String((e.data as any)?.error || '') } })
      }
      if (e.event === 'pipeline_complete') dispatch({ type: 'SET_STATUS', status: 'completed' })
      if (e.event === 'pipeline_error')    dispatch({ type: 'SET_STATUS', status: 'failed' })
    },
  })

  return state
}
