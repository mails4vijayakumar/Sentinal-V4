// webapp/src/hooks/useReports.ts
import { useEffect, useState, useCallback } from 'react'
import { api, type RunSummary } from '../lib/api'

export function useReports(hours = 24) {
  const [runs,    setRuns]    = useState<RunSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState<string | null>(null)

  const refresh = useCallback(async () => {
    setLoading(true)
    try {
      const data = await api.getRecentRuns(hours, 200)
      setRuns(data)
      setError(null)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [hours])

  useEffect(() => { refresh() }, [refresh])

  const completed   = runs.filter(r => r.status === 'completed')
  const failed      = runs.filter(r => r.status === 'failed')
  const p1          = runs.filter(r => r.flow === 'primary')
  const avgDuration = completed.length > 0
    ? Math.round(completed.reduce((s, r) => s + (r.duration_ms || 0), 0) / completed.length)
    : 0

  return { runs, loading, error, refresh, stats: { completed: completed.length, failed: failed.length, primary: p1.length, avgDuration } }
}
