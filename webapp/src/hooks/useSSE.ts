import { useCallback, useEffect, useRef, useState } from 'react'
import type { SSEEvent } from '../lib/events'

interface UseSSEOptions {
  onEvent?: (e: SSEEvent) => void
  enabled?: boolean
}

export function useSSE(url: string, { onEvent, enabled = true }: UseSSEOptions = {}) {
  const [connected, setConnected]   = useState(false)
  const [events, setEvents]         = useState<SSEEvent[]>([])
  const esRef                       = useRef<EventSource | null>(null)
  const onEventRef                  = useRef(onEvent)
  onEventRef.current = onEvent

  const connect = useCallback(() => {
    if (!enabled || !url) return
    esRef.current?.close()
    const es = new EventSource(url)
    esRef.current = es

    es.onopen = () => setConnected(true)

    es.addEventListener('message', (e) => {
      try {
        const parsed: SSEEvent = JSON.parse(e.data)
        setEvents(prev => [parsed, ...prev].slice(0, 200))
        onEventRef.current?.(parsed)
      } catch { /* ignore */ }
    })

    es.onerror = () => {
      setConnected(false)
      es.close()
      // Reconnect after 3s
      setTimeout(connect, 3000)
    }
  }, [url, enabled])

  useEffect(() => {
    connect()
    return () => { esRef.current?.close(); setConnected(false) }
  }, [connect])

  const clear = useCallback(() => setEvents([]), [])

  return { connected, events, clear }
}
