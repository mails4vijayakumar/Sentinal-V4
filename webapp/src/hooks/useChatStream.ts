import { useCallback, useRef, useState } from 'react'
import type { ChatMessage, Citation } from '../lib/api'
import { streamChat } from '../lib/api'

export function useChatStream() {
  const [messages,  setMessages]  = useState<ChatMessage[]>([])
  const [streaming, setStreaming] = useState(false)
  const [citations, setCitations] = useState<Citation[]>([])
  const abortRef                  = useRef<AbortController | null>(null)

  const send = useCallback(async (text: string) => {
    if (streaming || !text.trim()) return

    const userMsg: ChatMessage = { role: 'user', content: text }
    const allMsgs = [...messages, userMsg]
    setMessages([...allMsgs, { role: 'assistant', content: '' }])
    setStreaming(true)
    setCitations([])

    abortRef.current = new AbortController()
    let partial = ''

    try {
      for await (const token of streamChat(allMsgs, abortRef.current.signal)) {
        partial += token
        setMessages(prev => {
          const copy = [...prev]
          copy[copy.length - 1] = { role: 'assistant', content: partial }
          return copy
        })
      }
    } catch (e: unknown) {
      if ((e as Error)?.name !== 'AbortError') {
        setMessages(prev => {
          const copy = [...prev]
          copy[copy.length - 1] = { role: 'assistant', content: '⚠ Connection error. Please retry.' }
          return copy
        })
      }
    } finally {
      setStreaming(false)
      abortRef.current = null
    }
  }, [messages, streaming])

  const stop = useCallback(() => {
    abortRef.current?.abort()
    setStreaming(false)
  }, [])

  const reset = useCallback(() => {
    abortRef.current?.abort()
    setMessages([])
    setStreaming(false)
    setCitations([])
  }, [])

  return { messages, streaming, citations, send, stop, reset }
}
