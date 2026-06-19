import { useCallback, useState } from 'react'
import type { Citation } from '../lib/api'

export function useCitationPane() {
  const [open,     setOpen]     = useState(false)
  const [citations, setCitations] = useState<Citation[]>([])
  const [selected,  setSelected]  = useState<Citation | null>(null)

  const show = useCallback((cits: Citation[]) => {
    setCitations(cits)
    setSelected(cits[0] ?? null)
    setOpen(true)
  }, [])

  const hide = useCallback(() => {
    setOpen(false)
  }, [])

  const select = useCallback((c: Citation) => setSelected(c), [])

  return { open, citations, selected, show, hide, select }
}
