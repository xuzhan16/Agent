import type { ReactNode } from 'react'

interface StatusPillProps {
  status?: string
  children: ReactNode
  className?: string
}

const normalizeStatus = (status?: string) => {
  if (!status) return 'default'
  if (status === 'high_match') return 'high_match'
  if (['risk', 'warning'].includes(status)) return 'warning'
  if (['no_match', 'fail', 'danger'].includes(status)) return status
  if (['pass', 'success'].includes(status)) return status
  return 'unknown'
}

const StatusPill = ({ status, children, className = '' }: StatusPillProps) => (
  <span className={`product-status-pill status-${normalizeStatus(status)} ${className}`.trim()}>
    {children}
  </span>
)

export default StatusPill
