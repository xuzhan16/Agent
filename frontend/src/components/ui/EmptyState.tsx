import type { ReactNode } from 'react'
import { Alert } from 'antd'

interface EmptyStateProps {
  title: ReactNode
  description?: ReactNode
  action?: ReactNode
  status?: 'info' | 'warning' | 'error' | 'success'
  className?: string
}

const EmptyState = ({ title, description, action, status = 'info', className = '' }: EmptyStateProps) => (
  <div className={`product-empty-state empty-${status} ${className}`.trim()}>
    <Alert
      type={status}
      showIcon
      message={title}
      description={description}
    />
    {action ? <div className="product-empty-action">{action}</div> : null}
  </div>
)

export default EmptyState
