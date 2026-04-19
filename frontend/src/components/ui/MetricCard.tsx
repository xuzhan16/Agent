import type { ReactNode } from 'react'

interface MetricCardProps {
  label: ReactNode
  value: ReactNode
  description?: ReactNode
  tone?: 'blue' | 'green' | 'orange' | 'purple' | 'default' | string
  className?: string
}

const MetricCard = ({ label, value, description, tone = 'default', className = '' }: MetricCardProps) => (
  <div className={`product-metric-card tone-${tone} ${className}`.trim()}>
    <span className="product-metric-label">{label}</span>
    <strong className="product-metric-value">{value}</strong>
    {description ? <span className="product-metric-description">{description}</span> : null}
  </div>
)

export default MetricCard
