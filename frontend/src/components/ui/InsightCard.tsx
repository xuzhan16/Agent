import type { ReactNode } from 'react'
import { Tag } from 'antd'

interface InsightCardProps {
  eyebrow?: ReactNode
  title: ReactNode
  description?: ReactNode
  points?: ReactNode[]
  action?: ReactNode
  status?: 'success' | 'warning' | 'danger' | 'info' | 'neutral'
  className?: string
}

const InsightCard = ({
  eyebrow,
  title,
  description,
  points = [],
  action,
  status = 'neutral',
  className = '',
}: InsightCardProps) => (
  <div className={`product-insight-card insight-${status} ${className}`.trim()}>
    <div className="product-insight-head">
      {eyebrow ? <Tag>{eyebrow}</Tag> : null}
      <h3>{title}</h3>
    </div>
    {description ? <p className="product-insight-description">{description}</p> : null}
    {points.length > 0 ? (
      <ul className="product-insight-points">
        {points.map((point, index) => (
          <li key={index}>{point}</li>
        ))}
      </ul>
    ) : null}
    {action ? <div className="product-insight-action">{action}</div> : null}
  </div>
)

export default InsightCard
