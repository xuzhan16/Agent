import type { ReactNode } from 'react'
import { Card } from 'antd'

interface SectionCardProps {
  title?: ReactNode
  extra?: ReactNode
  children: ReactNode
  className?: string
}

const SectionCard = ({ title, extra, children, className = '' }: SectionCardProps) => (
  <Card className={`product-section-card ${className}`.trim()} title={title} extra={extra}>
    {children}
  </Card>
)

export default SectionCard
