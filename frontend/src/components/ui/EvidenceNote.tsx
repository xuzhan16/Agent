import type { ReactNode } from 'react'
import { Tag } from 'antd'

interface EvidenceNoteProps {
  title: ReactNode
  description?: ReactNode
  sources?: string[]
  compact?: boolean
  className?: string
}

const EvidenceNote = ({ title, description, sources = [], compact = false, className = '' }: EvidenceNoteProps) => (
  <div className={`product-evidence-note ${compact ? 'compact' : ''} ${className}`.trim()}>
    <div>
      <strong>{title}</strong>
      {description ? <p>{description}</p> : null}
    </div>
    {sources.length > 0 ? (
      <div className="product-evidence-sources">
        {sources.map((source) => (
          <Tag key={source}>{source}</Tag>
        ))}
      </div>
    ) : null}
  </div>
)

export default EvidenceNote
