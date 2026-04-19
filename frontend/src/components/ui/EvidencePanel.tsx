import type { ReactNode } from 'react'
import { Collapse, Tag } from 'antd'

export interface EvidenceSourceItem {
  label: ReactNode
  value?: ReactNode
  status?: 'available' | 'missing' | 'warning'
}

interface EvidencePanelProps {
  title?: ReactNode
  description?: ReactNode
  sources?: EvidenceSourceItem[]
  children?: ReactNode
  defaultOpen?: boolean
  className?: string
}

const statusColorMap: Record<string, string> = {
  available: 'success',
  missing: 'default',
  warning: 'warning',
}

const EvidencePanel = ({
  title = '数据来源与证据',
  description,
  sources = [],
  children,
  defaultOpen = false,
  className = '',
}: EvidencePanelProps) => (
  <div className={`product-evidence-panel ${className}`.trim()}>
    <Collapse
      bordered={false}
      defaultActiveKey={defaultOpen ? ['evidence'] : []}
      items={[
        {
          key: 'evidence',
          label: (
            <div className="product-evidence-panel-label">
              <strong>{title}</strong>
              {description ? <span>{description}</span> : null}
            </div>
          ),
          children: (
            <div className="product-evidence-panel-body">
              {sources.length > 0 ? (
                <div className="product-evidence-panel-sources">
                  {sources.map((source, index) => (
                    <Tag key={`${String(source.label)}-${index}`} color={statusColorMap[source.status || 'available']}>
                      {source.label}{source.value ? `：${source.value}` : ''}
                    </Tag>
                  ))}
                </div>
              ) : null}
              {children ? <div className="product-evidence-panel-content">{children}</div> : null}
            </div>
          ),
        },
      ]}
    />
  </div>
)

export default EvidencePanel
