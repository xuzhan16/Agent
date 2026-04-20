import type { ReactNode } from 'react'
import { Tag } from 'antd'

export interface ActionListItem {
  title: ReactNode
  description?: ReactNode
  meta?: ReactNode
  status?: 'current' | 'next' | 'done' | 'blocked' | 'neutral'
}

interface ActionListProps {
  title?: ReactNode
  items?: ActionListItem[]
  emptyText?: ReactNode
  className?: string
}

const statusLabelMap: Record<string, string> = {
  current: '当前',
  next: '后续',
  done: '完成',
  blocked: '受阻',
  neutral: '计划',
}

const ActionList = ({ title, items = [], emptyText = '暂无行动建议', className = '' }: ActionListProps) => (
  <div className={`product-action-list ${className}`.trim()}>
    {title ? <h3>{title}</h3> : null}
    {items.length > 0 ? (
      <ol>
        {items.map((item, index) => (
          <li key={index} className={`action-${item.status || 'neutral'}`}>
            <div className="product-action-index">{index + 1}</div>
            <div className="product-action-content">
              <div className="product-action-title-row">
                <strong>{item.title}</strong>
                <Tag>{statusLabelMap[item.status || 'neutral']}</Tag>
              </div>
              {item.description ? <p>{item.description}</p> : null}
              {item.meta ? <span className="product-action-meta">{item.meta}</span> : null}
            </div>
          </li>
        ))}
      </ol>
    ) : (
      <div className="product-empty-inline">{emptyText}</div>
    )}
  </div>
)

export default ActionList
