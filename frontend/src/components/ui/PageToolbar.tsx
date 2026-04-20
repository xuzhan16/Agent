import type { ReactNode } from 'react'

interface PageToolbarProps {
  title?: ReactNode
  description?: ReactNode
  actions?: ReactNode
  className?: string
}

const PageToolbar = ({ title, description, actions, className = '' }: PageToolbarProps) => (
  <div className={`product-page-toolbar ${className}`.trim()}>
    <div>
      {title ? <strong>{title}</strong> : null}
      {description ? <p>{description}</p> : null}
    </div>
    {actions ? <div className="product-page-toolbar-actions">{actions}</div> : null}
  </div>
)

export default PageToolbar
