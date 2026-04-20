import type { ReactNode } from 'react'

interface PageShellProps {
  children: ReactNode
  className?: string
}

const PageShell = ({ children, className = '' }: PageShellProps) => (
  <div className={`product-page-shell ${className}`.trim()}>{children}</div>
)

export default PageShell
