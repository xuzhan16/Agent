import type { ReactNode } from 'react'

interface HeroPanelProps {
  eyebrow?: ReactNode
  title: ReactNode
  description?: ReactNode
  extra?: ReactNode
  className?: string
}

const HeroPanel = ({ eyebrow, title, description, extra, className = '' }: HeroPanelProps) => (
  <section className={`product-hero-panel ${className}`.trim()}>
    <div className="product-hero-content">
      {eyebrow ? <span className="product-eyebrow">{eyebrow}</span> : null}
      <h1 className="product-hero-title">{title}</h1>
      {description ? <p className="product-hero-description">{description}</p> : null}
    </div>
    {extra ? <div className="product-hero-extra">{extra}</div> : null}
  </section>
)

export default HeroPanel
