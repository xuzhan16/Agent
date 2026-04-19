interface DistributionItem {
  name?: string
  ratio?: number
  count?: number
}

interface DistributionBarsProps {
  title: string
  data?: DistributionItem[] | null
  maxItems?: number
  emptyText?: string
  accent?: 'blue' | 'green' | 'orange'
  className?: string
}

const toPercent = (value?: number | null) => {
  if (value === undefined || value === null || Number.isNaN(value)) return 0
  return Math.round((value <= 1 ? value * 100 : value) * 10) / 10
}

const DistributionBars = ({
  title,
  data,
  maxItems = 5,
  emptyText = '暂无分布数据',
  accent = 'blue',
  className = '',
}: DistributionBarsProps) => {
  const items = (data || []).slice(0, maxItems)

  return (
    <div className={`product-distribution-bars accent-${accent} ${className}`.trim()}>
      <div className="product-distribution-title">{title}</div>
      {items.length > 0 ? (
        items.map((item, index) => {
          const percent = toPercent(item.ratio)
          return (
            <div className="product-distribution-row" key={`${title}-${item.name || index}`}>
              <span className="product-distribution-label" title={item.name || emptyText}>
                {item.name || emptyText}
              </span>
              <div className="product-distribution-track">
                <div className="product-distribution-fill" style={{ width: `${Math.min(percent, 100)}%` }} />
              </div>
              <span className="product-distribution-value">
                {percent}%{item.count ? ` · ${item.count}` : ''}
              </span>
            </div>
          )
        })
      ) : (
        <span className="product-empty-inline">{emptyText}</span>
      )}
    </div>
  )
}

export default DistributionBars
