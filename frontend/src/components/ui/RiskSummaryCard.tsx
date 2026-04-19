import type { ReactNode } from 'react'
import { Tag, Tooltip } from 'antd'

interface RiskSummaryCardProps {
  title: ReactNode
  riskLevel?: string
  studentValue?: ReactNode
  jobValue?: ReactNode
  ratio?: ReactNode
  message?: ReactNode
  icon?: ReactNode
  className?: string
}

const riskMeta = (riskLevel?: string) => {
  if (riskLevel === 'high_match') return { label: '高匹配', color: 'green', className: 'risk-high' }
  if (riskLevel === 'no_match') return { label: '不匹配', color: 'red', className: 'risk-low' }
  if (riskLevel === 'unknown') return { label: '资产不足', color: 'default', className: 'risk-unknown' }
  return { label: riskLevel ? '有风险' : '待评估', color: 'orange', className: 'risk-mid' }
}

const RiskSummaryCard = ({
  title,
  riskLevel,
  studentValue,
  jobValue,
  ratio,
  message,
  icon,
  className = '',
}: RiskSummaryCardProps) => {
  const meta = riskMeta(riskLevel)

  return (
    <div className={`product-risk-summary ${meta.className} ${className}`.trim()}>
      <div className="product-risk-head">
        <span className="product-risk-icon">{icon}</span>
        <strong>{title}</strong>
        <Tag color={meta.color}>{meta.label}</Tag>
      </div>
      <div className="product-risk-line"><span>学生侧</span><b>{studentValue || '暂无数据'}</b></div>
      <div className="product-risk-line"><span>岗位侧</span><b>{jobValue || '暂无数据'}</b></div>
      <div className="product-risk-line"><span>覆盖情况</span><b>{ratio || '待评估'}</b></div>
      <Tooltip title={message || '暂无风险说明'}>
        <p>{message || '暂无风险说明'}</p>
      </Tooltip>
    </div>
  )
}

export default RiskSummaryCard
