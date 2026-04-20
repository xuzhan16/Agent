import { Progress, Tag, Tooltip } from 'antd'
import type { RecommendedJobMatch, TargetJobMatch } from '../../types'

interface MatchDecisionPanelProps {
  targetMatch?: TargetJobMatch
  recommendedMatch?: RecommendedJobMatch
  fallbackTargetScore?: number
  targetName?: string
  recommendedName?: string
}

const emptyText = '暂无数据'

const toPercent = (value?: number | null) => {
  if (value === undefined || value === null || Number.isNaN(value)) return 0
  return Math.round((value <= 1 ? value * 100 : value) * 10) / 10
}

const scoreOf = (match?: TargetJobMatch, fallback?: number) => Math.round(
  match?.display_match_score
  ?? match?.asset_match_score
  ?? match?.overall_match_score
  ?? fallback
  ?? 0,
)

const scoreColor = (score: number) => {
  if (score >= 80) return '#16a34a'
  if (score >= 70) return '#f59e0b'
  if (score >= 60) return '#f97316'
  return '#dc2626'
}

const riskMeta = (risk?: string) => {
  if (risk === 'high_match') return { text: '高匹配', color: 'green' }
  if (risk === 'no_match') return { text: '不匹配', color: 'red' }
  if (risk === 'unknown') return { text: '资产不足', color: 'default' }
  return { text: risk ? '有风险' : '待评估', color: 'orange' }
}

const passText = (value?: boolean | null) => {
  if (value === undefined || value === null) return '待确认'
  return value ? '通过' : '未通过'
}

const isPending = (match?: TargetJobMatch) => {
  const status = match?.evaluation_status || match?.resolution_status || match?.job_name_resolution?.resolution_status
  return status === 'needs_confirmation'
}

const MatchSide = ({
  label,
  match,
  fallbackScore,
  fallbackName,
  featured = false,
}: {
  label: string
  match?: TargetJobMatch | RecommendedJobMatch
  fallbackScore?: number
  fallbackName?: string
  featured?: boolean
}) => {
  const pending = isPending(match)
  const score = scoreOf(match, fallbackScore)
  const risk = riskMeta(match?.risk_level)
  const knowledge = toPercent(match?.skill_knowledge_match?.knowledge_point_accuracy)
  const ability = Math.round(match?.ability_match?.overall_ability_match_score || 0)
  const reason = (match as RecommendedJobMatch | undefined)?.recommendation_reason || match?.message || '暂无详细解释'

  return (
    <div className={`product-match-side ${featured ? 'featured' : ''}`}>
      <div className="product-match-side-head">
        <div>
          <span>{label}</span>
          <h3>{match?.job_name || fallbackName || emptyText}</h3>
        </div>
        <Tag color={risk.color}>{risk.text}</Tag>
      </div>
      <div className="product-match-score-row">
        <Progress
          type="circle"
          percent={pending ? 0 : score}
          width={112}
          strokeColor={scoreColor(score)}
          format={() => <strong>{pending ? '待确认' : `${score}%`}</strong>}
        />
        <div className="product-match-facts">
          <div><span>赛题资产分</span><b>{pending ? '待确认' : `${Math.round(match?.asset_match_score ?? score)}%`}</b></div>
          <div><span>硬门槛</span><b>{passText(match?.contest_evaluation?.hard_info_pass)}</b></div>
          <div><span>知识点覆盖</span><b>{pending ? '待确认' : `${knowledge}%`}</b></div>
          <div><span>七维能力</span><b>{ability ? `${ability}%` : '暂无'}</b></div>
        </div>
      </div>
      <Tooltip title={reason}>
        <p>{reason}</p>
      </Tooltip>
    </div>
  )
}

const MatchDecisionPanel = ({
  targetMatch,
  recommendedMatch,
  fallbackTargetScore,
  targetName,
  recommendedName,
}: MatchDecisionPanelProps) => (
  <section className="product-match-decision-panel">
    <div className="product-decision-summary">
      <span>决策结论</span>
      <h2>{recommendedMatch?.job_name ? `优先关注 ${recommendedMatch.job_name}` : '等待生成推荐岗位'}</h2>
      <p>
        推荐岗位来自后端人岗匹配结果。页面只展示已有评分、门槛、知识点和能力证据，不在前端重新计算推荐逻辑。
      </p>
    </div>
    <div className="product-match-sides">
      <MatchSide label="用户目标岗位" match={targetMatch} fallbackScore={fallbackTargetScore} fallbackName={targetName} />
      <MatchSide label="系统推荐岗位" match={recommendedMatch} fallbackName={recommendedName} featured />
    </div>
  </section>
)

export default MatchDecisionPanel
