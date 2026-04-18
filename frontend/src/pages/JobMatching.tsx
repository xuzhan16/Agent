import { useState } from 'react'
import type { ReactNode } from 'react'
import { Card, Row, Col, Progress, Tag, Table, Tooltip, Divider, Alert, Button, Space, message } from 'antd'
import type { ColumnsType } from 'antd/es/table'
import {
  AimOutlined,
  ArrowRightOutlined,
  ArrowUpOutlined,
  BarChartOutlined,
  BulbOutlined,
  InfoCircleOutlined,
  SafetyCertificateOutlined,
  TrophyOutlined,
} from '@ant-design/icons'
import { useCareerStore } from '../store'
import { useNavigate } from 'react-router-dom'
import { careerApi } from '../services/api'
import TargetJobConfirmation from '../components/TargetJobConfirmation'
import type {
  HardInfoDisplay,
  RecommendedJobMatch,
  RequirementDistributionItem,
  TargetJobCandidate,
  TargetJobConfirmation as TargetJobConfirmationData,
  TargetJobProfileAssets,
  TargetJobMatch,
} from '../types'
import '../styles/JobMatching.css'

const emptyText = '暂无数据'

const firstNonEmpty = (...groups: Array<string[] | undefined>) => groups.find((group) => group && group.length) || []

const toPercent = (value?: number | null) => {
  if (value === undefined || value === null || Number.isNaN(value)) return 0
  return Math.round((value <= 1 ? value * 100 : value) * 10) / 10
}

const getScoreColor = (score: number) => {
  if (score >= 80) return '#16a34a'
  if (score >= 70) return '#f59e0b'
  if (score >= 60) return '#f97316'
  return '#ef4444'
}

const getRiskMeta = (risk?: string) => {
  if (risk === 'high_match') {
    return { label: '高匹配', color: 'green', className: 'risk-high' }
  }
  if (risk === 'no_match') {
    return { label: '不匹配', color: 'red', className: 'risk-low' }
  }
  if (risk === 'unknown') {
    return { label: '资产不足', color: 'default', className: 'risk-unknown' }
  }
  return { label: risk ? '有风险' : '待评估', color: 'orange', className: 'risk-mid' }
}

const passText = (value?: boolean | null) => {
  if (value === undefined || value === null) return '待确认'
  return value ? '通过' : '未通过'
}

const getResolutionStatus = (source?: TargetJobMatch | TargetJobProfileAssets | RecommendedJobMatch) => (
  source
    ? (
      source.evaluation_status
      || source.resolution_status
      || (source as TargetJobMatch).job_name_resolution?.resolution_status
      || (source as TargetJobProfileAssets).asset_resolution?.resolution_status
    )
    : undefined
)

const isNeedsConfirmation = (source?: TargetJobMatch | TargetJobProfileAssets | RecommendedJobMatch) => (
  getResolutionStatus(source) === 'needs_confirmation'
)

const normalizeCandidateJobs = (...sources: Array<TargetJobMatch | TargetJobProfileAssets | undefined>): TargetJobCandidate[] => {
  const candidates = sources.flatMap((source) => (
    source?.candidate_jobs
    || (source as TargetJobMatch | undefined)?.job_name_resolution?.candidate_jobs
    || (source as TargetJobProfileAssets | undefined)?.asset_resolution?.candidate_jobs
    || []
  ))
  const seen = new Set<string>()
  return candidates
    .map((candidate) => (typeof candidate === 'string' ? { standard_job_name: candidate } : candidate))
    .filter((candidate) => {
      const name = candidate.standard_job_name
      if (!name || seen.has(name)) return false
      seen.add(name)
      return true
    })
}

const getOptionalMatchDisplayScore = (match?: TargetJobMatch | RecommendedJobMatch, fallbackScore?: number) => {
  if (isNeedsConfirmation(match)) return null
  const value = match?.display_match_score ?? match?.asset_match_score ?? match?.overall_match_score ?? fallbackScore
  return value === undefined || value === null ? null : Math.round(value)
}

const getMatchDisplayScore = (match?: TargetJobMatch | RecommendedJobMatch, fallbackScore?: number) => (
  getOptionalMatchDisplayScore(match, fallbackScore) ?? 0
)

const getRuleScore = (match?: TargetJobMatch | RecommendedJobMatch, fallbackScore?: number) => (
  Math.round(match?.rule_match_score ?? match?.overall_match_score ?? fallbackScore ?? 0)
)

const getAssetScore = (match?: TargetJobMatch | RecommendedJobMatch) => (
  Math.round(match?.asset_match_score ?? match?.display_match_score ?? 0)
)

const getContestStatusText = (match?: TargetJobMatch | RecommendedJobMatch) => {
  const contest = match?.contest_evaluation
  if (isNeedsConfirmation(match)) return '待确认'
  if (!match?.asset_found || match?.evaluation_status === 'insufficient_asset') return '资产不足'
  if (contest?.contest_match_success === undefined || contest?.contest_match_success === null) return '待确认'
  return contest?.contest_match_success ? '通过' : '未通过'
}

const ScoreGuideCard = ({ targetMatch }: { targetMatch?: TargetJobMatch }) => {
  const pending = isNeedsConfirmation(targetMatch)
  const ruleScore = getRuleScore(targetMatch)
  const assetScore = getAssetScore(targetMatch)
  const knowledgePercent = toPercent(targetMatch?.skill_knowledge_match?.knowledge_point_accuracy)
  const contestText = getContestStatusText(targetMatch)

  const guideItems = [
    {
      title: '旧规则总分',
      value: pending ? '待确认' : `${ruleScore}%`,
      tone: 'blue',
      desc: '原有 job_match_scorer 的综合推荐分，偏兼容旧链路和整体推荐解释，不作为赛题硬通过依据。',
    },
    {
      title: '赛题资产分',
      value: pending ? '待确认' : `${assetScore}%`,
      tone: 'green',
      desc: '基于后处理资产计算，综合学历、专业、证书硬门槛和岗位知识点覆盖，是本页主展示分。',
    },
    {
      title: '知识覆盖率',
      value: pending ? '待确认' : `${knowledgePercent}%`,
      tone: 'purple',
      desc: '学生已掌握的岗位必备知识点 / 岗位必备知识点总数，达到 80% 才算技能评测通过。',
    },
    {
      title: '赛题评测',
      value: contestText,
      tone: contestText === '通过' ? 'green' : 'orange',
      desc: '最终赛题口径：学历、专业、证书三项硬信息通过，并且知识覆盖率达标，才算匹配成功。',
    },
  ]

  return (
    <Card className="match-card section-card score-guide-card" title={<span><InfoCircleOutlined /> 这些指标怎么看</span>}>
      {targetMatch?.score_explanation && (
        <Alert
          className="score-guide-alert"
          message="当前目标岗位评分说明"
          description={targetMatch.score_explanation}
          type={targetMatch.contest_evaluation?.contest_match_success ? 'success' : 'warning'}
          showIcon
        />
      )}
      {pending && (
        <Alert
          className="score-guide-alert"
          message="当前目标岗位需要先确认本地标准岗位"
          description={targetMatch?.score_explanation || targetMatch?.message || '系统已找到候选岗位，请先选择一个标准岗位后再计算赛题资产分、知识点覆盖率和最终评测结果。'}
          type="warning"
          showIcon
        />
      )}
      <Row gutter={[16, 16]}>
        {guideItems.map((item) => (
          <Col xs={24} md={12} xl={6} key={item.title}>
            <div className={`score-guide-item score-guide-${item.tone}`}>
              <span>{item.title}</span>
              <strong>{item.value}</strong>
              <p>{item.desc}</p>
            </div>
          </Col>
        ))}
      </Row>
    </Card>
  )
}

const DistributionBars = ({
  title,
  data,
  emptyHint = '暂无分布数据',
}: {
  title: string
  data?: RequirementDistributionItem[]
  emptyHint?: string
}) => {
  const items = (data || []).slice(0, 5)
  return (
    <div className="distribution-block">
      <div className="distribution-title">{title}</div>
      {items.length > 0 ? (
        items.map((item, index) => {
          const percent = toPercent(item.ratio)
          return (
            <div className="distribution-row" key={`${title}-${item.name || index}`}>
              <div className="distribution-label" title={item.name || emptyText}>
                {item.name || emptyText}
              </div>
              <div className="distribution-track">
                <div className="distribution-fill" style={{ width: `${Math.min(percent, 100)}%` }} />
              </div>
              <div className="distribution-value">
                {percent}%{item.count ? ` · ${item.count}` : ''}
              </div>
            </div>
          )
        })
      ) : (
        <div className="empty-inline">{emptyHint}</div>
      )}
    </div>
  )
}

const TagList = ({
  items,
  color = 'blue',
  limit = 10,
  empty = '暂无数据',
}: {
  items?: string[]
  color?: string
  limit?: number
  empty?: string
}) => {
  const values = (items || []).filter(Boolean).slice(0, limit)
  if (!values.length) return <span className="empty-inline">{empty}</span>
  return (
    <Space size={[6, 8]} wrap>
      {values.map((item) => (
        <Tag key={item} color={color} className="soft-tag">
          {item}
        </Tag>
      ))}
    </Space>
  )
}

const MetricPill = ({ label, value, tone = 'blue' }: { label: string; value: string; tone?: string }) => (
  <div className={`hero-metric hero-metric-${tone}`}>
    <span>{label}</span>
    <strong>{value || emptyText}</strong>
  </div>
)

const MatchCompareCard = ({
  title,
  match,
  fallbackScore,
  accent,
  description,
}: {
  title: string
  match?: TargetJobMatch | RecommendedJobMatch
  fallbackScore?: number
  accent: 'target' | 'recommended'
  description?: string
}) => {
  const pending = isNeedsConfirmation(match)
  const score = getMatchDisplayScore(match, fallbackScore)
  const ruleScore = getRuleScore(match, fallbackScore)
  const assetScore = getAssetScore(match)
  const risk = getRiskMeta(match?.risk_level)
  const knowledgePercent = toPercent(match?.skill_knowledge_match?.knowledge_point_accuracy)
  const hardPass = match?.contest_evaluation?.hard_info_pass
  const skillPass = match?.contest_evaluation?.skill_accuracy_pass
  const contestPass = match?.contest_evaluation?.contest_match_success
  const contestText = getContestStatusText(match)
  const reason = (match as RecommendedJobMatch | undefined)?.recommendation_reason || description || '暂无详细说明'

  return (
    <Card className={`match-card compare-card compare-card-${accent}`}>
      <div className="compare-card-top">
        <div>
          <span className="compare-eyebrow">{title}</span>
          <h2>{match?.job_name || emptyText}</h2>
        </div>
        <Tag color={risk.color}>{risk.label}</Tag>
      </div>
      <div className="compare-score-row">
        <Progress
          type="circle"
          percent={score}
          width={120}
          strokeColor={getScoreColor(score)}
          format={() => <span className="compare-score">{pending ? '待确认' : `${score}%`}</span>}
        />
        <div className="compare-facts">
          <div><span>赛题资产分</span><strong>{pending ? '待确认' : `${assetScore}%`}</strong></div>
          <div><span>旧规则总分</span><strong>{pending ? '仅供参考' : `${ruleScore}%`}</strong></div>
          <div><span>硬门槛</span><strong>{passText(hardPass)}</strong></div>
          <div><span>知识点</span><strong>{knowledgePercent}%</strong></div>
          <div><span>技能达标</span><strong>{passText(skillPass)}</strong></div>
          <div><span>赛题评测</span><strong>{contestText || passText(contestPass)}</strong></div>
        </div>
      </div>
      {pending && (
        <Alert
          className="compare-pending-alert"
          message="请先确认本地标准岗位"
          description={match?.message || '当前目标岗位未唯一命中标准岗位资产，暂不计算最终赛题评测分。'}
          type="warning"
          showIcon
        />
      )}
      <p className="compare-reason">{reason}</p>
    </Card>
  )
}

const RiskCard = ({
  title,
  icon,
  risk,
  studentValue,
  gateValue,
  ratioLabel,
  message,
}: {
  title: string
  icon: ReactNode
  risk?: string
  studentValue: string
  gateValue: string
  ratioLabel: string
  message?: string
}) => {
  const riskMeta = getRiskMeta(risk)
  return (
    <div className={`risk-card ${riskMeta.className}`}>
      <div className="risk-card-title">
        <span>{icon}</span>
        <strong>{title}</strong>
        <Tag color={riskMeta.color}>{riskMeta.label}</Tag>
      </div>
      <div className="risk-field"><span>学生侧</span><b>{studentValue || emptyText}</b></div>
      <div className="risk-field"><span>岗位侧</span><b>{gateValue || emptyText}</b></div>
      <div className="risk-field"><span>覆盖情况</span><b>{ratioLabel}</b></div>
      <p>{message || '暂无风险说明'}</p>
    </div>
  )
}

const HardInfoRiskCards = ({ hardInfo }: { hardInfo?: HardInfoDisplay }) => {
  const degree = hardInfo?.degree
  const major = hardInfo?.major
  const certificate = hardInfo?.certificate
  return (
    <Row gutter={[16, 16]}>
      <Col xs={24} lg={8}>
        <RiskCard
          title="学历匹配"
          icon={<SafetyCertificateOutlined />}
          risk={degree?.risk_level}
          studentValue={degree?.student_value || ''}
          gateValue={degree?.mainstream_requirement || ''}
          ratioLabel={`可覆盖 ${toPercent(degree?.qualified_ratio)}%，更高要求 ${toPercent(degree?.higher_requirement_ratio)}%`}
          message={degree?.message}
        />
      </Col>
      <Col xs={24} lg={8}>
        <RiskCard
          title="专业匹配"
          icon={<AimOutlined />}
          risk={major?.risk_level}
          studentValue={major?.student_value || ''}
          gateValue={(major?.mainstream_majors || []).slice(0, 3).join('、')}
          ratioLabel={`匹配占比 ${toPercent(major?.matched_ratio)}%`}
          message={major?.message}
        />
      </Col>
      <Col xs={24} lg={8}>
        <RiskCard
          title="证书匹配"
          icon={<TrophyOutlined />}
          risk={certificate?.risk_level}
          studentValue={(certificate?.student_values || []).join('、')}
          gateValue={firstNonEmpty(certificate?.must_have_certificates, certificate?.preferred_certificates).slice(0, 3).join('、')}
          ratioLabel={`命中比例 ${toPercent(certificate?.matched_ratio)}%`}
          message={certificate?.message}
        />
      </Col>
    </Row>
  )
}

const KnowledgePointPanel = ({ match }: { match?: TargetJobMatch }) => {
  if (isNeedsConfirmation(match)) {
    return (
      <Card className="match-card" title={<span><BulbOutlined /> 技能知识点覆盖</span>}>
        <Alert
          message="知识点覆盖率待确认"
          description="请先选择本次评估采用的本地标准岗位，系统随后会基于该岗位的必备知识点重新计算覆盖率。"
          type="warning"
          showIcon
        />
      </Card>
    )
  }
  const knowledge = match?.skill_knowledge_match
  const accuracy = toPercent(knowledge?.knowledge_point_accuracy)
  return (
    <Card className="match-card" title={<span><BulbOutlined /> 技能知识点覆盖</span>}>
      <Row gutter={[24, 24]} align="middle">
        <Col xs={24} md={8}>
          <div className="knowledge-score">
            <Progress
              type="circle"
              percent={accuracy}
              width={150}
              strokeColor={getScoreColor(accuracy)}
              format={() => <strong>{accuracy}%</strong>}
            />
            <p>{knowledge?.pass ? '达到赛题 80% 要求' : '仍需补齐关键知识点'}</p>
          </div>
        </Col>
        <Col xs={24} md={16}>
          <div className="knowledge-group">
            <span>已掌握知识点</span>
            <TagList items={knowledge?.matched_knowledge_points} color="green" empty="暂无已命中知识点" />
          </div>
          <Divider />
          <div className="knowledge-group">
            <span>缺失知识点</span>
            <TagList items={knowledge?.missing_knowledge_points} color="orange" empty="暂无明显缺口" />
          </div>
          <Divider />
          <div className="knowledge-group">
            <span>加分知识点</span>
            <TagList items={knowledge?.preferred_knowledge_points} color="blue" empty="暂无加分知识点" />
          </div>
        </Col>
      </Row>
    </Card>
  )
}

const JobMatching = () => {
  const studentInfo = useCareerStore((state) => state.studentInfo)
  const studentProfile = useCareerStore((state) => state.studentProfile)
  const jobMatches = useCareerStore((state) => state.jobMatches)
  const setJobMatches = useCareerStore((state) => state.setJobMatches)
  const setCareerPath = useCareerStore((state) => state.setCareerPath)
  const setLoading = useCareerStore((state) => state.setLoading)
  const setError = useCareerStore((state) => state.setError)
  const navigate = useNavigate()
  const [generatingPath, setGeneratingPath] = useState(false)
  const [confirmingTarget, setConfirmingTarget] = useState(false)

  if (!studentInfo || jobMatches.length === 0) {
    return (
      <div className="job-matching-container">
        <h1 className="page-title">📊 岗位匹配</h1>
        <p className="page-description">基于你的学生画像，系统自动计算与各个岗位的匹配度</p>
        <Card className="match-card" style={{ textAlign: 'center' }}>
          <Alert message="暂无匹配结果" description="暂无匹配结果，请先完成画像信息录入。" type="info" showIcon />
          <Button type="primary" style={{ marginTop: 24 }} onClick={() => navigate('/profile')}>
            前往学生画像
          </Button>
        </Card>
      </div>
    )
  }

  const primaryMatch = jobMatches[0]
  const rawProfile = primaryMatch.match_input_payload?.job_profile?.raw_job_profile_result
  const targetJobMatch = primaryMatch.target_job_match
  const recommendedJobMatch = primaryMatch.recommended_job_match
  const ranking = primaryMatch.recommendation_ranking || []
  const targetAssets = primaryMatch.target_job_profile_assets || rawProfile?.target_job_profile_assets
  const targetName = targetJobMatch?.job_name || targetAssets?.standard_job_name || primaryMatch.job_name || '未明确目标岗位'
  const recommendedName = recommendedJobMatch?.job_name || ranking[0]?.job_name || '暂无推荐岗位'
  const targetNeedsConfirmation = isNeedsConfirmation(targetJobMatch) || isNeedsConfirmation(targetAssets)
  const targetScoreValue = getOptionalMatchDisplayScore(targetJobMatch, primaryMatch.match_score)
  const targetScore = targetScoreValue ?? 0
  const targetScoreText = targetNeedsConfirmation ? '待确认' : `${targetScore}%`
  const knowledgePercent = toPercent(targetJobMatch?.skill_knowledge_match?.knowledge_point_accuracy)
  const knowledgeText = targetNeedsConfirmation ? '待确认' : `${knowledgePercent}%`
  const targetCandidates = normalizeCandidateJobs(targetJobMatch, targetAssets)

  const matchDetails = [
    {
      category: '基础要求',
      score: Math.round(primaryMatch.dimension_scores?.basic_requirement_score || 0),
      required: '学历 / 专业 / 证书',
      status: (primaryMatch.dimension_scores?.basic_requirement_score || 0) >= 80 ? '符合' : '部分符合',
    },
    {
      category: '职业技能',
      score: Math.round(primaryMatch.dimension_scores?.vocational_skill_score || 0),
      required: '硬技能 / 工具技能',
      status: (primaryMatch.dimension_scores?.vocational_skill_score || 0) >= 80 ? '符合' : '部分符合',
    },
    {
      category: '职业素质',
      score: Math.round(primaryMatch.dimension_scores?.professional_quality_score || 0),
      required: '实践经历 / 软技能',
      status: (primaryMatch.dimension_scores?.professional_quality_score || 0) >= 80 ? '符合' : '部分符合',
    },
    {
      category: '发展潜力',
      score: Math.round(primaryMatch.dimension_scores?.development_potential_score || 0),
      required: '成长潜力 / 方向匹配',
      status: (primaryMatch.dimension_scores?.development_potential_score || 0) >= 80 ? '符合' : '部分符合',
    },
  ]

  const rankingData = ranking.length > 0
    ? ranking.map((item) => ({
      key: `${item.rank || item.job_name}`,
      rank: item.rank,
      jobName: item.job_name || emptyText,
      score: Math.round(item.display_match_score ?? item.overall_match_score ?? 0),
      hardInfoPass: item.hard_info_pass,
      knowledge: toPercent(item.knowledge_point_accuracy),
      risk: item.risk_level,
      reason: item.recommendation_reason || emptyText,
    }))
    : jobMatches.map((job, index) => ({
      key: `${index}`,
      rank: index + 1,
      jobName: job.job_name,
      score: Math.round(job.match_score),
      hardInfoPass: undefined,
      knowledge: 0,
      risk: undefined,
      reason: job.reasons?.join('、') || '暂无说明',
    }))

  const columns: ColumnsType<(typeof rankingData)[number]> = [
    {
      title: '排名',
      dataIndex: 'rank',
      key: 'rank',
      width: 80,
      render: (rank?: number) => <span className={rank === 1 ? 'rank-top' : ''}>#{rank || '-'}</span>,
    },
    {
      title: '岗位名称',
      dataIndex: 'jobName',
      key: 'jobName',
      render: (jobName: string) => (
        <Space size={6} wrap>
          <strong>{jobName}</strong>
          {jobName === targetName && <Tag color="blue">目标岗位</Tag>}
          {jobName === recommendedName && <Tag color="green">推荐岗位</Tag>}
        </Space>
      ),
    },
    {
      title: '赛题资产分',
      dataIndex: 'score',
      key: 'score',
      width: 150,
      render: (score: number) => (
        <div className="table-score">
          <span style={{ color: getScoreColor(score) }}>{score}%</span>
          <Progress percent={score} strokeColor={getScoreColor(score)} size="small" showInfo={false} />
        </div>
      ),
    },
    {
      title: '硬门槛',
      dataIndex: 'hardInfoPass',
      key: 'hardInfoPass',
      width: 100,
      render: (passed?: boolean) => <Tag color={passed ? 'green' : 'orange'}>{passed === undefined ? '旧版' : passText(passed)}</Tag>,
    },
    {
      title: '知识点',
      dataIndex: 'knowledge',
      key: 'knowledge',
      width: 110,
      render: (value: number) => `${value}%`,
    },
    {
      title: '风险',
      dataIndex: 'risk',
      key: 'risk',
      width: 100,
      render: (risk?: string) => {
        const meta = getRiskMeta(risk)
        return <Tag color={meta.color}>{meta.label}</Tag>
      },
    },
    {
      title: '推荐理由',
      dataIndex: 'reason',
      key: 'reason',
      render: (reason: string) => (
        <Tooltip title={reason}>
          <span className="reason-cell">{reason}</span>
        </Tooltip>
      ),
    },
  ]

  const handleGenerateCareerPath = async () => {
    if (!studentProfile || jobMatches.length === 0) {
      setError('学生画像或岗位匹配数据不完整')
      return
    }

    setGeneratingPath(true)
    setLoading(true)
    setError(null)

    try {
      const response = await careerApi.planCareerPath()
      if (response.success) {
        setCareerPath(response.data)
        navigate('/career')
      } else {
        setError(response.message || '职业路径规划失败，请重试')
      }
    } catch {
      setError('职业路径规划接口调用失败，请检查后端服务')
    } finally {
      setGeneratingPath(false)
      setLoading(false)
    }
  }

  const handleConfirmTargetJob = async (candidate: TargetJobCandidate) => {
    const confirmedName = candidate.standard_job_name
    if (!confirmedName) {
      message.warning('候选岗位名称为空，无法确认')
      return
    }

    setConfirmingTarget(true)
    setLoading(true)
    setError(null)

    try {
      const response = await careerApi.confirmTargetJob({
        requested_job_name: targetName,
        confirmed_standard_job_name: confirmedName,
      })

      if (!response.success) {
        setError(response.message || '目标岗位确认失败，请重试')
        message.error(response.message || '目标岗位确认失败')
        return
      }

      const refreshed = await careerApi.matchJobs()
      if (refreshed.success) {
        setJobMatches(refreshed.data || [])
      } else {
        const data = response.data || {}
        setJobMatches([
          {
            ...primaryMatch,
            target_job_match: data.target_job_match as TargetJobMatch,
            target_job_profile_assets: data.target_job_profile_assets as TargetJobProfileAssets,
            target_job_confirmation: data.target_job_confirmation as TargetJobConfirmationData,
          },
          ...jobMatches.slice(1),
        ])
      }

      message.success(`已采用“${confirmedName}”作为本次评估标准岗位`)
    } catch {
      setError('目标岗位确认接口调用失败，请检查后端服务')
      message.error('目标岗位确认接口调用失败')
    } finally {
      setConfirmingTarget(false)
      setLoading(false)
    }
  }

  return (
    <div className="job-matching-container">
      <section className="matching-hero">
        <div>
          <span className="hero-kicker">岗位画像与人岗匹配</span>
          <h1>把岗位要求、学生能力和推荐路径放在同一张决策图里</h1>
          <p>基于岗位样本、知识图谱、语义知识库和学生画像生成，帮助你判断目标岗位风险与更稳的推荐岗位。</p>
        </div>
        <div className="hero-metrics">
          <MetricPill label="目标岗位" value={targetName} />
          <MetricPill label="推荐岗位" value={recommendedName} tone="green" />
          <MetricPill label="目标匹配分" value={targetScoreText} tone="orange" />
          <MetricPill label="知识点覆盖" value={knowledgeText} tone="purple" />
        </div>
      </section>

      <ScoreGuideCard targetMatch={targetJobMatch} />

      {targetNeedsConfirmation && (
        <TargetJobConfirmation
          requestedJobName={targetName}
          candidates={targetCandidates}
          loading={confirmingTarget}
          onConfirm={handleConfirmTargetJob}
        />
      )}

      <Card className="match-card section-card profile-link-card">
        <div>
          <span className="profile-link-kicker">全局岗位知识库</span>
          <h3>想先了解岗位本身的要求？</h3>
          <p>前往“岗位画像”栏目集中查看 10 个核心岗位的学历、专业、证书分布和知识点要求。</p>
        </div>
        <Button type="primary" onClick={() => navigate('/job-profile')}>
          查看岗位画像
        </Button>
      </Card>

      <Card className="match-card section-card" title={<span><BarChartOutlined /> 目标岗位主流画像</span>}>
        {targetNeedsConfirmation ? (
          <Alert
            message="目标岗位画像待确认"
            description="请先在上方候选卡片中选择一个本地标准岗位。确认后，系统会基于该标准岗位展示学历、专业、证书分布和知识点要求。"
            type="warning"
            showIcon
          />
        ) : targetAssets?.asset_found === false ? (
          <Alert
            message="当前目标岗位未命中标准岗位画像资产"
            description={targetAssets.message || '系统会优先尝试后端岗位名归一；若仍未命中，可参考推荐岗位或全局岗位画像。'}
            type="info"
            showIcon
          />
        ) : (
          <Row gutter={[24, 24]}>
            <Col xs={24} lg={8}>
              <div className="target-profile-panel">
                <h2>{targetAssets?.standard_job_name || targetName}</h2>
                <p>{targetAssets?.job_category || emptyText}</p>
                <div className="profile-stat-list">
                  <span>样本数 <b>{targetAssets?.sample_count || targetJobMatch?.sample_count || 0}</b></span>
                  <span>主流学历 <b>{targetAssets?.mainstream_degree || emptyText}</b></span>
                  <span>学历门槛 <b>{targetAssets?.degree_gate || emptyText}</b></span>
                </div>
                <Divider />
                <div className="knowledge-group">
                  <span>主流专业</span>
                  <TagList items={targetAssets?.mainstream_majors || targetAssets?.major_gate_set} color="blue" limit={6} />
                </div>
                <div className="knowledge-group">
                  <span>高频证书</span>
                  <TagList items={targetAssets?.mainstream_certificates || targetAssets?.preferred_certificates} color="green" limit={6} />
                </div>
              </div>
            </Col>
            <Col xs={24} lg={16}>
              <Row gutter={[16, 16]}>
                <Col xs={24} md={8}>
                  <DistributionBars title="学历分布" data={targetAssets?.degree_distribution || targetJobMatch?.requirement_distributions?.degree_distribution} />
                </Col>
                <Col xs={24} md={8}>
                  <DistributionBars title="专业分布" data={targetAssets?.major_distribution || targetJobMatch?.requirement_distributions?.major_distribution} />
                </Col>
                <Col xs={24} md={8}>
                  <DistributionBars title="证书分布" data={targetAssets?.certificate_distribution || targetJobMatch?.requirement_distributions?.certificate_distribution} />
                  <div className="no-cert-ratio">
                    无明确证书要求：{toPercent(targetAssets?.no_certificate_requirement_ratio || targetJobMatch?.requirement_distributions?.no_certificate_requirement_ratio)}%
                  </div>
                </Col>
              </Row>
              <Divider />
              <Row gutter={[16, 16]}>
                <Col xs={24} md={12}>
                  <div className="knowledge-group">
                    <span>必备知识点</span>
                    <TagList items={targetAssets?.required_knowledge_points} color="orange" limit={10} empty="暂无明确必备知识点" />
                  </div>
                </Col>
                <Col xs={24} md={12}>
                  <div className="knowledge-group">
                    <span>加分知识点</span>
                    <TagList items={targetAssets?.preferred_knowledge_points} color="blue" limit={10} empty="暂无加分知识点" />
                  </div>
                </Col>
              </Row>
            </Col>
          </Row>
        )}
      </Card>

      <Row gutter={[24, 24]} className="section-card">
        <Col xs={24} lg={12}>
          <MatchCompareCard
            title="用户目标岗位"
            match={targetJobMatch}
            fallbackScore={primaryMatch.match_score}
            accent="target"
            description={primaryMatch.analysis_summary}
          />
        </Col>
        <Col xs={24} lg={12}>
          <MatchCompareCard
            title="系统最推荐岗位"
            match={recommendedJobMatch}
            accent="recommended"
            description={primaryMatch.recommendation}
          />
        </Col>
      </Row>

      <Card className="match-card section-card" title={<span><SafetyCertificateOutlined /> 学历 / 专业 / 证书风险</span>}>
        <HardInfoRiskCards hardInfo={targetJobMatch?.hard_info_display} />
      </Card>

      <div className="section-card">
        <KnowledgePointPanel match={targetJobMatch} />
      </div>

      <Row gutter={[24, 24]} className="section-card">
        <Col xs={24}>
          <Card className="match-card" title={<span><ArrowUpOutlined /> 推荐岗位 Top N</span>}>
            <Table columns={columns} dataSource={rankingData} pagination={false} scroll={{ x: 980 }} rowClassName={(_, index) => (index === 0 ? 'top-ranking-row' : '')} />
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} className="section-card">
        <Col xs={24}>
          <Card className="match-card" title="详细匹配评分">
            <Row gutter={[24, 24]}>
              {matchDetails.map((detail) => (
                <Col xs={24} sm={12} key={detail.category}>
                  <div className="match-detail-item">
                    <div className="detail-head">
                      <span>{detail.category}</span>
                      <Tooltip title={detail.required}>
                        <span>{detail.score}/100</span>
                      </Tooltip>
                    </div>
                    <Progress percent={detail.score} strokeColor={getScoreColor(detail.score)} size="small" showInfo={false} />
                    <div className="detail-foot">
                      <span>{detail.required}</span>
                      <Tag color={detail.status === '符合' ? 'green' : 'orange'}>{detail.status}</Tag>
                    </div>
                  </div>
                </Col>
              ))}
            </Row>
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} className="section-card">
        <Col xs={24}>
          <Card className="match-card" title="💡 改进建议">
            {(primaryMatch.improvement_suggestions?.length || primaryMatch.recommendation) ? (
              <>
                <ol className="suggestion-list">
                  {(primaryMatch.improvement_suggestions || []).map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ol>
                {primaryMatch.recommendation && (
                  <Alert message="投递建议" description={primaryMatch.recommendation} type="info" showIcon style={{ marginTop: 16 }} />
                )}
              </>
            ) : (
              <span className="empty-inline">当前没有更详细的改进建议。</span>
            )}
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} className="section-card">
        <Col xs={24}>
          <Card className="match-card next-step-card">
            <Space direction="vertical" size="large">
              <div>
                <h3>🚀 下一步：职业路径规划</h3>
                <p>基于岗位匹配结果，为你制定详细的职业发展规划和行动建议</p>
              </div>
              <Button type="primary" size="large" icon={<ArrowRightOutlined />} loading={generatingPath} onClick={handleGenerateCareerPath}>
                {generatingPath ? '正在规划职业路径...' : '开始职业路径规划'}
              </Button>
            </Space>
          </Card>
        </Col>
      </Row>
    </div>
  )
}

export default JobMatching
