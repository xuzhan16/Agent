import { Card, Row, Col, Timeline, Button, Tag, Alert, Collapse } from 'antd'
import {
  ApartmentOutlined,
  ArrowRightOutlined,
  BulbOutlined,
  CheckCircleOutlined,
  NodeIndexOutlined,
  RiseOutlined,
  WarningOutlined,
} from '@ant-design/icons'
import { useCareerStore } from '../store'
import { useNavigate } from 'react-router-dom'
import type { CareerPathResult } from '../types'
import {
  ActionList,
  EmptyState,
  EvidencePanel,
  HeroPanel,
  InsightCard,
  MetricCard,
  PageShell,
  PageToolbar,
} from '../components/ui'
import '../styles/CareerPath.css'

const pathEmptyMessage = '当前目标岗位暂无可用晋升/转岗路径数据，系统不会强行生成路径。'

const safeList = <T,>(value?: T[] | null) => (Array.isArray(value) ? value : [])

const PathTimelineCard = ({
  title,
  paths,
  type,
}: {
  title: string
  paths: string[]
  type: 'direct' | 'transition'
}) => (
  <Card className="path-card target-path-card" title={title}>
    <Timeline
      items={paths.map((path, index) => ({
        dot: type === 'direct'
          ? <CheckCircleOutlined style={{ fontSize: 16, color: '#52c41a' }} />
          : <RiseOutlined style={{ fontSize: 16, color: '#1890ff' }} />,
        children: (
          <div className="path-item">
            <p style={{ margin: 0, fontWeight: 600 }}>{path}</p>
            {index === 0 && (
              <p style={{ margin: '4px 0 0 0', fontSize: 12, color: '#666' }}>
                {type === 'direct' ? '路径起点' : '转向起点'}
              </p>
            )}
            {index === paths.length - 1 && (
              <p style={{ margin: '4px 0 0 0', fontSize: 12, color: '#666' }}>
                {type === 'direct' ? '晋升目标' : '转向目标'}
              </p>
            )}
          </div>
        ),
      }))}
    />
  </Card>
)

const TargetPathSection = ({ careerPath }: { careerPath: CareerPathResult }) => {
  const directPath = safeList(careerPath.direct_path)
  const transitionPath = safeList(careerPath.transition_path)
  const longTermPath = safeList(careerPath.long_term_path)
  const hasPathData = careerPath.target_path_data_status === 'available'
    && (directPath.length > 0 || transitionPath.length > 0 || longTermPath.length > 0)

  if (!hasPathData) {
    return (
      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card className="path-card target-path-empty-card" title={<span><NodeIndexOutlined /> 目标岗位职业路径</span>}>
            <Alert
              message="当前目标岗位暂无路径数据"
              description={careerPath.target_path_data_message || pathEmptyMessage}
              type="info"
              showIcon
            />
            <p className="path-empty-explain">
              当前岗位在本地岗位图谱和离线岗位画像中暂未发现明确晋升/转岗关系。为保证结果可信，系统不会强行生成不存在的路径。
            </p>
          </Card>
        </Col>
      </Row>
    )
  }

  return (
    <>
      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24} lg={12}>
          <PathTimelineCard title="直接路径（真实晋升关系）" paths={directPath} type="direct" />
        </Col>
        <Col xs={24} lg={12}>
          <PathTimelineCard title="转向路径（真实转岗关系）" paths={transitionPath} type="transition" />
        </Col>
      </Row>

      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card className="path-card" title="长期发展路径（来自真实路径组合）">
            <div className="long-term-timeline">
              {longTermPath.length > 0 ? (
                longTermPath.map((path, index) => (
                  <div key={`${path}-${index}`} style={{ display: 'contents' }}>
                    <div className="timeline-stage">
                      <h4>{index === 0 ? '起点' : index === 1 ? '阶段 2' : index === 2 ? '阶段 3' : '长期'}</h4>
                      <p>{path}</p>
                    </div>
                    {index < longTermPath.length - 1 && <div className="timeline-arrow">→</div>}
                  </div>
                ))
              ) : (
                <p style={{ color: '#999' }}>暂无长期路径数据</p>
              )}
            </div>
          </Card>
        </Col>
      </Row>
    </>
  )
}

const JobPathGraphEntry = ({ onOpen }: { onOpen: () => void }) => (
  <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
    <Col xs={24}>
      <Card className="path-card job-path-graph-entry" title={<span><ApartmentOutlined /> 岗位路径知识图谱</span>}>
        <div>
          <h3>想查看全部真实岗位路径关系？</h3>
          <p>
            岗位路径图谱会一次性展示 Neo4j 中全部 PROMOTE_TO / TRANSFER_TO 关系。它是全局岗位路径事实，不依赖当前用户目标岗位，也不会由 LLM 编造。
          </p>
        </div>
        <Button type="primary" icon={<ApartmentOutlined />} onClick={onOpen}>
          查看完整岗位路径图谱
        </Button>
      </Card>
    </Col>
  </Row>
)

const CareerPath = () => {
  const studentInfo = useCareerStore((state) => state.studentInfo)
  const careerPath = useCareerStore((state) => state.careerPath)
  const navigate = useNavigate()

  const handleGoToReport = () => {
    navigate('/report')
  }

  // 如果没有职业路径数据，显示提示
  if (!studentInfo || !careerPath) {
    return (
      <PageShell className="career-path-container">
        <HeroPanel
          eyebrow="Career Planning"
          title="职业规划决策"
          description="职业规划会消费人岗匹配结果、岗位路径事实和学生画像，生成主路径、备选目标和阶段行动建议。"
        />
        <EmptyState
          title="还没有职业规划结果"
          description="请先完成岗位匹配分析，系统会基于匹配结果生成职业规划。"
          action={<Button type="primary" onClick={() => navigate('/matching')}>前往岗位匹配</Button>}
        />
      </PageShell>
    )
  }

  const careerPathData = {
    primaryTarget: careerPath.primary_target_job,
    secondaryTargets: safeList(careerPath.secondary_target_jobs),
    goal: careerPath.goal_positioning,
    directPath: safeList(careerPath.direct_path),
    transitionPath: safeList(careerPath.transition_path),
    longTermPath: safeList(careerPath.long_term_path),
  }

  const shortTermPlan = careerPath.short_term_plan?.map((plan, index) => ({
    title: plan,
    timeline: index === 0 ? '1个月内' : index === 1 ? '2-3个月' : '3-6个月',
    status: index < 2 ? 'current' : 'next',
  })) || []

  const midTermPlan = careerPath.mid_term_plan?.map((plan) => ({
    title: plan,
    timeline: '6-12个月',
    status: 'next',
  })) || []

  const longTermPlan = careerPath.long_term_path?.map((plan, index) => ({
    title: plan,
    timeline: index === 0 ? '1-2 年' : index === 1 ? '3-5 年' : '5+ 年',
    status: 'next',
  })) || []

  const riskGaps = careerPath.risk_and_gap || []
  const fallbackStrategy = careerPath.fallback_strategy || '暂无备选策略'
  const targetReasons = careerPath.target_selection_reason || []
  const pathReasons = careerPath.path_selection_reason || []
  const userTargetJob = careerPath.user_target_job || studentInfo.position || careerPathData.primaryTarget
  const systemRecommendedJob = careerPath.system_recommended_job || careerPath.primary_plan_job || careerPathData.primaryTarget
  const hasTargetPathData = careerPath.target_path_data_status === 'available'
  const decisionReasons = safeList(careerPath.goal_decision_reason).length > 0
    ? safeList(careerPath.goal_decision_reason)
    : targetReasons
  const llmDecision = careerPath.llm_goal_decision_explanation

  return (
    <PageShell className="career-path-container">
      <HeroPanel
        eyebrow="Planning Decision"
        title="职业规划决策"
        description="职业规划不会凭空生成岗位路径，而是基于人岗匹配结果、学生画像和本地岗位图谱，给出主目标、备选目标和阶段行动建议。"
        extra={(
          <Row gutter={[12, 12]}>
            <Col span={12}>
              <MetricCard label="用户原目标" value={userTargetJob || '未明确'} />
            </Col>
            <Col span={12}>
              <MetricCard label="系统主路径" value={careerPathData.primaryTarget || '待生成'} tone="green" />
            </Col>
            <Col span={12}>
              <MetricCard label="推荐岗位" value={systemRecommendedJob || '未明确'} />
            </Col>
            <Col span={12}>
              <MetricCard label="路径数据" value={hasTargetPathData ? '真实路径可用' : '暂无真实路径'} tone={hasTargetPathData ? 'green' : 'orange'} />
            </Col>
          </Row>
        )}
      />

      <Row gutter={[18, 18]}>
        <Col xs={24} lg={8}>
          <InsightCard
            eyebrow="主目标"
            title={careerPathData.primaryTarget || '待确认'}
            description={careerPathData.goal || '暂无目标定位说明'}
            status="info"
            action={<Tag>{careerPath.goal_decision_source || careerPath.path_strategy || '规则决策'}</Tag>}
          />
        </Col>
        <Col xs={24} lg={8}>
          <InsightCard
            eyebrow="用户目标"
            title={userTargetJob || '未明确'}
            description={careerPath.target_job_role ? `当前定位：${careerPath.target_job_role}` : '保留用户原始意向，作为主路径或补强后的冲刺方向。'}
            status="neutral"
          />
        </Col>
        <Col xs={24} lg={8}>
          <InsightCard
            eyebrow="备选方向"
            title={careerPathData.secondaryTargets.length > 0 ? careerPathData.secondaryTargets.join(' / ') : '暂无备选目标'}
            description={careerPath.recommended_job_role ? `推荐岗位定位：${careerPath.recommended_job_role}` : '备选目标用于降低单一路径风险。'}
            status="success"
          />
        </Col>
      </Row>

      <TargetPathSection careerPath={careerPath} />

      <JobPathGraphEntry onOpen={() => navigate('/job-path-graph')} />

      <Row gutter={[18, 18]} style={{ marginTop: 24 }}>
        <Col xs={24} lg={8}>
          <Card className="path-card" title="短期计划">
            <ActionList
              items={shortTermPlan.map((plan) => ({
                title: plan.title,
                meta: plan.timeline,
                status: plan.status === 'current' ? 'current' : 'next',
              }))}
              emptyText="暂无短期行动计划"
            />
          </Card>
        </Col>
        <Col xs={24} lg={8}>
          <Card className="path-card" title="中期计划">
            <ActionList
              items={midTermPlan.map((plan) => ({
                title: plan.title,
                meta: plan.timeline,
                status: 'next',
              }))}
              emptyText="暂无中期行动计划"
            />
          </Card>
        </Col>
        <Col xs={24} lg={8}>
          <Card className="path-card" title="长期计划">
            <ActionList
              items={(longTermPlan.length > 0 ? longTermPlan : [{
                title: '当前目标岗位暂无长期路径数据',
                timeline: '系统不会强行生成不存在的晋升路径。',
                status: 'blocked',
              }]).map((plan) => ({
                title: plan.title,
                meta: plan.timeline,
                status: plan.status === 'blocked' ? 'blocked' : 'next',
              }))}
            />
          </Card>
        </Col>
      </Row>

      {/* 风险与缺口 */}
      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card className="path-card" title={<span><WarningOutlined /> 风险与缺口</span>}>
            <ul style={{ lineHeight: 2.5 }}>
              {riskGaps.map((gap, index) => (
                <li key={index}>{gap}</li>
              ))}
            </ul>
          </Card>
        </Col>
      </Row>

      {/* 备选策略 */}
      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Alert
            message={<span><BulbOutlined /> 备选策略</span>}
            description={fallbackStrategy}
            type="warning"
            showIcon
          />
        </Col>
      </Row>

      {/* 行动建议 */}
      <Row gutter={[24, 24]} style={{ marginTop: 24 }} className="mb-24">
        <Col xs={24}>
          <Card className="path-card" title="决策依据与执行提醒">
            <Collapse
              items={[
                {
                  key: '1',
                  label: <strong>目标选择依据</strong>,
                  children: (
                    decisionReasons.length > 0 ? (
                      <ul>
                        {decisionReasons.map((item, index) => (
                          <li key={`target-reason-${index}`}>{item}</li>
                        ))}
                      </ul>
                    ) : (
                      <p>暂无更详细的目标选择依据。</p>
                    )
                  ),
                },
                {
                  key: '2',
                  label: <strong>路径决策依据</strong>,
                  children: (
                    pathReasons.length > 0 ? (
                      <ul>
                        {pathReasons.map((item, index) => (
                          <li key={`path-reason-${index}`}>{item}</li>
                        ))}
                      </ul>
                    ) : (
                      <p>暂无更详细的路径决策依据。</p>
                    )
                  ),
                },
                {
                  key: '3',
                  label: <strong>执行提醒</strong>,
                  children: (
                    riskGaps.length > 0 ? (
                      <ul>
                        {riskGaps.map((item, index) => (
                          <li key={`risk-gap-${index}`}>{item}</li>
                        ))}
                      </ul>
                    ) : (
                      <p>当前没有额外风险提醒。</p>
                    )
                  ),
                },
              ]}
            />
          </Card>
        </Col>
      </Row>

      <EvidencePanel
        title="规划证据来源"
        description={llmDecision?.decision_reason_summary || '职业规划由人岗匹配结果、学生画像和岗位路径事实共同支持。'}
        sources={[
          { label: '人岗匹配', value: '已消费' },
          { label: '目标路径', value: hasTargetPathData ? '真实路径可用' : '暂无真实路径', status: hasTargetPathData ? 'available' : 'warning' },
          { label: 'LLM', value: '只解释不编造路径' },
        ]}
      />

      <PageToolbar
        title="最后一步：生成完整报告"
        description="基于学生画像、人岗匹配、职业规划和本地岗位资产，生成完整职业规划报告。"
        actions={(
          <Button
            type="primary"
            size="large"
            icon={<ArrowRightOutlined />}
            onClick={handleGoToReport}
          >
            前往报告生成
          </Button>
        )}
      />
    </PageShell>
  )
}

export default CareerPath
