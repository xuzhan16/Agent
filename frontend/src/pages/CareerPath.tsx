import { Card, Row, Col, Timeline, Button, Tag, Space, Alert, Collapse, Steps } from 'antd'
import { NodeIndexOutlined, CheckCircleOutlined, RiseOutlined, BulbOutlined, WarningOutlined, ArrowRightOutlined } from '@ant-design/icons'
import { useCareerStore } from '../store'
import { useNavigate } from 'react-router-dom'
import '../styles/CareerPath.css'

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
      <div className="career-path-container">
        <h1 className="page-title">🗺️ 职业规划</h1>
        <p className="page-description">
          基于你的能力和市场需求，制定个性化的职业发展路径
        </p>

        <Card className="path-card" style={{ textAlign: 'center' }}>
          <Alert
            message="还没有职业规划结果"
            description="请先完成岗位匹配分析，系统会自动生成职业规划。"
            type="info"
            showIcon
          />
          <Button
            type="primary"
            style={{ marginTop: 24 }}
            onClick={() => navigate('/matching')}
          >
            前往岗位匹配
          </Button>
        </Card>
      </div>
    )
  }

  const careerPathData = {
    primaryTarget: careerPath.primary_target_job,
    secondaryTargets: careerPath.secondary_target_jobs,
    goal: careerPath.goal_positioning,
    directPath: careerPath.direct_path,
    transitionPath: careerPath.transition_path,
    longTermPath: careerPath.long_term_path,
  }

  const shortTermPlan = careerPath.short_term_plan?.map((plan, index) => ({
    title: plan,
    timeline: index === 0 ? '1个月内' : index === 1 ? '2-3个月' : '3-6个月',
    status: index < 2 ? 'current' : 'pending'
  })) || []

  const midTermPlan = careerPath.mid_term_plan?.map((plan) => ({
    title: plan,
    timeline: '6-12个月',
    status: 'pending'
  })) || []

  const longTermPlan = careerPath.long_term_path?.map((plan, index) => ({
    title: plan,
    timeline: index === 0 ? '1-2 年' : index === 1 ? '3-5 年' : '5+ 年',
    status: 'pending'
  })) || []

  const riskGaps = careerPath.risk_and_gap || []
  const fallbackStrategy = careerPath.fallback_strategy || '暂无备选策略'
  const targetReasons = careerPath.target_selection_reason || []
  const pathReasons = careerPath.path_selection_reason || []

  return (
    <div className="career-path-container">
      <h1 className="page-title">🗺️ 职业规划</h1>
      <p className="page-description">
        基于你的能力和市场需求，制定个性化的职业发展路径
      </p>

      {/* 目标设定 */}
      <Row gutter={[24, 24]}>
        <Col xs={24}>
          <Card className="path-card" title={<span><NodeIndexOutlined /> 职业目标</span>}>
            <Row gutter={[24, 24]}>
              <Col xs={24} sm={12}>
                <div className="target-item">
                  <h3 style={{ margin: '0 0 12px 0', color: '#667eea' }}>主要目标</h3>
                  <Tag color="blue" style={{ fontSize: 16, padding: '4px 16px' }}>
                    {careerPathData.primaryTarget}
                  </Tag>
                  <p style={{ margin: '12px 0 0 0', color: '#666', fontSize: 12 }}>
                    通过补强关键技能后作为重点冲刺方向
                  </p>
                </div>
              </Col>
              <Col xs={24} sm={12}>
                <div className="target-item">
                  <h3 style={{ margin: '0 0 12px 0', color: '#764ba2' }}>备选目标</h3>
                  <Space wrap>
                    {careerPathData.secondaryTargets.map((target, index) => (
                      <Tag key={index} color="magenta">{target}</Tag>
                    ))}
                  </Space>
                  <p style={{ margin: '12px 0 0 0', color: '#666', fontSize: 12 }}>
                    若主要目标受阻，可作为转向方向
                  </p>
                </div>
              </Col>
            </Row>
          </Card>
        </Col>
      </Row>

      {/* 职业路径展示 */}
      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24} lg={12}>
          <Card className="path-card" title="直接路径 (推荐)">
            <Timeline
              items={careerPathData.directPath.map((path, index) => ({
                dot: <CheckCircleOutlined style={{ fontSize: 16, color: '#52c41a' }} />,
                children: (
                  <div className="path-item">
                    <p style={{ margin: 0, fontWeight: 600 }}>{path}</p>
                    {index === 0 && <p style={{ margin: '4px 0 0 0', fontSize: 12, color: '#666' }}>当前目标</p>}
                    {index === careerPathData.directPath.length - 1 && <p style={{ margin: '4px 0 0 0', fontSize: 12, color: '#666' }}>长期目标</p>}
                  </div>
                ),
              }))}
            />
          </Card>
        </Col>

        <Col xs={24} lg={12}>
          <Card className="path-card" title="转向路径 (备选)">
            <Timeline
              items={careerPathData.transitionPath.map((path, index) => ({
                dot: <RiseOutlined style={{ fontSize: 16, color: '#1890ff' }} />,
                children: (
                  <div className="path-item">
                    <p style={{ margin: 0, fontWeight: 600 }}>{path}</p>
                    {index === 0 && <p style={{ margin: '4px 0 0 0', fontSize: 12, color: '#666' }}>快速入职</p>}
                  </div>
                ),
              }))}
            />
          </Card>
        </Col>
      </Row>

      {/* 长期路径 */}
      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card className="path-card" title="长期发展路径 (5+ 年)">
            <div className="long-term-timeline">
              {careerPathData.longTermPath.length > 0 ? (
                careerPathData.longTermPath.map((path, index) => (
                  <div key={`${path}-${index}`} style={{ display: 'contents' }}>
                    <div className="timeline-stage">
                      <h4>{index === 0 ? '应届→1年' : index === 1 ? '1-3年' : index === 2 ? '3-5年' : '5+年'}</h4>
                      <p>{path}</p>
                    </div>
                    {index < careerPathData.longTermPath.length - 1 && <div className="timeline-arrow">→</div>}
                  </div>
                ))
              ) : (
                <p style={{ color: '#999' }}>暂无长期路径数据</p>
              )}
            </div>
          </Card>
        </Col>
      </Row>

      {/* 短期计划 */}
      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card className="path-card" title={<span>📅 短期计划 (3-6个月)</span>}>
            <Steps
              current={0}
              direction="vertical"
              style={{ marginTop: 16 }}
              items={shortTermPlan.map((plan) => ({
                title: plan.title,
                description: <p style={{ margin: 0, fontSize: 12, color: '#666' }}>{plan.timeline}</p>,
                status: plan.status === 'current' ? 'process' : 'wait',
              }))}
            />
          </Card>
        </Col>
      </Row>

      {/* 中期计划 */}
      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card className="path-card" title={<span>📅 中期计划 (6-12个月)</span>}>
            <Steps
              direction="vertical"
              style={{ marginTop: 16 }}
              items={midTermPlan.map((plan) => ({
                title: plan.title,
                description: <p style={{ margin: 0, fontSize: 12, color: '#666' }}>{plan.timeline}</p>,
                status: 'wait',
              }))}
            />
          </Card>
        </Col>
      </Row>

      {/* 长期计划 */}
      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card className="path-card" title={<span>🎯 长期计划 (1-5+ 年)</span>}>
            <Steps
              direction="vertical"
              style={{ marginTop: 16 }}
              items={longTermPlan.map((plan) => ({
                title: plan.title,
                description: <p style={{ margin: 0, fontSize: 12, color: '#666' }}>{plan.timeline}</p>,
                status: 'wait',
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
          <Card className="path-card" title="💡 行动建议">
            <Collapse
              items={[
                {
                  key: '1',
                  label: <strong>🎯 目标选择依据</strong>,
                  children: (
                    targetReasons.length > 0 ? (
                      <ul>
                        {targetReasons.map((item, index) => (
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
                  label: <strong>🧭 路径决策依据</strong>,
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
                  label: <strong>🛠️ 执行提醒</strong>,
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

      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card className="path-card" style={{ textAlign: 'center' }}>
            <Space direction="vertical" size="large">
              <div>
                <h3 style={{ margin: '0 0 8px 0', color: '#333' }}>📄 最后一步：生成完整报告</h3>
                <p style={{ margin: 0, color: '#666' }}>
                  基于以上分析结果，生成包含所有信息的完整职业规划报告
                </p>
              </div>
              <Button
                type="primary"
                size="large"
                icon={<ArrowRightOutlined />}
                onClick={handleGoToReport}
                style={{ height: 48, fontSize: 16 }}
              >
                前往报告生成
              </Button>
            </Space>
          </Card>
        </Col>
      </Row>
    </div>
  )
}

export default CareerPath
