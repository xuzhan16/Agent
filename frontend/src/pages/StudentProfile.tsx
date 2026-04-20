import { Card, Row, Col, Progress, Avatar, Tag, Space, Timeline, Button } from 'antd'
import { UserOutlined, DatabaseOutlined, CodeOutlined, FileTextOutlined, ArrowRightOutlined } from '@ant-design/icons'
import {
  Radar,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  ResponsiveContainer,
  Tooltip,
} from 'recharts'
import { useCareerStore } from '../store'
import { useNavigate } from 'react-router-dom'
import { careerApi } from '../services/api'
import { useState } from 'react'
import {
  EmptyState,
  EvidencePanel,
  HeroPanel,
  InsightCard,
  MetricCard,
  PageShell,
  PageToolbar,
} from '../components/ui'
import '../styles/StudentProfile.css'

const EMPLOYMENT_ABILITY_DIMENSIONS = ['专业技能', '证书', '创新能力', '学习能力', '抗压能力', '沟通能力', '实习能力']

const clampScore = (value: number): number => Math.max(0, Math.min(100, Math.round(value)))

const parseScore = (value: unknown, fallback = 0): number => {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value
  }

  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!trimmed) {
      return fallback
    }
    const normalized = trimmed.endsWith('%') ? trimmed.slice(0, -1) : trimmed
    const parsed = Number(normalized)
    if (Number.isFinite(parsed)) {
      return parsed
    }
  }

  return fallback
}

const inferAbilityLevel = (score: number): string => {
  if (score >= 85) return '优秀'
  if (score >= 70) return '良好'
  if (score >= 55) return '中等'
  return '待提升'
}

const abilityColorByScore = (score: number): string => {
  if (score >= 85) return '#16a34a'
  if (score >= 70) return '#1677ff'
  if (score >= 55) return '#faad14'
  return '#ff4d4f'
}

const StudentProfile = () => {
  const studentInfo = useCareerStore((state) => state.studentInfo)
  const studentProfile = useCareerStore((state) => state.studentProfile)
  const setJobMatches = useCareerStore((state) => state.setJobMatches)
  const setLoading = useCareerStore((state) => state.setLoading)
  const setError = useCareerStore((state) => state.setError)
  const navigate = useNavigate()
  const [generatingMatches, setGeneratingMatches] = useState(false)

  const handleGenerateJobMatches = async () => {
    if (!studentProfile) {
      setError('学生画像未生成，请先上传简历')
      return
    }

    setGeneratingMatches(true)
    setLoading(true)
    setError(null)

    try {
      const response = await careerApi.matchJobs()
      if (response.success) {
        const matches = Array.isArray(response.data) ? response.data : []
        setJobMatches(matches)
        if (response.status === 'no_data' || matches.length === 0) {
          setError(response.message || '暂无匹配结果，请先完成画像信息录入')
        }
        navigate('/matching')
      } else {
        setError(response.message || '岗位匹配失败，请重试')
      }
    } catch (err) {
      setError('岗位匹配接口调用失败，请检查后端服务')
    } finally {
      setGeneratingMatches(false)
      setLoading(false)
    }
  }

  if (!studentInfo) {
    return (
      <PageShell className="student-profile-container">
        <HeroPanel
          eyebrow="Student Evidence Profile"
          title="学生能力画像"
          description="学生画像会把简历中的教育、技能、项目、实习和证书证据整理成后续岗位匹配可用的能力依据。"
        />
        <EmptyState
          title="还没有学生画像"
          description="请先上传简历或粘贴简历正文，系统会自动解析并生成学生画像。"
          action={<Button type="primary" onClick={() => navigate('/resume')}>前往简历上传</Button>}
        />
      </PageShell>
    )
  }

  const projectExperience = studentInfo.project_experience || []
  const internshipExperience = studentInfo.internship_experience || []
  const profilePayload = studentProfile?.profile_input_payload
  const normalizedEducation = profilePayload?.normalized_education
  const normalizedProfile = profilePayload?.normalized_profile
  const explicitProfile = profilePayload?.explicit_profile
  const practiceProfile = profilePayload?.practice_profile

  const technicalScore = Math.round(studentProfile?.competitiveness_score || 0)
  const completenessScore = Math.round(studentProfile?.complete_score || 0)
  const practiceScore = Math.min(
    100,
    Math.round(
      ((practiceProfile?.project_count || projectExperience.length) * 20)
      + ((practiceProfile?.internship_count || internshipExperience.length) * 30)
      + ((explicitProfile?.certificates?.length || studentInfo.certificates.length) > 0 ? 10 : 0)
    )
  )

  const hardSkills = normalizedProfile?.hard_skills || studentInfo.skills || []
  const toolSkills = normalizedProfile?.tool_skills || []
  const softSkills = studentProfile?.soft_skills || []
  const strengths = studentProfile?.strengths || []
  const weaknesses = studentProfile?.weaknesses || []
  const missingDimensions = studentProfile?.missing_dimensions || []
  const preferredDirections = studentProfile?.potential_profile?.preferred_directions || normalizedProfile?.occupation_hints || []
  const employmentAbilityProfile = studentProfile?.employment_ability_profile || {}
  const schoolLevel = studentInfo.school_level || normalizedEducation?.school_level || ''
  const schoolName = studentInfo.school || normalizedEducation?.school || ''
  const majorName = studentInfo.major || normalizedEducation?.major_std || normalizedEducation?.major_raw || ''
  const degreeName = studentInfo.degree || normalizedEducation?.degree || ''
  const graduationYear = studentInfo.graduation_year || normalizedEducation?.graduation_year || ''

  const projectCount = practiceProfile?.project_count || projectExperience.length
  const internshipCount = practiceProfile?.internship_count || internshipExperience.length
  const certificateCount = explicitProfile?.certificates?.length || studentInfo.certificates.length || 0
  const softSkillText = softSkills.join(' ').toLowerCase()
  const hasLearningSignal = softSkillText.includes('learning') || softSkillText.includes('学习')
  const hasPressureSignal = softSkillText.includes('problem_solving') || softSkillText.includes('抗压')
  const hasCommunicationSignal = softSkillText.includes('communication') || softSkillText.includes('沟通') || softSkillText.includes('协作')

  const fallbackAbilityScores: Record<string, number> = {
    专业技能: technicalScore,
    证书: certificateCount > 0 ? 40 + certificateCount * 20 : 20,
    创新能力: projectCount * 24 + (certificateCount > 0 ? 8 : 0),
    学习能力: technicalScore * 0.65 + hardSkills.length * 4 + (hasLearningSignal ? 12 : 0),
    抗压能力: (projectCount + internshipCount) * 16 + (hasPressureSignal ? 14 : 0),
    沟通能力: internshipCount * 25 + projectCount * 10 + (hasCommunicationSignal ? 15 : 0),
    实习能力: internshipCount * 45,
  }

  const fallbackAbilityEvidence: Record<string, string[]> = {
    专业技能: [`硬技能 ${hardSkills.length} 项`, `工具技能 ${toolSkills.length} 项`],
    证书: [`证书/资格 ${certificateCount} 项`],
    创新能力: [`项目经历 ${projectCount} 段`, `证书/获奖 ${certificateCount} 项`],
    学习能力: [`当前技能覆盖 ${hardSkills.length + toolSkills.length} 项`, hasLearningSignal ? '软技能含学习信号' : '建议补充学习能力证据'],
    抗压能力: [`项目 + 实习共 ${projectCount + internshipCount} 段`, hasPressureSignal ? '软技能含抗压信号' : '建议补充高压场景案例'],
    沟通能力: [`项目经历 ${projectCount} 段`, `实习经历 ${internshipCount} 段`],
    实习能力: [`实习经历 ${internshipCount} 段`],
  }

  const employmentAbilityDimensions = EMPLOYMENT_ABILITY_DIMENSIONS.map((dimension) => {
    const rawItem = employmentAbilityProfile?.[dimension] || {}
    const fallbackScore = fallbackAbilityScores[dimension] || 0
    const score = clampScore(parseScore(rawItem.score, fallbackScore))

    const rawLevel = typeof rawItem.level === 'string' ? rawItem.level.trim() : ''
    const level = rawLevel || inferAbilityLevel(score)

    const rawEvidence = Array.isArray(rawItem.evidence)
      ? rawItem.evidence.filter((item) => typeof item === 'string' && item.trim())
      : []
    const evidence = (rawEvidence.length > 0 ? rawEvidence : (fallbackAbilityEvidence[dimension] || [])).slice(0, 3)

    return {
      dimension,
      score,
      level,
      evidence,
    }
  })

  const employmentAbilityRadarData = employmentAbilityDimensions.map((item) => ({
    dimension: item.dimension,
    score: item.score,
  }))
  const topAbilities = employmentAbilityDimensions
    .filter((item) => item.score >= 70)
    .slice(0, 3)
    .map((item) => item.dimension)
  const riskAbilities = employmentAbilityDimensions
    .filter((item) => item.score < 60)
    .slice(0, 3)
    .map((item) => item.dimension)

  return (
    <PageShell className="student-profile-container">
      <HeroPanel
        eyebrow="Student Evidence Profile"
        title="学生能力画像"
        description="把简历信息拆解为可验证的教育、技能、项目、实习和证书证据，作为后续岗位匹配、风险判断和报告生成的输入。"
        extra={(
          <Row gutter={[12, 12]}>
            <Col span={12}>
              <MetricCard label="画像完整度" value={`${completenessScore}%`} tone="green" />
            </Col>
            <Col span={12}>
              <MetricCard label="竞争力分" value={`${technicalScore}%`} />
            </Col>
            <Col span={12}>
              <MetricCard label="项目 / 实习" value={`${projectCount} / ${internshipCount}`} tone="purple" />
            </Col>
            <Col span={12}>
              <MetricCard label="目标方向" value={studentInfo.position || preferredDirections[0] || '待确认'} />
            </Col>
          </Row>
        )}
      />

      <Row gutter={[24, 24]}>
        <Col xs={24}>
          <Card className="profile-card">
            <Row gutter={[24, 24]}>
              <Col xs={24} sm="auto">
                <Avatar
                  size={80}
                  icon={<UserOutlined />}
                  style={{ background: '#667eea' }}
                />
              </Col>
              <Col xs={24} sm="auto">
                <div style={{ display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
                  <h2 style={{ margin: 0, fontSize: 24, color: '#333' }}>{studentInfo.name}</h2>
                  <p style={{ margin: '4px 0', color: '#666' }}>
                    {degreeName || '学历待确认'} | {majorName || '专业待确认'}
                  </p>
                  <Space size={16}>
                    <span>📧 {studentInfo.email}</span>
                    <span>📱 {studentInfo.phone}</span>
                  </Space>
                </div>
              </Col>
              <Col xs={24} sm={{ flex: 1 }} style={{ textAlign: 'right' }}>
                <div style={{ display: 'flex', flexDirection: 'column', justifyContent: 'center', height: '100%' }}>
                  <Space wrap>
                    <Tag color="blue">学生</Tag>
                    <Tag color="green">求职中</Tag>
                    {schoolLevel && <Tag color="gold">{schoolLevel}</Tag>}
                    <Tag color="magenta">{graduationYear ? `${graduationYear}届` : '毕业年份待确认'}</Tag>
                  </Space>
                </div>
              </Col>
            </Row>
          </Card>
        </Col>
      </Row>

      <Row gutter={[18, 18]} style={{ marginTop: 24 }}>
        <Col xs={24} lg={8}>
          <InsightCard
            eyebrow="证据链"
            title="当前画像证据已归档"
            description="系统会把简历中的教育、技能、项目、实习和证书拆成后续匹配可引用的证据。"
            points={[
              `硬技能 ${hardSkills.length} 项，工具技能 ${toolSkills.length} 项`,
              `项目经历 ${projectCount} 段，实习经历 ${internshipCount} 段`,
              `证书/资格 ${certificateCount} 项`,
            ]}
            status="info"
          />
        </Col>
        <Col xs={24} lg={8}>
          <InsightCard
            eyebrow="能力强项"
            title={topAbilities.length > 0 ? topAbilities.join(' / ') : '暂无明显高分维度'}
            description="这些维度会在岗位匹配和报告中优先作为优势证据呈现。"
            status={topAbilities.length > 0 ? 'success' : 'neutral'}
          />
        </Col>
        <Col xs={24} lg={8}>
          <InsightCard
            eyebrow="待补强"
            title={riskAbilities.length > 0 ? riskAbilities.join(' / ') : '暂无明显短板'}
            description="低分维度不会直接判定失败，但会影响岗位适配解释和行动计划建议。"
            status={riskAbilities.length > 0 ? 'warning' : 'success'}
          />
        </Col>
      </Row>

      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24} sm={12}>
          <Card title={<span><DatabaseOutlined /> 教育背景</span>} className="profile-card">
            <div className="timeline-item">
              <div className="timeline-year">{graduationYear || '待确认'}</div>
              <div className="timeline-content">
                <p style={{ margin: '4px 0', fontWeight: 600 }}>{schoolName || '学校待确认'}</p>
                <p style={{ margin: '4px 0', color: '#666' }}>{majorName || '专业待确认'}</p>
                <Space size={6} wrap>
                  <Tag color="blue">{degreeName || '学历待确认'}</Tag>
                  {schoolLevel && <Tag color="gold">{schoolLevel}</Tag>}
                </Space>
              </div>
            </div>
          </Card>
        </Col>
        <Col xs={24} sm={12}>
          <Card title={<span><FileTextOutlined /> 证书资格</span>} className="profile-card">
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              {studentInfo.certificates.length > 0 ? (
                studentInfo.certificates.map((cert, index) => (
                  <Tag key={index} color="green" style={{ padding: '4px 12px' }}>
                    {cert}
                  </Tag>
                ))
              ) : (
                <span style={{ color: '#999' }}>暂无证书</span>
              )}
            </div>
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card title={<span><CodeOutlined /> 技能概览</span>} className="profile-card">
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              {hardSkills.length > 0 ? (
                hardSkills.map((skill, index) => (
                  <Tag key={index} color="blue" style={{ padding: '6px 14px' }}>
                    {skill}
                  </Tag>
                ))
              ) : (
                <span style={{ color: '#999' }}>暂无技能数据</span>
              )}
            </div>
            {toolSkills.length > 0 && (
              <div style={{ marginTop: 16 }}>
                <p style={{ marginBottom: 8, color: '#666' }}>工具技能</p>
                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                  {toolSkills.map((skill, index) => (
                    <Tag key={`${skill}-${index}`} color="geekblue">
                      {skill}
                    </Tag>
                  ))}
                </div>
              </div>
            )}
            {softSkills.length > 0 && (
              <div style={{ marginTop: 16 }}>
                <p style={{ marginBottom: 8, color: '#666' }}>软技能</p>
                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                  {softSkills.map((skill, index) => (
                    <Tag key={`${skill}-${index}`} color="purple">
                      {skill}
                    </Tag>
                  ))}
                </div>
              </div>
            )}
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24} md={12}>
          <Card title="项目经验" className="profile-card">
            <Timeline
              items={projectExperience.length > 0 ? projectExperience.map((project) => ({
                children: (
                  <div className="timeline-item-detail">
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start' }}>
                      <div>
                        <h4 style={{ margin: '0 0 4px 0' }}>{project.project_name}</h4>
                        <p style={{ margin: '4px 0', color: '#666', fontSize: 12 }}>
                          {project.role}
                        </p>
                      </div>
                      <Tag color="blue">{project.role}</Tag>
                    </div>
                    <p style={{ margin: '8px 0 0 0', color: '#666' }}>{project.description}</p>
                  </div>
                ),
              })) : [{ children: <span style={{ color: '#999' }}>暂无项目经验</span> }]} />
          </Card>
        </Col>

        <Col xs={24} md={12}>
          <Card title="实习经历" className="profile-card">
            <Timeline
              items={internshipExperience.length > 0 ? internshipExperience.map((item) => ({
                children: (
                  <div className="timeline-item-detail">
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start' }}>
                      <div>
                        <h4 style={{ margin: '0 0 4px 0' }}>{item.company_name}</h4>
                        <p style={{ margin: '4px 0', color: '#666', fontSize: 12 }}>
                          {item.position}
                        </p>
                      </div>
                      <Tag color="magenta">{item.position}</Tag>
                    </div>
                    <p style={{ margin: '8px 0 0 0', color: '#666' }}>{item.description}</p>
                  </div>
                ),
              })) : [{ children: <span style={{ color: '#999' }}>暂无实习经历</span> }]} />
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card title="能力评估概览" className="profile-card">
            <Row gutter={[24, 24]}>
              <Col xs={24} sm={8}>
                <div className="assessment-item">
                  <p style={{ margin: '0 0 12px 0', color: '#667eea', fontWeight: 600 }}>技术能力</p>
                  <Progress
                    type="circle"
                    percent={technicalScore}
                    format={(percent) => `${percent}%`}
                    width={100}
                    strokeColor="#667eea"
                  />
                </div>
              </Col>
              <Col xs={24} sm={8}>
                <div className="assessment-item">
                  <p style={{ margin: '0 0 12px 0', color: '#764ba2', fontWeight: 600 }}>学历背景</p>
                  <Progress
                    type="circle"
                    percent={completenessScore}
                    format={(percent) => `${percent}%`}
                    width={100}
                    strokeColor="#764ba2"
                  />
                </div>
              </Col>
              <Col xs={24} sm={8}>
                <div className="assessment-item">
                  <p style={{ margin: '0 0 12px 0', color: '#52c41a', fontWeight: 600 }}>实践经验</p>
                  <Progress
                    type="circle"
                    percent={practiceScore}
                    format={(percent) => `${percent}%`}
                    width={100}
                    strokeColor="#52c41a"
                  />
                </div>
              </Col>
            </Row>
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card title="就业能力画像（核心维度）" className="profile-card ability-dimension-card">
            <div className="ability-radar-wrap">
              <ResponsiveContainer width="100%" height={360}>
                <RadarChart
                  data={employmentAbilityRadarData}
                  margin={{ top: 16, right: 28, bottom: 10, left: 28 }}
                >
                  <PolarGrid stroke="#d8e3f4" />
                  <PolarAngleAxis
                    dataKey="dimension"
                    tick={{ fill: '#22314d', fontSize: 13, fontWeight: 600 }}
                  />
                  <PolarRadiusAxis
                    angle={90}
                    domain={[0, 100]}
                    tickCount={6}
                    tick={{ fill: '#6b7a96', fontSize: 11 }}
                  />
                  <Radar
                    name="核心维度得分"
                    dataKey="score"
                    stroke="#2f6fff"
                    fill="#2f6fff"
                    fillOpacity={0.28}
                    strokeWidth={2}
                  />
                  <Tooltip formatter={(value) => [`${value} 分`, '得分']} />
                </RadarChart>
              </ResponsiveContainer>
            </div>

            <Row gutter={[16, 16]} className="ability-dimension-summary-row">
              {employmentAbilityDimensions.map((item) => (
                <Col xs={24} sm={12} lg={8} key={item.dimension}>
                  <div className="ability-dimension-item ability-dimension-item--compact">
                    <div className="ability-dimension-header ability-dimension-header--compact">
                      <span className="ability-dimension-title">{item.dimension}</span>
                      <Tag color={abilityColorByScore(item.score)}>{item.level}</Tag>
                    </div>
                    <p className="ability-dimension-score">{item.score} 分</p>
                    {item.evidence.length > 0 && (
                      <p className="ability-dimension-evidence">{item.evidence.join('；')}</p>
                    )}
                  </div>
                </Col>
              ))}
            </Row>
          </Card>
        </Col>
      </Row>

      {studentProfile && (
        <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
          <Col xs={24} lg={12}>
            <Card title="画像总结" className="profile-card">
              <p style={{ marginBottom: 12, color: '#333', lineHeight: 1.8 }}>
                {studentProfile.summary || '暂无总结'}
              </p>
              <p style={{ marginBottom: 8, color: '#666' }}>
                竞争力等级：<strong>{studentProfile.score_level || '待评估'}</strong>
              </p>
              {preferredDirections.length > 0 && (
                <div style={{ marginTop: 12 }}>
                  <p style={{ marginBottom: 8, color: '#666' }}>推荐方向</p>
                  <Space wrap>
                    {preferredDirections.map((direction, index) => (
                      <Tag key={`${direction}-${index}`} color="cyan">
                        {direction}
                      </Tag>
                    ))}
                  </Space>
                </div>
              )}
            </Card>
          </Col>

          <Col xs={24} lg={12}>
            <Card title="优势与待补强项" className="profile-card">
              <div style={{ marginBottom: 16 }}>
                <p style={{ marginBottom: 8, color: '#16a34a', fontWeight: 600 }}>优势</p>
                {strengths.length > 0 ? (
                  <ul style={{ paddingLeft: 20, margin: 0, lineHeight: 1.9 }}>
                    {strengths.slice(0, 5).map((item, index) => (
                      <li key={`strength-${index}`}>{item}</li>
                    ))}
                  </ul>
                ) : (
                  <span style={{ color: '#999' }}>暂无优势总结</span>
                )}
              </div>

              <div style={{ marginBottom: 16 }}>
                <p style={{ marginBottom: 8, color: '#f59e0b', fontWeight: 600 }}>待补强项</p>
                {weaknesses.length > 0 ? (
                  <ul style={{ paddingLeft: 20, margin: 0, lineHeight: 1.9 }}>
                    {weaknesses.slice(0, 5).map((item, index) => (
                      <li key={`weakness-${index}`}>{item}</li>
                    ))}
                  </ul>
                ) : (
                  <span style={{ color: '#999' }}>暂无短板总结</span>
                )}
              </div>

              {missingDimensions.length > 0 && (
                <div>
                  <p style={{ marginBottom: 8, color: '#666' }}>缺失维度</p>
                  <Space wrap>
                    {missingDimensions.map((item, index) => (
                      <Tag key={`${item}-${index}`} color="orange">
                        {item}
                      </Tag>
                    ))}
                  </Space>
                </div>
              )}
            </Card>
          </Col>
        </Row>
      )}

      <EvidencePanel
        title="画像证据来源"
        description="学生画像只消费简历解析结果和规则画像，不在前端重新生成能力结论。"
        sources={[
          { label: '简历解析', value: studentInfo.name || '已加载' },
          { label: '学生画像', value: studentProfile ? '已生成' : '待生成', status: studentProfile ? 'available' : 'warning' },
          { label: '能力维度', value: `${employmentAbilityDimensions.length} 项` },
        ]}
      />

      <PageToolbar
        title="下一步：岗位匹配分析"
        description="基于学生画像与岗位后处理资产，系统会给出目标岗位匹配、推荐岗位、风险和知识点覆盖情况。"
        actions={(
          <Button
            type="primary"
            size="large"
            icon={<ArrowRightOutlined />}
            loading={generatingMatches}
            onClick={handleGenerateJobMatches}
          >
            {generatingMatches ? '正在分析岗位匹配...' : '开始岗位匹配分析'}
          </Button>
        )}
      />
    </PageShell>
  )
}

export default StudentProfile
