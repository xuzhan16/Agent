import { Card, Row, Col, Progress, Avatar, Tag, Space, Timeline, Alert, Button } from 'antd'
import { UserOutlined, DatabaseOutlined, CodeOutlined, FileTextOutlined, ArrowRightOutlined } from '@ant-design/icons'
import { useCareerStore } from '../store'
import { useNavigate } from 'react-router-dom'
import { careerApi } from '../services/api'
import { useState } from 'react'
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
      <div className="student-profile-container">
        <h1 className="page-title">👤 学生画像</h1>
        <p className="page-description">请先上传简历，生成学生画像后即可查看此页面。</p>

        <Card className="profile-card" style={{ textAlign: 'center' }}>
          <Alert
            message="还没有学生画像"
            description="请前往“简历上传”页面提交简历，系统会自动解析并生成画像。"
            type="info"
            showIcon
          />
          <Button type="primary" style={{ marginTop: 24 }} onClick={() => navigate('/resume')}>
            前往简历上传
          </Button>
        </Card>
      </div>
    )
  }

  const projectExperience = studentInfo.project_experience || []
  const internshipExperience = studentInfo.internship_experience || []
  const profilePayload = studentProfile?.profile_input_payload
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

  return (
    <div className="student-profile-container">
      <h1 className="page-title">👤 学生画像</h1>
      <p className="page-description">
        基于简历信息自动生成的个性化职业画像
      </p>

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
                    {studentInfo.degree} | {studentInfo.major}
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
                    <Tag color="magenta">{studentInfo.graduation_year}届</Tag>
                  </Space>
                </div>
              </Col>
            </Row>
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24} sm={12}>
          <Card title={<span><DatabaseOutlined /> 教育背景</span>} className="profile-card">
            <div className="timeline-item">
              <div className="timeline-year">{studentInfo.graduation_year}</div>
              <div className="timeline-content">
                <p style={{ margin: '4px 0', fontWeight: 600 }}>{studentInfo.school}</p>
                <p style={{ margin: '4px 0', color: '#666' }}>{studentInfo.major}</p>
                <p style={{ margin: '4px 0', color: '#999', fontSize: 12 }}>{studentInfo.degree} 学位</p>
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
            <Row gutter={[16, 16]}>
              {employmentAbilityDimensions.map((item) => (
                <Col xs={24} sm={12} lg={8} key={item.dimension}>
                  <div className="ability-dimension-item">
                    <div className="ability-dimension-header">
                      <span className="ability-dimension-title">{item.dimension}</span>
                      <Tag color={abilityColorByScore(item.score)}>{item.level}</Tag>
                    </div>
                    <Progress
                      percent={item.score}
                      size="small"
                      strokeColor={abilityColorByScore(item.score)}
                    />
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

      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card className="profile-card" style={{ textAlign: 'center' }}>
            <Space direction="vertical" size="large">
              <div>
                <h3 style={{ margin: '0 0 8px 0', color: '#333' }}>🎯 下一步：岗位匹配分析</h3>
                <p style={{ margin: 0, color: '#666' }}>
                  基于你的学生画像，系统将为你推荐最适合的岗位并分析匹配度
                </p>
              </div>
              <Button
                type="primary"
                size="large"
                icon={<ArrowRightOutlined />}
                loading={generatingMatches}
                onClick={handleGenerateJobMatches}
                style={{ height: 48, fontSize: 16 }}
              >
                {generatingMatches ? '正在分析岗位匹配...' : '开始岗位匹配分析'}
              </Button>
            </Space>
          </Card>
        </Col>
      </Row>
    </div>
  )
}

export default StudentProfile
