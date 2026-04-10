import { Card, Row, Col, Progress, Avatar, Tag, Space, Timeline, Alert, Button } from 'antd'
import { UserOutlined, DatabaseOutlined, CodeOutlined, FileTextOutlined, ArrowRightOutlined } from '@ant-design/icons'
import { useCareerStore } from '../store'
import { useNavigate } from 'react-router-dom'
import { careerApi } from '../services/api'
import { useState } from 'react'
import '../styles/StudentProfile.css'

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
      const response = await careerApi.matchJobs(studentProfile)
      if (response.success) {
        setJobMatches(response.data)
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
              {studentInfo.skills.length > 0 ? (
                studentInfo.skills.map((skill, index) => (
                  <Tag key={index} color="blue" style={{ padding: '6px 14px' }}>
                    {skill}
                  </Tag>
                ))
              ) : (
                <span style={{ color: '#999' }}>暂无技能数据</span>
              )}
            </div>
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
                    percent={75}
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
                    percent={85}
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
                    percent={60}
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
