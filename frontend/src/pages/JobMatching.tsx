import { Card, Row, Col, Progress, Tag, Table, Tooltip, Divider, Alert, Button, Space } from 'antd'
import { BarChartOutlined, CheckCircleOutlined, ExclamationCircleOutlined, ArrowUpOutlined, ArrowRightOutlined } from '@ant-design/icons'
import { useCareerStore } from '../store'
import { useNavigate } from 'react-router-dom'
import { careerApi } from '../services/api'
import { useState } from 'react'
import '../styles/JobMatching.css'

const JobMatching = () => {
  const studentInfo = useCareerStore((state) => state.studentInfo)
  const studentProfile = useCareerStore((state) => state.studentProfile)
  const jobMatches = useCareerStore((state) => state.jobMatches)
  const setCareerPath = useCareerStore((state) => state.setCareerPath)
  const setLoading = useCareerStore((state) => state.setLoading)
  const setError = useCareerStore((state) => state.setError)
  const navigate = useNavigate()
  const [generatingPath, setGeneratingPath] = useState(false)

  // 如果没有岗位匹配数据，显示提示
  if (!studentInfo || jobMatches.length === 0) {
    return (
      <div className="job-matching-container">
        <h1 className="page-title">📊 岗位匹配</h1>
        <p className="page-description">
          基于你的学生画像，系统自动计算与各个岗位的匹配度
        </p>

        <Card className="match-card" style={{ textAlign: 'center' }}>
          <Alert
            message="还没有岗位匹配结果"
            description="请先完成学生画像生成，系统会自动进行岗位匹配分析。"
            type="info"
            showIcon
          />
          <Button
            type="primary"
            style={{ marginTop: 24 }}
            onClick={() => navigate('/profile')}
          >
            前往学生画像
          </Button>
        </Card>
      </div>
    )
  }

  // 计算总体匹配度
  const overallScore = jobMatches.length > 0
    ? Math.round(jobMatches.reduce((sum, job) => sum + job.match_score, 0) / jobMatches.length)
    : 0

  const getMatchLevel = (score: number) => {
    if (score >= 90) return 'A+'
    if (score >= 85) return 'A'
    if (score >= 80) return 'A-'
    if (score >= 75) return 'B+'
    if (score >= 70) return 'B'
    if (score >= 65) return 'B-'
    if (score >= 60) return 'C+'
    if (score >= 55) return 'C'
    return 'C-'
  }

  const matchResult = {
    targetJob: jobMatches[0]?.job_name || '数据分析师',
    overallScore: overallScore,
    matchLevel: getMatchLevel(overallScore),
    matchDescription: overallScore >= 80 ? '优秀匹配' : overallScore >= 70 ? '良好匹配' : overallScore >= 60 ? '中等匹配' : '需要改进',
  }

  const matchDetails = [
    { category: '学历背景', score: 85, required: '本科及以上', status: '符合' },
    { category: '专业方向', score: 80, required: '计算机/统计学', status: '符合' },
    { category: '技能要求', score: 75, required: 'Python/SQL', status: '部分符合' },
    { category: '工作经验', score: 60, required: '2-3年', status: '不足' },
    { category: '项目经验', score: 70, required: '数据分析相关', status: '符合' },
    { category: '工具掌握', score: 65, required: 'Excel/BI工具', status: '部分符合' },
  ]

  const jobRecommendations = jobMatches.map((job, index) => ({
    key: `${index}`,
    jobName: job.job_name,
    company: '暂无公司信息',
    score: Math.round(job.match_score),
    level: job.match_level,
    match: job.reasons?.join('、') || '暂无说明',
    requirement: job.reasons?.slice(0, 3).join('、') || '技能匹配',
    salary: '面议',
  }))

  const handleGenerateCareerPath = async () => {
    if (!studentProfile || jobMatches.length === 0) {
      setError('学生画像或岗位匹配数据不完整')
      return
    }

    setGeneratingPath(true)
    setLoading(true)
    setError(null)

    try {
      const response = await careerApi.planCareerPath(studentProfile, jobMatches)
      if (response.success) {
        setCareerPath(response.data)
        navigate('/career')
      } else {
        setError(response.message || '职业路径规划失败，请重试')
      }
    } catch (err) {
      setError('职业路径规划接口调用失败，请检查后端服务')
    } finally {
      setGeneratingPath(false)
      setLoading(false)
    }
  }

  const getScoreColor = (score: number) => {
    if (score >= 80) return '#52c41a'
    if (score >= 70) return '#faad14'
    if (score >= 60) return '#ff7a45'
    return '#ff4d4f'
  }

  const getMatchLevelColor = (level: string) => {
    if (level.includes('A')) return '#52c41a'
    if (level.includes('B')) return '#1890ff'
    if (level.includes('C')) return '#faad14'
    return '#ff4d4f'
  }

  const columns = [
    {
      title: '岗位名称',
      dataIndex: 'jobName',
      key: 'jobName',
    },
    {
      title: '公司',
      dataIndex: 'company',
      key: 'company',
    },
    {
      title: '匹配度',
      dataIndex: 'score',
      key: 'score',
      render: (score: number) => (
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ color: getScoreColor(score), fontWeight: 600 }}>{score}%</span>
          <Progress
            type="line"
            percent={score}
            strokeColor={getScoreColor(score)}
            size="small"
            style={{ width: 100 }}
          />
        </div>
      ),
    },
    {
      title: '等级',
      dataIndex: 'level',
      key: 'level',
      render: (level: string) => (
        <Tag color={getMatchLevelColor(level)}>{level}</Tag>
      ),
    },
    {
      title: '薪资',
      dataIndex: 'salary',
      key: 'salary',
    },
  ]

  return (
    <div className="job-matching-container">
      <h1 className="page-title">📊 岗位匹配</h1>
      <p className="page-description">
        基于你的学生画像，系统自动计算与各个岗位的匹配度
      </p>

      {!studentInfo && (
        <Alert
          message="未找到学生信息"
          description="请先上传简历并生成学生画像，再查看岗位匹配结果。"
          type="warning"
          showIcon
          style={{ marginBottom: 24 }}
        />
      )}

      <Row gutter={[24, 24]}>
        <Col xs={24} lg={12}>
          <Card className="match-card" title={<span><BarChartOutlined /> 目标岗位分析</span>}>
            <div style={{ textAlign: 'center', padding: '40px 0' }}>
              <h2 style={{ margin: '0 0 16px 0', color: '#333' }}>{matchResult.targetJob}</h2>
              <div style={{ position: 'relative', display: 'inline-block', marginBottom: 24 }}>
                <Progress
                  type="circle"
                  percent={matchResult.overallScore}
                  width={200}
                  strokeColor={{
                    '0%': '#ff4d4f',
                    '50%': '#faad14',
                    '100%': '#52c41a',
                  }}
                  format={(percent) => (
                    <div>
                      <div style={{ fontSize: 24, fontWeight: 'bold', color: '#333' }}>
                        {percent}%
                      </div>
                      <div style={{ fontSize: 12, color: '#666' }}>匹配度</div>
                    </div>
                  )}
                />
              </div>
              <p style={{ margin: '16px 0 0 0', color: '#666' }}>
                <span style={{
                  display: 'inline-block',
                  padding: '4px 12px',
                  background: '#f0f2f5',
                  borderRadius: 20,
                  fontSize: 12,
                }}>
                  {matchResult.matchDescription}
                </span>
              </p>
            </div>
          </Card>
        </Col>

        <Col xs={24} lg={12}>
          <Card className="match-card" title="匹配说明">
            <div style={{ padding: '20px 0' }}>
              <p style={{ margin: '12px 0', display: 'flex', alignItems: 'center', gap: 8 }}>
                <CheckCircleOutlined style={{ color: '#52c41a', fontSize: 16 }} />
                <span>学历背景满足岗位要求 <strong>本科及以上</strong></span>
              </p>
              <p style={{ margin: '12px 0', display: 'flex', alignItems: 'center', gap: 8 }}>
                <CheckCircleOutlined style={{ color: '#52c41a', fontSize: 16 }} />
                <span>专业方向匹配，具备<strong>计算机基础</strong></span>
              </p>
              <Divider />
              <p style={{ margin: '12px 0', display: 'flex', alignItems: 'center', gap: 8 }}>
                <ExclamationCircleOutlined style={{ color: '#faad14', fontSize: 16 }} />
                <span>工作经验缺乏，建议补强实习时长</span>
              </p>
              <p style={{ margin: '12px 0', display: 'flex', alignItems: 'center', gap: 8 }}>
                <ExclamationCircleOutlined style={{ color: '#faad14', fontSize: 16 }} />
                <span>BI工具掌握不足，建议学习相关工具</span>
              </p>
            </div>
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card className="match-card" title="详细匹配评分">
            <Row gutter={[24, 24]}>
              {matchDetails.map((detail, index) => (
                <Col xs={24} sm={12} key={index}>
                  <div className="match-detail-item">
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                      <span style={{ fontWeight: 600 }}>{detail.category}</span>
                      <Tooltip title={detail.required}>
                        <span style={{ color: '#667eea', fontSize: 12 }}>{detail.score}/100</span>
                      </Tooltip>
                    </div>
                    <Progress
                      percent={detail.score}
                      strokeColor={getScoreColor(detail.score)}
                      size="small"
                      format={() => ''}
                    />
                    <div style={{ marginTop: 8, display: 'flex', justifyContent: 'space-between' }}>
                      <span style={{ fontSize: 12, color: '#666' }}>{detail.required}</span>
                      <Tag
                        color={detail.status === '符合' ? 'green' : detail.status === '部分符合' ? 'orange' : 'red'}
                        style={{ fontSize: 10 }}
                      >
                        {detail.status}
                      </Tag>
                    </div>
                  </div>
                </Col>
              ))}
            </Row>
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card className="match-card" title={<span><ArrowUpOutlined /> 推荐岗位</span>}>
            <Table
              columns={columns}
              dataSource={jobRecommendations}
              pagination={false}
              scroll={{ x: 800 }}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} style={{ marginTop: 24 }} className="mb-24">
        <Col xs={24}>
          <Card className="match-card" title="💡 改进建议">
            <ol style={{ lineHeight: 2, paddingLeft: 24 }}>
              <li>
                <strong>补强实习经验</strong> - 利用大二/大三时间争取数据分析岗位实习机会，积累行业实战经验
              </li>
              <li>
                <strong>学习BI工具</strong> - 掌握 Tableau、Power BI 等可视化工具，提升竞争力
              </li>
              <li>
                <strong>完成项目作品</strong> - 完成 1-2 个数据分析完整项目，建立作品集
              </li>
              <li>
                <strong>完善简历与面试材料</strong> - 针对目标岗位进行简历和项目包装
              </li>
            </ol>
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
        <Col xs={24}>
          <Card className="match-card" style={{ textAlign: 'center' }}>
            <Space direction="vertical" size="large">
              <div>
                <h3 style={{ margin: '0 0 8px 0', color: '#333' }}>🚀 下一步：职业路径规划</h3>
                <p style={{ margin: 0, color: '#666' }}>
                  基于岗位匹配结果，为你制定详细的职业发展规划和行动建议
                </p>
              </div>
              <Button
                type="primary"
                size="large"
                icon={<ArrowRightOutlined />}
                loading={generatingPath}
                onClick={handleGenerateCareerPath}
                style={{ height: 48, fontSize: 16 }}
              >
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
