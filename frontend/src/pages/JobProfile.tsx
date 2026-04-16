import { useEffect, useState } from 'react'
import { Alert, Button, Card, Col, Divider, Row, Space, Spin, Tag } from 'antd'
import {
  AppstoreOutlined,
  BookOutlined,
  DatabaseOutlined,
  ReloadOutlined,
  TeamOutlined,
} from '@ant-design/icons'
import { careerApi } from '../services/api'
import type { CoreJobProfile, JobProfileAssetsData, RequirementDistributionItem } from '../types'
import '../styles/JobProfile.css'

const emptyText = '暂无数据'

const toArray = (value?: string | string[]) => {
  if (Array.isArray(value)) return value.filter(Boolean)
  if (!value) return []
  return value.split(/[、,，/|｜]/).map((item) => item.trim()).filter(Boolean)
}

const toPercent = (value?: number) => {
  if (value === undefined || Number.isNaN(value)) return 0
  return Math.round((value <= 1 ? value * 100 : value) * 10) / 10
}

const TagList = ({
  items,
  color = 'blue',
  limit = 12,
  empty = emptyText,
}: {
  items?: string[]
  color?: string
  limit?: number
  empty?: string
}) => {
  const values = (items || []).filter(Boolean).slice(0, limit)
  if (!values.length) return <span className="job-profile-empty">{empty}</span>
  return (
    <Space size={[6, 8]} wrap>
      {values.map((item) => (
        <Tag key={item} color={color} className="job-profile-tag">
          {item}
        </Tag>
      ))}
    </Space>
  )
}

const DistributionBars = ({
  title,
  data,
  maxItems = 5,
}: {
  title: string
  data?: RequirementDistributionItem[]
  maxItems?: number
}) => {
  const items = (data || []).slice(0, maxItems)
  return (
    <div className="profile-distribution">
      <div className="profile-distribution-title">{title}</div>
      {items.length ? (
        items.map((item, index) => {
          const percent = toPercent(item.ratio)
          return (
            <div className="profile-distribution-row" key={`${title}-${item.name || index}`}>
              <span className="profile-distribution-label" title={item.name || emptyText}>
                {item.name || emptyText}
              </span>
              <div className="profile-distribution-track">
                <div className="profile-distribution-fill" style={{ width: `${Math.min(percent, 100)}%` }} />
              </div>
              <span className="profile-distribution-value">
                {percent}%{item.count ? ` · ${item.count}` : ''}
              </span>
            </div>
          )
        })
      ) : (
        <span className="job-profile-empty">暂无分布数据</span>
      )}
    </div>
  )
}

const CoreJobCard = ({
  job,
  selected,
  onSelect,
}: {
  job: CoreJobProfile
  selected: boolean
  onSelect: (job: CoreJobProfile) => void
}) => {
  const jobName = job.standard_job_name || emptyText
  return (
    <button className={`profile-job-card ${selected ? 'selected' : ''}`} onClick={() => onSelect(job)} type="button">
      <div className="profile-job-card-head">
        <span>#{job.display_order || '-'}</span>
        {selected && <Tag color="blue">当前查看</Tag>}
      </div>
      <h3>{jobName}</h3>
      <p>{job.job_category || emptyText}</p>
      <div className="profile-job-card-meta">
        <span>样本 {job.sample_count || 0}</span>
        <span>主流学历 {job.mainstream_degree || emptyText}</span>
      </div>
      <div className="profile-job-card-summary">
        <span>专业：{toArray(job.mainstream_majors_summary).slice(0, 3).join('、') || emptyText}</span>
        <span>证书：{toArray(job.mainstream_cert_summary).slice(0, 2).join('、') || emptyText}</span>
      </div>
      <TagList items={job.top_skills} limit={4} empty="暂无技能标签" />
    </button>
  )
}

const JobDetail = ({ job }: { job?: CoreJobProfile }) => {
  if (!job) {
    return (
      <Card className="job-profile-card">
        <Alert message="请选择一个岗位" description="点击上方核心岗位卡片后查看详情。" type="info" showIcon />
      </Card>
    )
  }

  return (
    <Card className="job-profile-card detail-card" title={<span><BookOutlined /> {job.standard_job_name || '岗位详情'}</span>}>
      <Row gutter={[24, 24]}>
        <Col xs={24} lg={8}>
          <div className="detail-summary-panel">
            <span className="detail-kicker">岗位主流画像</span>
            <h2>{job.standard_job_name || emptyText}</h2>
            <p>{job.selection_reason || '基于岗位样本与后处理资产生成。'}</p>
            <div className="detail-stat-list">
              <span>岗位类别 <b>{job.job_category || emptyText}</b></span>
              <span>岗位层级 <b>{job.job_level_summary || emptyText}</b></span>
              <span>样本数量 <b>{job.sample_count || 0}</b></span>
              <span>学历门槛 <b>{job.degree_gate || job.mainstream_degree || emptyText}</b></span>
            </div>
          </div>
        </Col>
        <Col xs={24} lg={16}>
          <Row gutter={[16, 16]}>
            <Col xs={24} md={8}>
              <DistributionBars title="学历分布" data={job.degree_distribution} />
            </Col>
            <Col xs={24} md={8}>
              <DistributionBars title="专业分布" data={job.major_distribution} />
            </Col>
            <Col xs={24} md={8}>
              <DistributionBars title="证书分布" data={job.certificate_distribution} />
              <div className="profile-no-cert">
                无明确证书要求：{toPercent(job.no_certificate_requirement_ratio)}%
              </div>
            </Col>
          </Row>
          <Divider />
          <Row gutter={[16, 16]}>
            <Col xs={24} md={12}>
              <div className="detail-tag-group">
                <span>主流专业集合</span>
                <TagList items={job.major_gate_set} color="blue" limit={10} />
              </div>
            </Col>
            <Col xs={24} md={12}>
              <div className="detail-tag-group">
                <span>证书要求</span>
                <TagList
                  items={[...(job.must_have_certificates || []), ...(job.preferred_certificates || [])]}
                  color="green"
                  limit={10}
                  empty="多数岗位无明确证书要求"
                />
              </div>
            </Col>
            <Col xs={24} md={12}>
              <div className="detail-tag-group">
                <span>必备知识点</span>
                <TagList items={job.required_knowledge_points} color="orange" limit={14} empty="暂无明确必备知识点" />
              </div>
            </Col>
            <Col xs={24} md={12}>
              <div className="detail-tag-group">
                <span>加分知识点</span>
                <TagList items={job.preferred_knowledge_points} color="purple" limit={14} empty="暂无加分知识点" />
              </div>
            </Col>
            <Col xs={24}>
              <div className="detail-tag-group">
                <span>高频技能与技术栈</span>
                <TagList
                  items={[...(job.top_skills || []), ...(job.hard_skills || []), ...(job.tools_or_tech_stack || [])]}
                  color="geekblue"
                  limit={20}
                  empty="暂无技能标签"
                />
              </div>
            </Col>
          </Row>
        </Col>
      </Row>
    </Card>
  )
}

const JobProfile = () => {
  const [data, setData] = useState<JobProfileAssetsData>({})
  const [selectedJobName, setSelectedJobName] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const coreJobs = data.core_job_profiles || []
  const selectedJob = coreJobs.find((job) => job.standard_job_name === selectedJobName) || coreJobs[0]

  const loadAssets = async () => {
    setLoading(true)
    setError(null)
    try {
      const response = await careerApi.getJobProfileAssets()
      if (response.success) {
        const nextData = response.data || {}
        setData(nextData)
        const firstJob = nextData.core_job_profiles?.[0]?.standard_job_name || ''
        setSelectedJobName((current) => current || firstJob)
      } else {
        setError(response.message || '岗位画像资产读取失败')
      }
    } catch {
      setError('岗位画像接口调用失败，请检查后端服务')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadAssets()
  }, [])

  return (
    <div className="job-profile-container">
      <section className="job-profile-hero">
        <div>
          <span className="job-profile-kicker">岗位画像知识库</span>
          <h1>集中查看 10 个核心岗位的学历、专业、证书与知识点要求</h1>
          <p>岗位画像是全局岗位资产，所有用户看到一致；这里不计算学生风险，只展示岗位本身。</p>
        </div>
        <div className="job-profile-metrics">
          <div><span>核心岗位</span><strong>{data.summary?.core_job_count || coreJobs.length || 0}</strong></div>
          <div><span>标准岗位</span><strong>{data.summary?.standard_job_count || 0}</strong></div>
          <div><span>岗位样本</span><strong>{data.summary?.sample_count || 0}</strong></div>
          <div><span>更新时间</span><strong>{data.summary?.generated_at || emptyText}</strong></div>
        </div>
      </section>

      {error && (
        <Alert message="岗位画像读取异常" description={error} type="warning" showIcon style={{ marginBottom: 24 }} />
      )}

      <Spin spinning={loading}>
        <Card
          className="job-profile-card"
          title={<span><AppstoreOutlined /> 10 个核心岗位</span>}
          extra={<Button icon={<ReloadOutlined />} onClick={loadAssets}>刷新资产</Button>}
        >
          {coreJobs.length ? (
            <div className="profile-job-grid">
              {coreJobs.map((job) => (
                <CoreJobCard
                  key={job.standard_job_name}
                  job={job}
                  selected={job.standard_job_name === selectedJob?.standard_job_name}
                  onSelect={(nextJob) => setSelectedJobName(nextJob.standard_job_name || '')}
                />
              ))}
            </div>
          ) : (
            <Alert
              message="暂无核心岗位画像"
              description="请确认 outputs/match_assets 中已经生成 core_jobs.json 等后处理资产。"
              type="info"
              showIcon
            />
          )}
        </Card>

        <div className="job-profile-section">
          <JobDetail job={selectedJob} />
        </div>

        <Row gutter={[24, 24]} className="job-profile-section">
          <Col xs={24} md={12}>
            <Card className="job-profile-card" title={<span><DatabaseOutlined /> 数据说明</span>}>
              <p className="profile-help-text">
                该页面读取本地后处理资产，展示岗位样本聚合后的主流画像、要求分布和知识点资产，不依赖具体学生。
              </p>
            </Card>
          </Col>
          <Col xs={24} md={12}>
            <Card className="job-profile-card" title={<span><TeamOutlined /> 页面边界</span>}>
              <p className="profile-help-text">
                如果需要查看学历、专业、证书是否适合某个学生，请进入“岗位匹配”页面查看个性化风险分析。
              </p>
            </Card>
          </Col>
        </Row>
      </Spin>
    </div>
  )
}

export default JobProfile
