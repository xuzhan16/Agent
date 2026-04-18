import { Alert, Button, Card, Col, Progress, Row, Space, Tag } from 'antd'
import { CheckCircleOutlined, QuestionCircleOutlined } from '@ant-design/icons'
import type { TargetJobCandidate } from '../types'
import '../styles/TargetJobConfirmation.css'

const toPercent = (value?: number) => {
  if (value === undefined || Number.isNaN(value)) return 0
  return Math.round((value <= 1 ? value * 100 : value) * 10) / 10
}

const compactList = (items?: string[], limit = 5) => (items || []).filter(Boolean).slice(0, limit)

interface TargetJobConfirmationProps {
  requestedJobName: string
  candidates: TargetJobCandidate[]
  loading?: boolean
  onConfirm: (candidate: TargetJobCandidate) => void
}

const TargetJobConfirmation = ({
  requestedJobName,
  candidates,
  loading = false,
  onConfirm,
}: TargetJobConfirmationProps) => {
  if (!candidates.length) {
    return (
      <Card className="target-confirm-card">
        <Alert
          type="warning"
          showIcon
          message="当前目标岗位暂未命中本地标准岗位"
          description="系统没有找到足够可靠的候选岗位。你可以换一个更具体的岗位名称后重新上传或重新分析，例如 Java开发工程师、前端开发工程师、软件测试工程师。"
        />
      </Card>
    )
  }

  return (
    <Card className="target-confirm-card" title={<span><QuestionCircleOutlined /> 请确认本次评估采用的本地标准岗位</span>}>
      <Alert
        className="target-confirm-alert"
        type="info"
        showIcon
        message={`“${requestedJobName || '当前目标岗位'}”未唯一命中本地岗位资产`}
        description="为了避免把宽泛岗位强行映射到错误画像，系统先给出候选标准岗位。请选择最接近你真实意向的岗位，后续学历、专业、证书分布和知识点覆盖率都会基于该岗位重新计算。"
      />

      <Row gutter={[16, 16]}>
        {candidates.map((candidate) => {
          const score = toPercent(candidate.candidate_score)
          const name = candidate.standard_job_name || '未命名岗位'
          return (
            <Col xs={24} lg={12} xl={8} key={name}>
              <div className="target-candidate-card">
                <div className="target-candidate-head">
                  <div>
                    <h3>{name}</h3>
                    <p>{candidate.job_category || '暂无岗位类别'} · 样本 {candidate.sample_count || 0}</p>
                  </div>
                  {candidate.is_core_job && <Tag color="blue">核心岗位</Tag>}
                </div>

                <div className="target-candidate-score">
                  <span>候选相关度</span>
                  <Progress percent={score} size="small" strokeColor="#2563eb" />
                </div>

                <div className="target-candidate-meta">
                  <span>主流学历 <b>{candidate.mainstream_degree || '暂无'}</b></span>
                  <span>专业方向 <b>{compactList(candidate.mainstream_majors, 2).join('、') || '暂无'}</b></span>
                  <span>证书要求 <b>{compactList(candidate.mainstream_certificates, 2).join('、') || '暂无强制证书'}</b></span>
                </div>

                <div className="target-candidate-tags">
                  <span>关键技能</span>
                  <Space size={[4, 6]} wrap>
                    {compactList(candidate.top_skills, 5).map((item) => (
                      <Tag key={item} color="geekblue">{item}</Tag>
                    ))}
                    {!compactList(candidate.top_skills, 5).length && <em>暂无技能标签</em>}
                  </Space>
                </div>

                <p className="target-candidate-reason">{candidate.match_reason || '该岗位是本地资产中的可能相关标准岗位。'}</p>

                <Button
                  type="primary"
                  icon={<CheckCircleOutlined />}
                  loading={loading}
                  block
                  onClick={() => onConfirm(candidate)}
                >
                  使用该岗位评估
                </Button>
              </div>
            </Col>
          )
        })}
      </Row>
    </Card>
  )
}

export default TargetJobConfirmation
