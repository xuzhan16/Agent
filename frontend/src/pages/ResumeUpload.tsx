import { Upload, Row, Col, Button, Alert, Space, Spin, message, Input, Tag } from 'antd'
import {
  ArrowRightOutlined,
  FileTextOutlined,
  UploadOutlined,
  UserOutlined,
} from '@ant-design/icons'
import { useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { careerApi } from '../services/api'
import { useCareerStore } from '../store'
import { PipelineStatus } from '../types'
import {
  EmptyState,
  HeroPanel,
  InsightCard,
  MetricCard,
  PageShell,
  PageToolbar,
  SectionCard,
  WorkflowStepper,
  type WorkflowStepItem,
} from '../components/ui'
import '../styles/ResumeUpload.css'

const { Dragger } = Upload

const ResumeUpload = () => {
  const [file, setFile] = useState<File | null>(null)
  const [manualResumeText, setManualResumeText] = useState('')
  const [uploading, setUploading] = useState(false)
  const [uploadSuccess, setUploadSuccess] = useState(false)
  const [parseResult, setParseResult] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const [pipelineStatus, setPipelineStatus] = useState<PipelineStatus | null>(null)
  const pollTimerRef = useRef<number | null>(null)

  const studentProfile = useCareerStore((state) => state.studentProfile)
  const jobMatches = useCareerStore((state) => state.jobMatches)
  const setStudentInfo = useCareerStore((state) => state.setStudentInfo)
  const setStudentProfile = useCareerStore((state) => state.setStudentProfile)
  const navigate = useNavigate()

  const stopPipelinePolling = () => {
    if (pollTimerRef.current !== null) {
      window.clearInterval(pollTimerRef.current)
      pollTimerRef.current = null
    }
  }

  const startPipelinePolling = () => {
    stopPipelinePolling()
    pollTimerRef.current = window.setInterval(async () => {
      try {
        const statusResponse = await careerApi.getPipelineStatus()
        if (statusResponse.success) {
          const statusData = statusResponse.data
          setPipelineStatus(statusData)
          if (statusData.status === 'completed' || statusData.status === 'failed') {
            stopPipelinePolling()
          }
        }
      } catch {
        // 进度轮询失败时静默重试，避免打断主流程。
      }
    }, 2000)
  }

  useEffect(() => {
    return () => {
      stopPipelinePolling()
    }
  }, [])

  const handlePipelineSuccess = async (studentInfo: any) => {
    setParseResult(studentInfo)
    setUploadSuccess(true)
    setStudentInfo(studentInfo)

    const profileResponse = await careerApi.buildStudentProfile()
    if (profileResponse.success) {
      setStudentProfile(profileResponse.data)
      message.success('简历解析成功，学生画像生成完毕')
    } else {
      message.warning('简历解析成功，但学生画像生成失败，请稍后重试')
    }
  }

  const handleUpload = async (uploadFile: File) => {
    setFile(uploadFile)
    setUploading(true)
    setError(null)
    setPipelineStatus({
      status: 'running',
      current_step: 0,
      total_steps: 6,
      step_name: '准备开始',
      error: null,
    })
    startPipelinePolling()

    try {
      const response = await careerApi.parseResume(uploadFile)
      if (response.success) {
        await handlePipelineSuccess(response.data)
      } else {
        setError(response.message || '简历解析失败，请重试')
        message.error(response.message || '简历解析失败，请重试')
      }
    } catch (err) {
      setError('简历解析接口调用失败，请检查后端服务')
      message.error('简历解析接口调用失败，请检查后端服务')
    } finally {
      stopPipelinePolling()
      setUploading(false)
    }
  }

  const handleManualSubmit = async () => {
    if (!manualResumeText.trim()) {
      message.warning('请先粘贴简历文本内容')
      return
    }

    setUploading(true)
    setError(null)
    setPipelineStatus({
      status: 'running',
      current_step: 0,
      total_steps: 6,
      step_name: '准备开始',
      error: null,
    })
    startPipelinePolling()

    try {
      const response = await careerApi.parseManualResume(manualResumeText)
      if (response.success) {
        await handlePipelineSuccess(response.data)
      } else {
        setError(response.message || '简历解析失败，请重试')
        message.error(response.message || '简历解析失败，请重试')
      }
    } catch (err) {
      setError('手动录入简历接口调用失败，请检查后端服务')
      message.error('手动录入简历接口调用失败，请检查后端服务')
    } finally {
      stopPipelinePolling()
      setUploading(false)
    }
  }

  const props = {
    name: 'resume',
    multiple: false,
    accept: '.pdf,.doc,.docx,.txt',
    beforeUpload: (uploadFile: File) => {
      const extension = uploadFile.name.split('.').pop()?.toLowerCase()
      const isValid = ['pdf', 'doc', 'docx', 'txt'].includes(extension ?? '')

      if (!isValid) {
        message.error('只支持 PDF、Word 或 TXT 格式的文件')
        return false
      }

      const isLt5M = uploadFile.size / 1024 / 1024 < 5
      if (!isLt5M) {
        message.error('文件大小必须小于 5MB')
        return false
      }

      handleUpload(uploadFile)
      return false
    },
  }

  const workflowSteps: WorkflowStepItem[] = [
    {
      title: '提交简历',
      description: file ? file.name : manualResumeText ? '已录入文本' : '上传文件或粘贴文本',
      status: uploadSuccess ? 'finish' : uploading ? 'finish' : 'process',
      tag: file ? '文件' : manualResumeText ? '文本' : '待提交',
    },
    {
      title: '解析画像',
      description: uploading ? pipelineStatus?.step_name || '正在解析' : uploadSuccess ? '学生画像已生成' : '提交后自动执行',
      status: uploadSuccess ? 'finish' : uploading ? 'process' : 'wait',
      tag: uploading ? '运行中' : uploadSuccess ? '完成' : '待处理',
    },
    {
      title: '进入匹配',
      description: jobMatches.length > 0 ? '已有匹配结果' : '画像生成后可继续分析',
      status: jobMatches.length > 0 ? 'finish' : uploadSuccess || studentProfile ? 'process' : 'wait',
      tag: jobMatches.length > 0 ? '完成' : '下一步',
    },
  ]

  const currentStep = jobMatches.length > 0 ? 2 : uploadSuccess || studentProfile ? 2 : uploading ? 1 : 0
  const parsedSkills = Array.isArray(parseResult?.skills) ? parseResult.skills : []

  return (
    <PageShell className="resume-upload-container">
      <HeroPanel
        eyebrow="Resume Intake"
        title="简历上传与画像生成"
        description="从文件或文本开始，系统会抽取教育背景、技能、项目、实习和证书证据，并把结果接入后续岗位画像、人岗匹配和报告生成。"
        extra={(
          <Row gutter={[12, 12]}>
            <Col span={12}>
              <MetricCard label="输入方式" value={file ? '文件上传' : manualResumeText ? '文本录入' : '待选择'} />
            </Col>
            <Col span={12}>
              <MetricCard label="解析状态" value={uploading ? '解析中' : uploadSuccess ? '已完成' : '待开始'} tone={uploadSuccess ? 'green' : 'orange'} />
            </Col>
            <Col span={12}>
              <MetricCard label="学生画像" value={studentProfile ? '已生成' : '待生成'} tone={studentProfile ? 'green' : 'purple'} />
            </Col>
            <Col span={12}>
              <MetricCard label="匹配结果" value={jobMatches.length > 0 ? `${jobMatches.length} 条` : '待分析'} />
            </Col>
          </Row>
        )}
      />

      <WorkflowStepper steps={workflowSteps} current={currentStep} />

      <Row gutter={[18, 18]} className="resume-input-grid">
        <Col xs={24} lg={12}>
          <SectionCard title="上传简历文件">
            <Dragger {...props} className="upload-area">
              <div className="resume-dragger-content">
                <p className="ant-upload-drag-icon">
                  <UploadOutlined />
                </p>
                <p className="ant-upload-text">点击或拖拽简历文件到此区域</p>
                <p className="ant-upload-hint">支持 PDF、Word 或 TXT，文件大小不超过 5MB</p>
              </div>
            </Dragger>
            {file && (
              <div className="resume-file-chip">
                <FileTextOutlined />
                <span>{file.name}</span>
              </div>
            )}
          </SectionCard>
        </Col>

        <Col xs={24} lg={12}>
          <SectionCard title="直接粘贴简历文本">
            <Input.TextArea
              value={manualResumeText}
              onChange={(event) => setManualResumeText(event.target.value)}
              placeholder="如果你暂时没有文件，也可以直接粘贴简历正文。建议包含教育背景、项目经历、实习经历、技能证书和求职意向。"
              autoSize={{ minRows: 10, maxRows: 16 }}
              maxLength={12000}
            />
            <PageToolbar
              className="resume-text-toolbar"
              description={`已输入 ${manualResumeText.length} / 12000 字`}
              actions={(
                <Button type="primary" onClick={handleManualSubmit} loading={uploading}>
                  使用文本生成画像
                </Button>
              )}
            />
          </SectionCard>
        </Col>
      </Row>

      {error && (
        <EmptyState
          status="error"
          title="简历解析未完成"
          description={`${error}。你可以检查后端服务，或改用文本录入方式再次提交。`}
          action={<Button onClick={() => setError(null)}>我知道了</Button>}
        />
      )}

      {uploading && pipelineStatus && (
        <SectionCard title="处理进度">
          <div className="resume-running-panel">
            <Spin size="large" />
            <div>
              <strong>第 {pipelineStatus.current_step || 0}/{pipelineStatus.total_steps || 6} 步：{pipelineStatus.step_name || '处理中'}</strong>
              <p>状态：{pipelineStatus.status || 'running'}{pipelineStatus.error ? `；错误：${pipelineStatus.error}` : ''}</p>
            </div>
          </div>
        </SectionCard>
      )}

      {!uploadSuccess && !uploading && (
        <Alert
          message="流程说明"
          description="简历解析完成后，系统会自动构建学生画像。画像生成后，你可以继续进入岗位画像、人岗匹配、职业规划和报告生成。"
          type="info"
          showIcon
        />
      )}

      {uploadSuccess && parseResult && (
        <SectionCard
          title="解析结果"
          extra={<Button type="primary" icon={<ArrowRightOutlined />} onClick={() => navigate('/profile')}>查看学生画像</Button>}
        >
          <Row gutter={[18, 18]}>
            <Col xs={24} lg={8}>
              <InsightCard
                eyebrow={<UserOutlined />}
                title={parseResult.name || '学生信息已解析'}
                description={`求职意向：${parseResult.position || '暂无明确记录'}`}
                points={[
                  `邮箱：${parseResult.email || '暂无'}`,
                  `电话：${parseResult.phone || '暂无'}`,
                  `教育背景：${parseResult.education || '暂无'}`,
                ]}
                status="success"
              />
            </Col>
            <Col xs={24} lg={16}>
              <div className="resume-result-grid">
                <div>
                  <span>工作经验</span>
                  <strong>{parseResult.experience || '暂无明确记录'}</strong>
                </div>
                <div>
                  <span>技能证据</span>
                  <div className="resume-skill-tags">
                    {parsedSkills.length > 0 ? (
                      parsedSkills.slice(0, 12).map((skill: string, index: number) => (
                        <Tag key={`${skill}-${index}`}>{skill}</Tag>
                      ))
                    ) : (
                      <span className="product-empty-inline">暂无技能数据</span>
                    )}
                  </div>
                </div>
              </div>
              <PageToolbar
                className="resume-next-toolbar"
                title="下一步"
                description="建议先查看学生画像，再进入岗位匹配分析。"
                actions={(
                  <Space wrap>
                    <Button onClick={() => navigate('/profile')}>查看学生画像</Button>
                    <Button type="primary" onClick={() => navigate('/matching')}>进入岗位匹配</Button>
                  </Space>
                )}
              />
            </Col>
          </Row>
        </SectionCard>
      )}
    </PageShell>
  )
}

export default ResumeUpload
