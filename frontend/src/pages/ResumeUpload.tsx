import { Upload, Card, Row, Col, Button, Steps, Alert, Space, Spin, message } from 'antd'
import { UploadOutlined, FileTextOutlined, CheckCircleOutlined, SyncOutlined } from '@ant-design/icons'
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { careerApi } from '../services/api'
import { useCareerStore } from '../store'
import '../styles/ResumeUpload.css'

const { Dragger } = Upload

const ResumeUpload = () => {
  const [file, setFile] = useState<File | null>(null)
  const [uploading, setUploading] = useState(false)
  const [uploadSuccess, setUploadSuccess] = useState(false)
  const [parseResult, setParseResult] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)

  const setStudentInfo = useCareerStore((state) => state.setStudentInfo)
  const setStudentProfile = useCareerStore((state) => state.setStudentProfile)
  const navigate = useNavigate()

  const handleUpload = async (uploadFile: File) => {
    setFile(uploadFile)
    setUploading(true)
    setError(null)

    try {
      const response = await careerApi.parseResume(uploadFile)
      if (response.success) {
        const studentInfo = response.data
        setParseResult(studentInfo)
        setUploadSuccess(true)
        setStudentInfo(studentInfo)

        const profileResponse = await careerApi.buildStudentProfile(studentInfo)
        if (profileResponse.success) {
          setStudentProfile(profileResponse.data)
          message.success('简历解析成功，学生画像生成完毕')
        } else {
          message.warning('简历解析成功，但学生画像生成失败，请稍后重试')
        }
      } else {
        setError(response.message || '简历解析失败，请重试')
        message.error(response.message || '简历解析失败，请重试')
      }
    } catch (err) {
      setError('简历解析接口调用失败，请检查后端服务')
      message.error('简历解析接口调用失败，请检查后端服务')
    } finally {
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

  return (
    <div className="resume-upload-container">
      <h1 className="page-title">📄 简历上传与解析</h1>
      <p className="page-description">
        上传你的简历，我们将使用 AI 技术自动解析并提取关键信息
      </p>

      <Row gutter={[24, 24]}>
        <Col xs={24} lg={12}>
          <Card className="upload-card" title="步骤 1：上传简历">
            <Dragger {...props} className="upload-area">
              <div style={{ padding: '40px 0' }}>
                <p className="ant-upload-drag-icon">
                  <UploadOutlined style={{ fontSize: 48, color: '#3b82f6' }} />
                </p>
                <p className="ant-upload-text" style={{ fontSize: 16, fontWeight: 600 }}>
                  点击或拖拽简历文件到此区域
                </p>
                <p className="ant-upload-hint">
                  支持 PDF、Word (.doc/.docx) 或 TXT 格式 • 文件大小不超过 5MB
                </p>
              </div>
            </Dragger>
            {file && (
              <div style={{ marginTop: 16 }}>
                <Space>
                  <FileTextOutlined style={{ fontSize: 20, color: '#3b82f6' }} />
                  <span>{file.name}</span>
                </Space>
              </div>
            )}
          </Card>
        </Col>

        <Col xs={24} lg={12}>
          <Card className="progress-card" title="处理进度">
            <Steps
              current={uploadSuccess ? 2 : uploading ? 1 : 0}
              items={[
                {
                  title: '上传文件',
                  description: file ? '已选择' : '等待上传',
                  icon: <UploadOutlined />,
                },
                {
                  title: '解析中',
                  description: uploading ? '正在处理' : uploadSuccess ? '已完成' : '待处理',
                  icon: uploading ? <SyncOutlined spin /> : <CheckCircleOutlined />,
                },
                {
                  title: '完成',
                  description: uploadSuccess ? '解析成功' : '待处理',
                  icon: <CheckCircleOutlined />,
                },
              ]}
            />
            {error && (
              <Alert
                style={{ marginTop: 16 }}
                message="错误"
                description={error}
                type="error"
                showIcon
                closable
              />
            )}
          </Card>
        </Col>
      </Row>

      {!uploadSuccess && !uploading && (
        <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
          <Col xs={24}>
            <Alert
              message="提示"
              description="支持的文件格式包括 PDF、Microsoft Word 和纯文本文件。系统将使用 AI 技术自动提取您的个人信息、教育背景、工作经验和技能等。"
              type="info"
              showIcon
              closable
            />
          </Col>
        </Row>
      )}

      {uploading && (
        <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
          <Col xs={24}>
            <Card style={{ textAlign: 'center', padding: '40px' }}>
              <Spin size="large" tip="正在解析简历..." />
            </Card>
          </Col>
        </Row>
      )}

      {uploadSuccess && parseResult && (
        <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
          <Col xs={24}>
            <Card
              title={<span style={{ color: '#16a34a' }}>✓ 简历解析成功</span>}
              extra={<Button type="primary" onClick={() => navigate('/profile')}>查看学生画像</Button>}
            >
              <Row gutter={[24, 24]}>
                <Col xs={24} sm={12}>
                  <div className="result-item">
                    <label>姓名</label>
                    <p>{parseResult.name}</p>
                  </div>
                </Col>
                <Col xs={24} sm={12}>
                  <div className="result-item">
                    <label>邮箱</label>
                    <p>{parseResult.email}</p>
                  </div>
                </Col>
                <Col xs={24} sm={12}>
                  <div className="result-item">
                    <label>电话</label>
                    <p>{parseResult.phone}</p>
                  </div>
                </Col>
                <Col xs={24} sm={12}>
                  <div className="result-item">
                    <label>求职意向</label>
                    <p>{parseResult.position}</p>
                  </div>
                </Col>
                <Col xs={24} sm={12}>
                  <div className="result-item">
                    <label>教育背景</label>
                    <p>{parseResult.education}</p>
                  </div>
                </Col>
                <Col xs={24} sm={12}>
                  <div className="result-item">
                    <label>工作经验</label>
                    <p>{parseResult.experience}</p>
                  </div>
                </Col>
                <Col xs={24}>
                  <div className="result-item">
                    <label>技能</label>
                    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 8 }}>
                      {parseResult.skills?.map((skill: string, index: number) => (
                        <span key={index} className="skill-tag">{skill}</span>
                      ))}
                    </div>
                  </div>
                </Col>
              </Row>
            </Card>
          </Col>
        </Row>
      )}
    </div>
  )
}

export default ResumeUpload