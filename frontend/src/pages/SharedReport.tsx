import { Card, Row, Col, Spin, Alert, Button } from 'antd'
import { useSearchParams, useNavigate } from 'react-router-dom'
import { useEffect, useState } from 'react'
import { careerApi } from '../services/api'

const SharedReport = () => {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const [loading, setLoading] = useState(true)
  const [reportContent, setReportContent] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const reportId = searchParams.get('reportId')
    if (!reportId) {
      setError('分享链接无效，请确认链接是否完整。')
      setLoading(false)
      return
    }

    const fetchSharedReport = async () => {
      try {
        const response = await careerApi.getSharedReport(reportId)
        if (response.success) {
          setReportContent(response.data)
        } else {
          setError(response.message || '无法获取共享报告内容。')
        }
      } catch (err: any) {
        console.error('[SharedReport] fetch error:', err)
        const statusCode = err?.response?.status
        const detail = err?.response?.data?.detail
        if (statusCode === 404) {
          setError(detail || '报告已过期或不存在')
        } else {
          setError('共享报告加载失败，请稍后重试。')
        }
      } finally {
        setLoading(false)
      }
    }

    fetchSharedReport()
  }, [searchParams])

  return (
    <div className="report-container">
      <h1 className="page-title">共享职业规划报告</h1>
      <p className="page-description">通过分享链接查看职业规划报告内容</p>

      {loading ? (
        <div style={{ textAlign: 'center', padding: 80 }}>
          <Spin size="large" />
        </div>
      ) : error ? (
        <Alert message="加载失败" description={error} type="error" showIcon closable />
      ) : (
        <Card className="report-card">
          <div style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', fontFamily: 'Microsoft YaHei, Arial, sans-serif', lineHeight: 1.75, color: '#333' }}>
            {reportContent}
          </div>
          <Row justify="center" style={{ marginTop: 24 }}>
            <Col>
              <Button type="primary" onClick={() => navigate('/report')}>
                返回报告页面
              </Button>
            </Col>
          </Row>
        </Card>
      )}
    </div>
  )
}

export default SharedReport
