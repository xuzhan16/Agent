import { Card, Row, Col, Button, Space, Alert, Select, DatePicker, Statistic, Badge, message, Input, Tag } from 'antd'
import { DownloadOutlined, FileTextOutlined, ShareAltOutlined, PrinterOutlined, CheckCircleOutlined } from '@ant-design/icons'
import { useState } from 'react'
import { useCareerStore } from '../store'
import { careerApi } from '../services/api'
import { ReportDetail } from '../types'
import '../styles/Report.css'

const Report = () => {
  const studentInfo = useCareerStore((state) => state.studentInfo)
  const studentProfile = useCareerStore((state) => state.studentProfile)
  const jobMatches = useCareerStore((state) => state.jobMatches)
  const careerPath = useCareerStore((state) => state.careerPath)
  const [generating, setGenerating] = useState(false)
  const [reportFormat, setReportFormat] = useState<'pdf' | 'html' | 'txt'>('txt')
  const [reportGenerated, setReportGenerated] = useState(false)
  const [reportFileName, setReportFileName] = useState<string | null>(null)
  const [reportContent, setReportContent] = useState<string | null>(null)
  const [editableReport, setEditableReport] = useState('')
  const [reportDetail, setReportDetail] = useState<ReportDetail | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [downloading, setDownloading] = useState(false)
  const [savingEdits, setSavingEdits] = useState(false)

  const loadReportDetail = async () => {
    const detailResponse = await careerApi.getReportDetail()
    if (!detailResponse.success) {
      throw new Error(detailResponse.message || '获取报告详情失败')
    }

    const detail = detailResponse.data
    setReportDetail(detail)
    setReportContent(detail.report_text)
    setEditableReport(detail.report_text || '')
    setReportFileName(detail.file_name || 'final_report.md')
    return detail
  }

  const handleGenerateReport = async () => {
    console.log('[Report] handleGenerateReport called')
    console.log('[Report] studentInfo:', studentInfo)
    console.log('[Report] studentProfile:', studentProfile)
    console.log('[Report] jobMatches length:', jobMatches?.length)
    console.log('[Report] careerPath:', careerPath)
    
    if (!studentInfo || !studentProfile || jobMatches.length === 0 || !careerPath) {
      const msg = '请先完成简历解析、学生画像、岗位匹配和职业规划流程，再生成报告。'
      console.warn('[Report]', msg)
      setError(msg)
      return
    }

    setError(null)
    setGenerating(true)

    try {
      const payload = {
        student_info: studentInfo,
        student_profile: studentProfile,
        job_matches: jobMatches,
        career_path: careerPath,
        report_format: reportFormat,
      }
      
      console.log('[Report] Sending generateReport request with payload:', payload)
      const response = await careerApi.generateReport(payload)
      console.log('[Report] generateReport response:', response)

      if (response.success) {
        console.log('[Report] Report generated successfully, filename:', response.data)
        setReportFileName(response.data)
        setReportGenerated(true)
        const detail = await loadReportDetail()
        console.log('[Report] Report detail loaded, length:', detail.report_text?.length)
      } else {
        const msg = response.message || '报告生成失败，请重试'
        console.error('[Report]', msg)
        setError(msg)
      }
    } catch (err) {
      console.error('[Report] generateReport error:', err)
      setError('报告生成接口调用失败，请检查后端服务')
    } finally {
      setGenerating(false)
    }
  }

  const previewReport = async () => {
    const content = editableReport || reportContent || reportDataToText()

    const html = `<!DOCTYPE html><html><head><meta charset="utf-8"/><title>报告预览</title><style>body{font-family: Arial, Helvetica, sans-serif; padding:24px; color:#111;} pre{white-space: pre-wrap; word-break: break-word; font-size:14px; line-height:1.75;}</style></head><body><pre>${content
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')}</pre></body></html>`

    const previewWindow = window.open('', '_blank')
    if (!previewWindow) {
      setError('浏览器拦截了弹窗，请允许弹窗后重试')
      return
    }
    previewWindow.document.open()
    previewWindow.document.write(html)
    previewWindow.document.close()
    previewWindow.focus()
  }

  const reportDataToText = () => {
    return `大学生职业规划分析报告\n\n` +
      `报告生成时间：${reportData.generatedTime}\n` +
      `报告ID：${reportData.reportId}\n` +
      `主要目标：${reportData.primaryTarget}\n` +
      `岗位匹配度：${reportData.matchScore}%\n\n` +
      reportData.sections.map((section, index) => `${index + 1}. ${section}`).join('\n')
  }

  const downloadReport = async () => {
    console.log('[Report] downloadReport called, reportFileName:', reportFileName)
    
    if (!reportFileName) {
      console.warn('[Report] No reportFileName available')
      setError('报告尚未生成或文件名为空')
      return
    }

    setDownloading(true)
    setError(null)

    try {
      console.log('[Report] Starting download with format:', reportFormat)
      
      // 获取报告内容
      let content = editableReport || reportContent
      if (!content) {
        console.log('[Report] Fetching report content from server')
        const detail = await loadReportDetail()
        content = detail.report_text
      }

      console.log('[Report] Content length:', content?.length)

      // 根据选择的格式创建文件
      let filename = reportFileName
      let fileContent: Blob

      if (reportFormat === 'pdf') {
        console.log('[Report] Generating PDF via print dialog')
        const htmlContent = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>职业规划报告 - ${new Date().toLocaleDateString()}</title>
  <style>
    body {
      font-family: 'Microsoft YaHei', 'SimSun', Arial, sans-serif;
      font-size: 12px;
      line-height: 1.6;
      margin: 20px;
      color: #333;
      max-width: 800px;
    }
    h1 {
      color: #0066cc;
      font-size: 18px;
      margin-bottom: 20px;
      text-align: center;
      border-bottom: 2px solid #0066cc;
      padding-bottom: 10px;
    }
    pre {
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 11px;
      background: #f9f9f9;
      padding: 10px;
      border-radius: 4px;
      border: 1px solid #ddd;
    }
    @media print {
      body { margin: 0; font-size: 10px; }
      pre { background: white !important; border: none !important; }
    }
  </style>
</head>
<body>
  <h1>大学生职业规划分析报告</h1>
  <pre>${content.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')}</pre>
  <script>
    window.onload = function() {
      setTimeout(function() {
        window.print();
        setTimeout(function() {
          window.close();
        }, 1000);
      }, 500);
    };
  </script>
</body>
</html>`

        const printWindow = window.open('', '_blank', 'width=800,height=600')
        if (printWindow) {
          printWindow.document.open()
          printWindow.document.write(htmlContent)
          printWindow.document.close()
          printWindow.focus()
          console.log('[Report] Print window opened for PDF generation')
        } else {
          alert('浏览器阻止了弹窗，请允许弹窗后重试，或使用TXT格式下载')
        }

        return
      } else if (reportFormat === 'html') {
        console.log('[Report] Generating HTML')
        const htmlContent = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>职业规划报告</title>
  <style>
    body { font-family: Arial, sans-serif; padding: 20px; line-height: 1.6; color: #333; }
    h1 { color: #0066cc; }
    h2 { color: #0066cc; margin-top: 20px; border-bottom: 2px solid #0066cc; padding-bottom: 5px; }
    pre { white-space: pre-wrap; word-break: break-word; background: #f5f5f5; padding: 10px; }
  </style>
</head>
<body>
  <pre>${content.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')}</pre>
</body>
</html>`
        fileContent = new Blob([htmlContent], { type: 'text/html' })
        filename = reportFileName.replace(/\.[^.]+$/, '.html')
        console.log('[Report] HTML generated, size:', fileContent.size)
      } else {
        console.log('[Report] Generating TXT')
        fileContent = new Blob([content], { type: 'text/plain' })
        filename = reportFileName.replace(/\.[^.]+$/, '.txt')
        console.log('[Report] TXT generated, size:', fileContent.size)
      }

      const link = document.createElement('a')
      const objectUrl = URL.createObjectURL(fileContent)
      link.href = objectUrl
      link.download = filename
      console.log('[Report] Triggering download:', filename)

      document.body.appendChild(link)
      link.click()

      setTimeout(() => {
        document.body.removeChild(link)
        URL.revokeObjectURL(objectUrl)
        console.log('[Report] Download cleanup completed')
      }, 100)

      console.log('[Report] Download triggered successfully')
    } catch (err) {
      console.error('[Report] Download error:', err)
      const errMsg = `下载失败：${err instanceof Error ? err.message : '未知错误'}`
      setError(errMsg)
    } finally {
      setDownloading(false)
    }
  }

  const printReport = async () => {
    if (!reportFileName) return

    try {
      let content = editableReport || reportContent
      if (!content) {
        const detail = await loadReportDetail()
        content = detail.report_text
      }

      const html = `<!DOCTYPE html><html><head><meta charset="utf-8"/><title>报告预览</title><style>body{font-family: Arial, Helvetica, sans-serif; padding:24px; color:#111;} pre{white-space: pre-wrap; word-break: break-word; font-size:14px; line-height:1.75;}</style></head><body><pre>${content
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')}</pre></body></html>`
      const printWindow = window.open('', '_blank')
      if (!printWindow) {
        throw new Error('浏览器拦截了弹窗')
      }
      printWindow.document.open()
      printWindow.document.write(html)
      printWindow.document.close()
      printWindow.focus()
      printWindow.print()
    } catch (err) {
      console.error(err)
      setError('打印预览失败，请检查后端服务或浏览器弹窗设置')
    }
  }

  const saveReportEdits = async () => {
    if (!editableReport.trim()) {
      setError('报告内容不能为空')
      return
    }

    setSavingEdits(true)
    setError(null)

    try {
      const response = await careerApi.updateReport(editableReport)
      if (!response.success) {
        throw new Error(response.message || '保存失败')
      }
      setReportContent(editableReport)
      if (response.data) {
        setReportFileName(response.data)
      }
      await loadReportDetail()
      message.success('报告内容已保存')
    } catch (err) {
      console.error('[Report] saveReportEdits error:', err)
      setError('保存报告内容失败，请稍后重试')
    } finally {
      setSavingEdits(false)
    }
  }

  const shareReport = async () => {
    if (!reportGenerated || !reportFileName) {
      setError('请先生成报告后再分享链接。')
      return
    }

    const shareUrl = `${window.location.origin}/report/view?reportId=${encodeURIComponent(
      reportFileName
    )}`

    try {
      if (navigator.share) {
        await navigator.share({
          title: '职业规划报告',
          text: '点击查看我的职业规划报告',
          url: shareUrl,
        })
      } else if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(shareUrl)
        message.success('分享链接已复制到剪贴板')
      } else {
        const textarea = document.createElement('textarea')
        textarea.value = shareUrl
        document.body.appendChild(textarea)
        textarea.select()
        document.execCommand('copy')
        document.body.removeChild(textarea)
        message.success('分享链接已复制到剪贴板')
      }
    } catch (err) {
      console.error('[Report] shareReport error:', err)
      message.error('分享链接失败，请稍后重试')
    }
  }

  const reportData = {
    generatedTime: new Date().toLocaleString(),
    studentName: studentInfo?.name || '张三',
    reportId: reportFileName || 'RPT-2026-0001',
    primaryTarget: careerPath?.primary_target_job || '未明确目标岗位',
    matchScore: jobMatches.length > 0 ? Math.round(jobMatches.reduce((sum, job) => sum + job.match_score, 0) / jobMatches.length * 100) / 100 : 78.57,
    sections: reportDetail?.report_sections?.length
      ? reportDetail.report_sections.map((section) => section.section_title)
      : [
          '学生基本信息',
          '能力评估',
          '岗位匹配分析',
          '职业规划建议',
          '改进行动计划',
          '长期发展路径'
        ]
  }

  return (
    <div className="report-container">
      <h1 className="page-title">📋 报告生成</h1>
      <p className="page-description">
        根据完整的分析数据生成专业的职业规划分析报告
      </p>

      {!reportGenerated ? (
        <>
          {error && (
            <Row gutter={[24, 24]}>
              <Col xs={24}>
                <Alert
                  message="错误"
                  description={error}
                  type="error"
                  showIcon
                  closable
                  style={{ marginBottom: 24 }}
                />
              </Col>
            </Row>
          )}
          {/* 报告配置 */}
          <Row gutter={[24, 24]}>
            <Col xs={24}>
              <Card className="report-card" title="报告配置">
                <Row gutter={[24, 24]}>
                  <Col xs={24} sm={12}>
                    <div className="config-item">
                      <label>报告格式</label>
                      <Select
                        value={reportFormat}
                        onChange={setReportFormat}
                        options={[
                          { value: 'pdf', label: 'PDF (最推荐)' },
                          { value: 'txt', label: 'TXT (兼容性最好)' },
                          { value: 'html', label: 'HTML (在线查看)' },
                        ]}
                      />
                    </div>
                  </Col>
                  <Col xs={24} sm={12}>
                    <div className="config-item">
                      <label>生成版本</label>
                      <Select
                        value="complete"
                        options={[
                          { value: 'complete', label: '完整版 (推荐)' },
                          { value: 'brief', label: '简版' },
                          { value: 'detailed', label: '详细版' },
                        ]}
                      />
                    </div>
                  </Col>
                  <Col xs={24} sm={12}>
                    <div className="config-item">
                      <label>包含内容</label>
                      <Select
                        mode="multiple"
                        maxTagCount="responsive"
                        value={['basic', 'assessment', 'matching', 'career', 'plan']}
                        options={[
                          { value: 'basic', label: '基本信息' },
                          { value: 'assessment', label: '能力评估' },
                          { value: 'matching', label: '岗位匹配' },
                          { value: 'career', label: '职业规划' },
                          { value: 'plan', label: '行动计划' },
                        ]}
                      />
                    </div>
                  </Col>
                  <Col xs={24} sm={12}>
                    <div className="config-item">
                      <label>报告日期</label>
                      <DatePicker
                        value={null}
                        placeholder="选择日期"
                        style={{ width: '100%' }}
                      />
                    </div>
                  </Col>
                </Row>
              </Card>
            </Col>
          </Row>

          {/* 预览信息 */}
          <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
            <Col xs={24}>
              <Alert
                message="报告预览"
                description={`本报告将为 ${reportData.studentName} 生成一份完整的职业规划分析，包含 ${reportData.sections.length} 个主要部分。`}
                type="info"
                showIcon
              />
            </Col>
          </Row>

          {/* 报告内容概览 */}
          <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
            <Col xs={24}>
              <Card className="report-card" title="报告内容概览">
                <div className="sections-grid">
                  {reportData.sections.map((section, index) => (
                    <div key={index} className="section-item">
                      <div className="section-number">{String(index + 1).padStart(2, '0')}</div>
                      <div className="section-name">{section}</div>
                    </div>
                  ))}
                </div>
              </Card>
            </Col>
          </Row>

          {/* 生成按钮 */}
          <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
            <Col xs={24} style={{ textAlign: 'center' }}>
              <Space size="large">
                <Button
                  type="primary"
                  size="large"
                  icon={<FileTextOutlined />}
                  loading={generating}
                  onClick={handleGenerateReport}
                  className="primary-button"
                >
                  {generating ? '生成中...' : '生成报告'}
                </Button>
                <Button size="large" onClick={previewReport}>预览</Button>
              </Space>
            </Col>
          </Row>
        </>
      ) : (
        <>
          {/* 生成成功提示 */}
          <Row gutter={[24, 24]}>
            <Col xs={24}>
              <Card className="report-card" style={{ textAlign: 'center', padding: '60px 40px' }}>
                <CheckCircleOutlined style={{ fontSize: 64, color: '#52c41a', marginBottom: 24 }} />
                <h2 style={{ margin: '0 0 16px 0', color: '#333' }}>报告生成成功</h2>
                <p style={{ color: '#666', marginBottom: 24 }}>
                  你的职业规划分析报告已生成完毕，可以下载或分享
                </p>
                <Row gutter={[16, 16]} style={{ marginTop: 32 }}>
                  <Col xs={24} sm={8}>
                    <Statistic
                      title="报告ID"
                      value={reportData.reportId}
                      valueStyle={{ fontSize: 14 }}
                    />
                  </Col>
                  <Col xs={24} sm={8}>
                    <Statistic
                      title="生成时间"
                      value={reportData.generatedTime}
                      valueStyle={{ fontSize: 12 }}
                    />
                  </Col>
                  <Col xs={24} sm={8}>
                    <Statistic
                      title="报告格式"
                      value={reportFormat.toUpperCase()}
                      valueStyle={{ fontSize: 14 }}
                    />
                  </Col>
                </Row>
              </Card>
            </Col>
          </Row>

          {/* 操作按钮 */}
          <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
            <Col xs={24}>
              <Card className="report-card" title="报告操作">
                <Row gutter={[16, 16]} align="middle">
                  <Col xs={24} sm={12} lg={4}>
                    <div className="config-item">
                      <label>下载格式</label>
                      <Select
                        value={reportFormat}
                        onChange={setReportFormat}
                        options={[
                          { value: 'txt', label: 'TXT (推荐先测试)' },
                          { value: 'pdf', label: 'PDF (浏览器打印)' },
                          { value: 'html', label: 'HTML (网页格式)' },
                        ]}
                      />
                      {reportFormat === 'pdf' && (
                        <div style={{ fontSize: '12px', color: '#666', marginTop: '4px' }}>
                          💡 将打开新窗口，可用 Ctrl+P 打印为PDF
                        </div>
                      )}
                    </div>
                  </Col>
                  <Col xs={24} sm={12} lg={5}>
                    <Button 
                      type="primary" 
                      block 
                      icon={<DownloadOutlined />}
                      onClick={downloadReport}
                      loading={downloading}
                      disabled={!reportFileName || downloading}
                      className="action-button"
                    >
                      {downloading ? '下载中...' : '下载报告'}
                    </Button>
                  </Col>
                  <Col xs={24} sm={12} lg={5}>
                    <Button 
                      icon={<PrinterOutlined />}
                      block
                      onClick={printReport}
                      className="action-button"
                    >
                      打印预览
                    </Button>
                  </Col>
                  <Col xs={24} sm={12} lg={5}>
                    <Button 
                      icon={<ShareAltOutlined />}
                      block
                      onClick={shareReport}
                      className="action-button"
                    >
                      分享链接
                    </Button>
                  </Col>
                  <Col xs={24} sm={12} lg={5}>
                    <Button 
                      block
                      onClick={() => setReportGenerated(false)}
                      className="action-button"
                    >
                      生成新报告
                    </Button>
                  </Col>
                </Row>
              </Card>
            </Col>
          </Row>

          {/* 报告摘要 */}
          <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
            <Col xs={24}>
              <Card className="report-card" title="报告摘要">
                <Row gutter={[24, 24]}>
                  <Col xs={24} sm={12}>
                    <div className="summary-item">
                      <h4>姓名</h4>
                      <p>{reportData.studentName}</p>
                    </div>
                  </Col>
                  <Col xs={24} sm={12}>
                    <div className="summary-item">
                      <h4>主要目标</h4>
                      <Badge 
                        status="success" 
                        text={reportData.primaryTarget}
                        style={{ fontSize: 14 }}
                      />
                    </div>
                  </Col>
                  <Col xs={24} sm={12}>
                    <div className="summary-item">
                      <h4>岗位匹配度</h4>
                      <p>{reportData.matchScore}%</p>
                    </div>
                  </Col>
                  <Col xs={24} sm={12}>
                    <div className="summary-item">
                      <h4>报告部分数</h4>
                      <p>{reportData.sections.length} 个主要章节</p>
                    </div>
                  </Col>
                </Row>
              </Card>
            </Col>
          </Row>

          <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
            <Col xs={24} lg={12}>
              <Card className="report-card" title="完整性检查">
                <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                  <Tag color={reportDetail?.completeness_check?.is_complete ? 'success' : 'warning'}>
                    {reportDetail?.completeness_check?.is_complete ? '报告结构完整' : '仍有缺失章节'}
                  </Tag>
                  {reportDetail?.report_summary && (
                    <Alert
                      message="报告摘要"
                      description={reportDetail.report_summary}
                      type="info"
                      showIcon
                    />
                  )}
                  {(reportDetail?.completeness_check?.missing_sections || []).length > 0 && (
                    <div>
                      <p style={{ marginBottom: 8, color: '#666' }}>缺失章节</p>
                      <Space wrap>
                        {reportDetail?.completeness_check?.missing_sections?.map((item, index) => (
                          <Tag key={`${item}-${index}`} color="orange">
                            {item}
                          </Tag>
                        ))}
                      </Space>
                    </div>
                  )}
                </Space>
              </Card>
            </Col>
            <Col xs={24} lg={12}>
              <Card className="report-card" title="编辑建议">
                {(reportDetail?.edit_suggestions || []).length > 0 ? (
                  <ul style={{ lineHeight: 2, paddingLeft: 20, margin: 0 }}>
                    {reportDetail?.edit_suggestions?.map((item, index) => (
                      <li key={`edit-suggestion-${index}`}>{item}</li>
                    ))}
                  </ul>
                ) : (
                  <span style={{ color: '#999' }}>当前没有额外编辑建议。</span>
                )}
              </Card>
            </Col>
          </Row>

          <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
            <Col xs={24}>
              <Card
                className="report-card"
                title="报告内容编辑"
                extra={
                  <Button type="primary" onClick={saveReportEdits} loading={savingEdits}>
                    保存修改
                  </Button>
                }
              >
                <Input.TextArea
                  value={editableReport}
                  onChange={(event) => setEditableReport(event.target.value)}
                  autoSize={{ minRows: 14, maxRows: 28 }}
                  placeholder="你可以在这里继续润色或手动调整报告内容。"
                />
              </Card>
            </Col>
          </Row>

          {/* 后续建议 */}
          <Row gutter={[24, 24]} style={{ marginTop: 24 }} className="mb-24">
            <Col xs={24}>
              <Card className="report-card" title="💡 后续建议">
                <ul style={{ lineHeight: 2.2 }}>
                  <li>
                    <strong>定期更新</strong> - 建议每 3 个月更新一次你的技能和项目信息，重新生成报告排查进度
                  </li>
                  <li>
                    <strong>分享给导师</strong> - 可将这份报告分享给学校导师或职业顾问，寻求专业指导
                  </li>
                  <li>
                    <strong>跟进计划</strong> - 严格按照报告中的"改进行动计划"执行，定期检查进度
                  </li>
                  <li>
                    <strong>市场调研</strong> - 定期访问招聘网站，了解目标岗位的最新需求变化
                  </li>
                  <li>
                    <strong>持续学习</strong> - 根据报告建议，系统学习相关技能，积累项目实战经验
                  </li>
                </ul>
              </Card>
            </Col>
          </Row>
        </>
      )}
    </div>
  )
}

export default Report
