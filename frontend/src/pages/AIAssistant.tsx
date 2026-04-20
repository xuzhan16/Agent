import { useEffect, useRef, useState } from 'react'
import { Alert, Button, Card, Collapse, Empty, Input, Space, Spin, Table, Tag, Typography } from 'antd'
import { MessageOutlined, ReloadOutlined, RobotOutlined, SendOutlined, UserOutlined } from '@ant-design/icons'
import { careerApi } from '../services/api'
import { DataSourceTags, EvidenceNote, HeroPanel } from '../components/ui'
import { AIChatData, AIContextSummaryData, AIResultCard } from '../types'
import '../styles/AIAssistant.css'

const { TextArea } = Input

const scenarioPrompts = [
  {
    title: '查公司机会',
    desc: '按城市、薪资、岗位方向查询本地岗位样本',
    prompt: '北京 20k 以上有哪些前端岗位和公司？',
  },
  {
    title: '看岗位要求',
    desc: '查询学历、专业、证书和知识点要求',
    prompt: '当前推荐岗位的学历、专业、证书和知识点要求是什么？',
  },
  {
    title: '解释匹配风险',
    desc: '解释硬门槛、知识点和七维能力差距',
    prompt: '我的岗位匹配主要风险是什么，下一步应该补什么？',
  },
  {
    title: '查岗位路径',
    desc: '基于本地图谱查看晋升和转岗关系',
    prompt: '岗位路径图谱里有哪些真实晋升关系？',
  },
]

interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  source?: string
  contextSources?: string[]
  data?: AIChatData
}

const AIAssistant = () => {
  const [summaryLoading, setSummaryLoading] = useState(false)
  const [summaryData, setSummaryData] = useState<AIContextSummaryData | null>(null)
  const [summaryError, setSummaryError] = useState<string | null>(null)

  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [conversationId, setConversationId] = useState('')
  const [draft, setDraft] = useState('')
  const [sending, setSending] = useState(false)

  const messageListRef = useRef<HTMLDivElement | null>(null)

  const scrollToBottom = () => {
    if (!messageListRef.current) {
      return
    }
    messageListRef.current.scrollTop = messageListRef.current.scrollHeight
  }

  const loadContextSummary = async () => {
    setSummaryLoading(true)
    setSummaryError(null)
    try {
      const response = await careerApi.getAIContextSummary()
      if (!response.success) {
        throw new Error(response.message || '上下文摘要获取失败')
      }
      setSummaryData(response.data)
    } catch (error) {
      console.error('[AIAssistant] getAIContextSummary error:', error)
      setSummaryError('上下文加载失败，请确认后端服务已启动')
    } finally {
      setSummaryLoading(false)
    }
  }

  useEffect(() => {
    void loadContextSummary()
  }, [])

  useEffect(() => {
    scrollToBottom()
  }, [messages])

  const handleSend = async () => {
    const question = draft.trim()
    if (!question || sending) {
      return
    }

    const userMessage: ChatMessage = {
      id: `u_${Date.now()}`,
      role: 'user',
      content: question,
    }

    setMessages((prev) => [...prev, userMessage])
    setDraft('')
    setSending(true)

    try {
      const response = await careerApi.chatWithAI({
        message: question,
        conversation_id: conversationId,
        web_search_enabled: false,
      })

      if (!response.success) {
        throw new Error(response.message || 'AI 对话失败')
      }

      const chatData = response.data
      setConversationId(chatData.conversation_id || conversationId)
      setSummaryData({
        summary: chatData.context_summary,
        loaded_files: chatData.loaded_files || [],
        missing_files: chatData.missing_files || [],
      })

      setMessages((prev) => [
        ...prev,
        {
          id: `a_${Date.now()}`,
          role: 'assistant',
          content: chatData.answer,
          source: chatData.source,
          contextSources: chatData.used_context_sources || [],
          data: chatData,
        },
      ])
    } catch (error) {
      console.error('[AIAssistant] chat error:', error)
      setMessages((prev) => [
        ...prev,
        {
          id: `a_${Date.now()}`,
          role: 'assistant',
          content: '请求失败，请检查后端服务或稍后重试。',
          source: 'error',
          contextSources: [],
        },
      ])
    } finally {
      setSending(false)
    }
  }

  const handleNewConversation = () => {
    setConversationId('')
    setMessages([])
    setDraft('')
  }

  const handleQuickPrompt = (prompt: string) => {
    setDraft(prompt)
  }

  const renderCompanyCards = (cards?: AIResultCard[]) => {
    const companyCards = (cards || []).filter((card) => card.type === 'company')
    if (companyCards.length === 0) {
      return null
    }

    return (
      <div className="ai-company-card-grid">
        {companyCards.slice(0, 8).map((card, index) => (
          <Card
            key={`${card.company_name || 'company'}_${index}`}
            className="ai-company-result-card"
            size="small"
          >
            <div className="ai-company-card-header">
              <Typography.Text strong>{card.company_name || '未知公司'}</Typography.Text>
              {card.city && <Tag color="blue">{card.city}</Tag>}
            </div>
            <div className="ai-company-card-title">
              {card.standard_job_name || card.job_title || '岗位未记录'}
            </div>
            <div className="ai-company-card-meta">
              <Tag color="green">{card.salary_range || '暂无薪资'}</Tag>
              {card.industry && <Tag>{card.industry}</Tag>}
              {card.company_size && <Tag>{card.company_size}</Tag>}
            </div>
            <Typography.Paragraph className="ai-company-card-reason" ellipsis={{ rows: 2 }}>
              {card.reason || card.match_reason || '与当前查询条件相关。'}
            </Typography.Paragraph>
          </Card>
        ))}
      </div>
    )
  }

  const renderResultTable = (data?: AIChatData) => {
    const table = data?.result_table
    const rows = table?.rows || []
    if (!table || rows.length === 0) {
      return null
    }
    const columnKeys = table.columns && table.columns.length > 0
      ? table.columns
      : Object.keys(rows[0] || {})
    const columns = columnKeys.map((key) => ({
      title: key,
      dataIndex: key,
      key,
      render: (value: unknown) => String(value ?? '暂无'),
    }))
    const dataSource = rows.map((row, index) => ({
      key: `row_${index}`,
      ...row,
    }))
    return (
      <div className="ai-result-table-wrap">
        <Typography.Text strong>{table.title || '查询结果'}</Typography.Text>
        <Table
          size="small"
          columns={columns}
          dataSource={dataSource}
          pagination={false}
          scroll={{ x: true }}
        />
      </div>
    )
  }

  const renderEvidenceCollapse = (data?: AIChatData) => {
    const sql = data?.evidence?.sql || data?.sql_debug
    const pathGraph = data?.evidence?.path_graph
    const semanticHits = data?.evidence?.semantic_hits || []
    if (!sql?.enabled && !pathGraph?.enabled && semanticHits.length === 0) {
      return null
    }

    return (
      <Collapse
        className="ai-evidence-collapse"
        size="small"
        items={[
          {
            key: 'evidence',
            label: '数据来源与查询细节',
            children: (
              <Space direction="vertical" size={8} style={{ width: '100%' }}>
                {sql?.enabled && (
                  <div className="ai-evidence-block">
                    <Typography.Text type="secondary">结构化查询</Typography.Text>
                    <div className="ai-evidence-tags">
                      <Tag color={sql.data_source === 'sqlite_jobs_db' ? 'green' : 'orange'}>
                        {sql.data_source === 'sqlite_jobs_db' ? 'jobs.db' : (sql.data_source || 'unknown')}
                      </Tag>
                      {sql.db_table && <Tag>表：{sql.db_table}</Tag>}
                      <Tag>行数：{sql.row_count ?? 0}</Tag>
                      {sql.sql_source && <Tag>{sql.sql_source}</Tag>}
                    </div>
                    {sql.generated_sql && (
                      <pre className="ai-sql-preview">{sql.generated_sql}</pre>
                    )}
                    {sql.error && <Alert type="warning" message={sql.error} showIcon />}
                  </div>
                )}
                {semanticHits.length > 0 && (
                  <div className="ai-evidence-block">
                    <Typography.Text type="secondary">语义知识召回</Typography.Text>
                    <pre className="ai-sql-preview">
                      {JSON.stringify(semanticHits.slice(0, 3), null, 2)}
                    </pre>
                  </div>
                )}
                {pathGraph?.enabled && (
                  <div className="ai-evidence-block">
                    <Typography.Text type="secondary">岗位路径图谱</Typography.Text>
                    <div className="ai-evidence-tags">
                      <Tag color={pathGraph.source === 'neo4j' ? 'green' : 'orange'}>
                        {pathGraph.source || '本地图谱'}
                      </Tag>
                      <Tag>状态：{pathGraph.path_graph_status || 'unknown'}</Tag>
                      <Tag>晋升：{pathGraph.stats?.promote_edge_count ?? 0}</Tag>
                      <Tag>转岗：{pathGraph.stats?.transfer_edge_count ?? 0}</Tag>
                    </div>
                    {pathGraph.message && <Typography.Text type="secondary">{pathGraph.message}</Typography.Text>}
                  </div>
                )}
              </Space>
            ),
          },
        ]}
      />
    )
  }

  const renderPathGraphResult = (data?: AIChatData) => {
    const pathGraph = data?.evidence?.path_graph
    if (!pathGraph?.enabled) {
      return null
    }
    const edges = pathGraph.matched_edges || []
    return (
      <div className="ai-path-graph-result">
        <div className="ai-path-graph-header">
          <Typography.Text strong>岗位路径图谱结果</Typography.Text>
          <Space size={6} wrap>
            <Tag color={pathGraph.source === 'neo4j' ? 'green' : 'orange'}>
              {pathGraph.source === 'neo4j' ? 'Neo4j' : (pathGraph.source || '本地图谱')}
            </Tag>
            <Tag>晋升 {pathGraph.stats?.promote_edge_count ?? 0}</Tag>
            <Tag>转岗 {pathGraph.stats?.transfer_edge_count ?? 0}</Tag>
          </Space>
        </div>
        {edges.length > 0 ? (
          <div className="ai-path-edge-list">
            {edges.slice(0, 8).map((edge, index) => (
              <div className="ai-path-edge-item" key={`${edge.source_job}_${edge.target_job}_${index}`}>
                <span>{edge.source_job || '未知岗位'}</span>
                <strong>{edge.label || edge.relation || '关系'}</strong>
                <span>{edge.target_job || '未知岗位'}</span>
              </div>
            ))}
          </div>
        ) : (
          <Alert type="info" showIcon message="本地图谱中未查到相关真实路径关系。" />
        )}
      </div>
    )
  }

  const renderLocalSourceTags = (data?: AIChatData) => {
    const sources = data?.local_sources_used && data.local_sources_used.length > 0
      ? data.local_sources_used
      : (data?.used_context_sources || [])
    return <DataSourceTags sources={sources} className="ai-local-source-tags" offline />
  }

  const renderAssistantMessage = (message: ChatMessage) => {
    const data = message.data
    return (
      <>
        <p className="ai-message-text">{message.content}</p>
        {renderCompanyCards(data?.result_cards)}
        {renderResultTable(data)}
        {renderPathGraphResult(data)}
        {renderEvidenceCollapse(data)}
        {renderLocalSourceTags(data)}
        {data?.intent && data.intent !== 'general' && (
          <div className="ai-followup-actions">
            {[
              '继续基于上一次查询结果，按薪资从高到低排序',
              '继续基于上一次查询结果，只看当前系统推荐岗位方向',
              '继续基于上一次查询结果，只看北京',
              '结合当前学生画像，生成投递建议',
              '查看当前推荐岗位的学历、专业、证书和知识点要求',
            ].map((prompt) => (
              <Button size="small" key={prompt} onClick={() => handleQuickPrompt(prompt)}>
                {prompt}
              </Button>
            ))}
          </div>
        )}
      </>
    )
  }

  return (
    <div className="ai-assistant-container">
      <HeroPanel
        className="ai-assistant-hero"
        eyebrow="本地多知识源 Agent"
        title="AI 助手工作台"
        description="基于学生画像、岗位匹配、职业路径与报告数据进行问答；公司、薪资、岗位路径等事实优先来自本地知识源。"
        extra={(
          <div className="ai-hero-source-grid">
            <span>SQLite jobs.db</span>
            <span>Neo4j 路径图谱</span>
            <span>JSON + embedding</span>
            <span>未联网</span>
          </div>
        )}
      />

      <Card className="ai-capability-card">
        <div className="ai-capability-header">
          <RobotOutlined />
          <div>
            <Typography.Title level={4}>当前助手能做什么</Typography.Title>
            <Typography.Text type="secondary">
              当前助手使用本地知识源，不使用联网搜索：SQLite jobs.db 查公司/薪资/城市样本，岗位路径图谱查晋升/转岗关系，语义知识库补充岗位要求，LLM 只负责解释和总结。
            </Typography.Text>
          </div>
        </div>
        <div className="ai-capability-tags">
          <Tag color="blue">查询城市/薪资/公司样本</Tag>
          <Tag color="cyan">查询岗位晋升/转岗路径</Tag>
          <Tag color="cyan">解释学生画像与匹配结果</Tag>
          <Tag color="purple">查询学历/专业/证书要求</Tag>
          <Tag color="green">分析推荐岗位差异</Tag>
          <Tag color="geekblue">总结报告行动建议</Tag>
          <Tag color="orange">联网检索暂未启用</Tag>
        </div>
        <EvidenceNote
          className="ai-capability-note"
          title="回答边界"
          description="公司、薪资、城市与路径事实来自本地知识源；LLM 负责解释和组织答案，不直接编造事实。"
          sources={['jobs.db', 'Neo4j', 'semantic KB', 'state files']}
          compact
        />
      </Card>

      <Card className="ai-scenario-card" title="常用分析场景">
        <div className="ai-scenario-grid">
          {scenarioPrompts.map((item) => (
            <button className="ai-scenario-item" key={item.title} type="button" onClick={() => handleQuickPrompt(item.prompt)}>
              <strong>{item.title}</strong>
              <span>{item.desc}</span>
            </button>
          ))}
        </div>
      </Card>

      <Card className="ai-summary-card" title="上下文状态" extra={<Button icon={<ReloadOutlined />} onClick={loadContextSummary}>刷新</Button>}>
        {summaryLoading ? (
          <Spin />
        ) : summaryError ? (
          <Alert type="error" message={summaryError} showIcon />
        ) : summaryData ? (
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            <Typography.Text>{summaryData.summary}</Typography.Text>
            <div>
              <Typography.Text type="secondary">已加载文件：</Typography.Text>
              <div className="ai-context-tags">
                {(summaryData.loaded_files || []).map((name) => (
                  <Tag color="blue" key={name}>{name}</Tag>
                ))}
              </div>
            </div>
            <div>
              <Typography.Text type="secondary">缺失文件：</Typography.Text>
              <div className="ai-context-tags">
                {(summaryData.missing_files || []).length > 0 ? (
                  (summaryData.missing_files || []).map((name) => (
                    <Tag color="orange" key={name}>{name}</Tag>
                  ))
                ) : (
                  <Tag color="green">无</Tag>
                )}
              </div>
            </div>
          </Space>
        ) : (
          <Alert type="info" message="暂无上下文信息" showIcon />
        )}
      </Card>

      <Card className="ai-chat-card" title={<Space><RobotOutlined />智能问答</Space>}>
        <div className="ai-chat-messages" ref={messageListRef}>
          {messages.length === 0 ? (
            <Empty description="输入你的问题，AI 会基于当前档案回答" image={Empty.PRESENTED_IMAGE_SIMPLE} />
          ) : (
            messages.map((message) => (
              <div key={message.id} className={`ai-message-row ${message.role}`}>
                {message.role === 'assistant' && <RobotOutlined className="ai-message-avatar" />}
                <div className={`ai-message-bubble ${message.role}`}>
                  {message.role === 'assistant' ? renderAssistantMessage(message) : (
                    <p className="ai-message-text">{message.content}</p>
                  )}
                  {message.role === 'assistant' && (
                    <div className="ai-message-meta">
                      <Tag>{message.source || 'unknown'}</Tag>
                      {(message.contextSources || []).map((src) => (
                        <Tag key={`${message.id}_${src}`} color="purple">{src}</Tag>
                      ))}
                    </div>
                  )}
                </div>
                {message.role === 'user' && <UserOutlined className="ai-message-avatar user" />}
              </div>
            ))
          )}
        </div>

        <div className="ai-input-panel">
          <TextArea
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            onPressEnter={(event) => {
              if (event.shiftKey) {
                return
              }
              event.preventDefault()
              void handleSend()
            }}
            autoSize={{ minRows: 3, maxRows: 6 }}
            placeholder="例如：结合我的画像，给我未来3个月的求职执行计划"
            disabled={sending}
          />
          <div className="ai-input-actions">
            <Space size={12} wrap>
              <Button icon={<MessageOutlined />} onClick={handleNewConversation} disabled={sending}>新会话</Button>
              <Tag color="orange">联网检索暂未启用，本页回答基于本地数据</Tag>
            </Space>
            <Button type="primary" icon={<SendOutlined />} loading={sending} onClick={() => void handleSend()}>
              发送
            </Button>
          </div>
          <Typography.Text type="secondary" className="ai-conversation-id">
            当前会话ID：{conversationId || '未创建（发送第一条消息后自动生成）'}
          </Typography.Text>
        </div>
      </Card>
    </div>
  )
}

export default AIAssistant
