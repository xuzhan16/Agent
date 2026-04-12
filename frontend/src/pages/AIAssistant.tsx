import { useEffect, useRef, useState } from 'react'
import { Alert, Button, Card, Empty, Input, Space, Spin, Switch, Tag, Typography } from 'antd'
import { MessageOutlined, ReloadOutlined, RobotOutlined, SendOutlined, UserOutlined } from '@ant-design/icons'
import { careerApi } from '../services/api'
import { AIContextSummaryData } from '../types'
import '../styles/AIAssistant.css'

const { TextArea } = Input

interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  source?: string
  contextSources?: string[]
}

const AIAssistant = () => {
  const [summaryLoading, setSummaryLoading] = useState(false)
  const [summaryData, setSummaryData] = useState<AIContextSummaryData | null>(null)
  const [summaryError, setSummaryError] = useState<string | null>(null)

  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [conversationId, setConversationId] = useState('')
  const [draft, setDraft] = useState('')
  const [sending, setSending] = useState(false)
  const [webSearchEnabled, setWebSearchEnabled] = useState(false)

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
        web_search_enabled: webSearchEnabled,
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

  return (
    <div className="ai-assistant-container">
      <h1 className="page-title">AI 助手</h1>
      <p className="page-description">基于学生画像、岗位匹配、职业路径与报告数据进行智能问答。</p>

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
                  <p className="ai-message-text">{message.content}</p>
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
              <Space>
                <Typography.Text type="secondary">联网检索</Typography.Text>
                <Switch checked={webSearchEnabled} onChange={setWebSearchEnabled} />
              </Space>
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
