import { Button, Col, Row, Tag } from 'antd'
import {
  ApartmentOutlined,
  BarChartOutlined,
  MessageOutlined,
  NodeIndexOutlined,
  ProfileOutlined,
  UploadOutlined,
  UserOutlined,
} from '@ant-design/icons'
import { Link } from 'react-router-dom'
import { useCareerStore } from '../store'
import {
  EvidencePanel,
  HeroPanel,
  InsightCard,
  MetricCard,
  PageShell,
  SectionCard,
  WorkflowStepper,
  type WorkflowStepItem,
} from '../components/ui'
import '../styles/Home.css'

const Home = () => {
  const studentInfo = useCareerStore((state) => state.studentInfo)
  const studentProfile = useCareerStore((state) => state.studentProfile)
  const jobMatches = useCareerStore((state) => state.jobMatches)
  const careerPath = useCareerStore((state) => state.careerPath)

  const hasResume = Boolean(studentInfo)
  const hasProfile = Boolean(studentProfile)
  const hasMatch = jobMatches.length > 0
  const hasCareerPath = Boolean(careerPath)
  const matchCount = jobMatches.length

  const workflowSteps: WorkflowStepItem[] = [
    {
      title: '简历上传',
      description: hasResume ? '已解析基础信息' : '等待上传或文本录入',
      status: hasResume ? 'finish' : 'process',
      tag: hasResume ? '完成' : '当前',
    },
    {
      title: '学生画像',
      description: hasProfile ? '能力画像已生成' : '解析后自动生成',
      status: hasProfile ? 'finish' : hasResume ? 'process' : 'wait',
      tag: hasProfile ? '完成' : '待处理',
    },
    {
      title: '岗位画像',
      description: '查看核心岗位与目标岗位资产',
      status: hasProfile ? 'finish' : 'wait',
      tag: '本地资产',
    },
    {
      title: '岗位匹配',
      description: hasMatch ? `已生成 ${matchCount} 条匹配结果` : '需要学生画像后执行',
      status: hasMatch ? 'finish' : hasProfile ? 'process' : 'wait',
      tag: hasMatch ? '完成' : '待分析',
    },
    {
      title: '职业规划',
      description: hasCareerPath ? '规划结果已生成' : '基于匹配结果生成',
      status: hasCareerPath ? 'finish' : hasMatch ? 'process' : 'wait',
      tag: hasCareerPath ? '完成' : '待生成',
    },
    {
      title: '报告生成',
      description: '汇总画像、匹配、路径与行动建议',
      status: hasCareerPath ? 'process' : 'wait',
      tag: '最终交付',
    },
  ]

  const workflowCurrent = hasCareerPath ? 5 : hasMatch ? 4 : hasProfile ? 3 : hasResume ? 1 : 0

  const moduleCards = [
    {
      title: '学生画像分析',
      description: '查看教育背景、技能、项目、实习和七维能力证据链。',
      icon: <UserOutlined />,
      to: '/profile',
      status: hasProfile ? '已生成' : '待生成',
    },
    {
      title: '岗位画像中心',
      description: '集中查看 10 个核心岗位、目标岗位要求和能力画像。',
      icon: <ProfileOutlined />,
      to: '/job-profile',
      status: '本地资产',
    },
    {
      title: '人岗匹配决策',
      description: '对比目标岗位和系统推荐岗位，识别硬门槛与知识点风险。',
      icon: <BarChartOutlined />,
      to: '/matching',
      status: hasMatch ? '已分析' : '待分析',
    },
    {
      title: '职业规划',
      description: '基于匹配结果生成主目标、备选目标和阶段行动计划。',
      icon: <NodeIndexOutlined />,
      to: '/career',
      status: hasCareerPath ? '已生成' : '待生成',
    },
    {
      title: '岗位路径图谱',
      description: '查看 Neo4j 中真实 PROMOTE_TO / TRANSFER_TO 岗位路径关系。',
      icon: <ApartmentOutlined />,
      to: '/job-path-graph',
      status: '图谱事实',
    },
    {
      title: 'AI 本地问答助手',
      description: '基于本地 SQL、图谱、语义知识库和状态文件进行追问。',
      icon: <MessageOutlined />,
      to: '/ai-assistant',
      status: '未联网',
    },
  ]

  return (
    <PageShell className="home-workbench">
      <HeroPanel
        eyebrow="Career Planning Workbench"
        title="职业规划智能工作台"
        description="把学生画像、岗位资产、人岗匹配、路径图谱和报告生成放在同一条决策链路里，帮助你快速判断目标岗位风险和可执行的补强方向。"
        extra={(
          <Row gutter={[12, 12]}>
            <Col span={12}>
              <MetricCard label="学生画像" value={hasProfile ? '已生成' : '待生成'} tone={hasProfile ? 'green' : 'orange'} />
            </Col>
            <Col span={12}>
              <MetricCard label="匹配结果" value={hasMatch ? `${matchCount} 条` : '待分析'} />
            </Col>
            <Col span={12}>
              <MetricCard label="职业规划" value={hasCareerPath ? '已完成' : '待生成'} tone={hasCareerPath ? 'green' : 'purple'} />
            </Col>
            <Col span={12}>
              <MetricCard label="本地知识源" value="SQL / Neo4j / RAG" tone="purple" />
            </Col>
          </Row>
        )}
      />

      <div className="home-primary-actions">
        <Link to="/resume">
          <Button type="primary" icon={<UploadOutlined />} size="large">
            开始或更新简历解析
          </Button>
        </Link>
        <Link to={hasMatch ? '/matching' : '/job-profile'}>
          <Button size="large">
            {hasMatch ? '查看匹配决策' : '先看岗位画像'}
          </Button>
        </Link>
      </div>

      <WorkflowStepper steps={workflowSteps} current={workflowCurrent} className="home-workflow" />

      <Row gutter={[18, 18]} className="home-module-grid">
        {moduleCards.map((item) => (
          <Col xs={24} md={12} xl={8} key={item.title}>
            <Link to={item.to} className="home-module-link">
              <InsightCard
                eyebrow={<span className="home-module-icon">{item.icon}</span>}
                title={item.title}
                description={item.description}
                status="info"
                action={<Tag>{item.status}</Tag>}
              />
            </Link>
          </Col>
        ))}
      </Row>

      <Row gutter={[18, 18]} className="home-bottom-grid">
        <Col xs={24} lg={14}>
          <SectionCard title="当前链路状态">
            <div className="home-status-board">
              <div>
                <span>简历解析</span>
                <strong>{hasResume ? studentInfo?.name || '已完成' : '未开始'}</strong>
              </div>
              <div>
                <span>目标岗位</span>
                <strong>{studentInfo?.position || careerPath?.primary_target_job || '待确认'}</strong>
              </div>
              <div>
                <span>系统主路径</span>
                <strong>{careerPath?.primary_target_job || '待生成'}</strong>
              </div>
              <div>
                <span>报告准备度</span>
                <strong>{hasResume && hasProfile && hasMatch && hasCareerPath ? '可生成' : '待补齐流程'}</strong>
              </div>
            </div>
          </SectionCard>
        </Col>
        <Col xs={24} lg={10}>
          <EvidencePanel
            title="本地知识底座"
            description="系统默认使用本地可信知识源，不把事实判断交给大模型凭空生成。"
            sources={[
              { label: 'SQLite', value: '岗位事实与市场细节' },
              { label: 'Neo4j', value: '岗位结构与路径关系' },
              { label: 'JSON + embedding', value: '语义召回与相似要求' },
              { label: 'LLM', value: '解释、归纳、报告生成' },
            ]}
            defaultOpen
          />
        </Col>
      </Row>
    </PageShell>
  )
}

export default Home
