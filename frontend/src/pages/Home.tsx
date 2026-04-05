import { Card, Row, Col, Statistic, Button, Timeline, Space } from 'antd'
import { UserOutlined, BarChartOutlined, NodeIndexOutlined, FileTextOutlined, RocketOutlined, TeamOutlined, CheckCircleOutlined } from '@ant-design/icons'
import { Link } from 'react-router-dom'
import '../styles/Home.css'

const Home = () => {
  return (
    <div className="home-container">
      <div className="hero-section">
        <h1 className="page-title">欢迎使用 AI 职业规划系统</h1>
        <p className="page-description">
          基于人工智能的职业规划平台，为您提供智能的岗位匹配、职业路径规划和个性化建议
        </p>
        <Space size="large">
          <Link to="/resume">
            <Button type="primary" size="large" className="primary-button">
              开始规划 →
            </Button>
          </Link>
          <Button size="large">了解更多</Button>
        </Space>
      </div>

      <Row gutter={[24, 24]} style={{ marginTop: 48 }}>
        <Col xs={24} sm={12} lg={6}>
          <Card className="stat-card" hoverable>
            <Statistic
              title="已处理简历"
              value={1}
              prefix={<UserOutlined />}
              suffix="份"
              valueStyle={{ color: '#667eea' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card className="stat-card" hoverable>
            <Statistic
              title="岗位匹配"
              value={0}
              prefix={<BarChartOutlined />}
              suffix="次"
              valueStyle={{ color: '#764ba2' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card className="stat-card" hoverable>
            <Statistic
              title="职业规划"
              value={0}
              prefix={<NodeIndexOutlined />}
              suffix="个"
              valueStyle={{ color: '#f093fb' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card className="stat-card" hoverable>
            <Statistic
              title="生成报告"
              value={0}
              prefix={<FileTextOutlined />}
              suffix="份"
              valueStyle={{ color: '#4facfe' }}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} style={{ marginTop: 48 }}>
        <Col xs={24} lg={12}>
          <Card 
            title={<h3 style={{ margin: 0, color: '#667eea' }}>✨ 核心功能</h3>}
            className="feature-card"
          >
            <ul style={{ lineHeight: 2.5 }}>
              <li>
                <CheckCircleOutlined style={{ color: '#52c41a', marginRight: 8 }} />
                <strong>简历智能解析</strong> - 支持多种格式的简历文件上传和 AI 解析
              </li>
              <li>
                <CheckCircleOutlined style={{ color: '#52c41a', marginRight: 8 }} />
                <strong>学生画像构建</strong> - 基于简历信息自动生成职业画像
              </li>
              <li>
                <CheckCircleOutlined style={{ color: '#52c41a', marginRight: 8 }} />
                <strong>岗位智能匹配</strong> - 精准计算与目标岗位的匹配度
              </li>
              <li>
                <CheckCircleOutlined style={{ color: '#52c41a', marginRight: 8 }} />
                <strong>职业路径规划</strong> - 生成个性化的职业发展路径
              </li>
              <li>
                <CheckCircleOutlined style={{ color: '#52c41a', marginRight: 8 }} />
                <strong>报告自动生成</strong> - 导出专业的职业规划分析报告
              </li>
            </ul>
          </Card>
        </Col>

        <Col xs={24} lg={12}>
          <Card 
            title={<h3 style={{ margin: 0, color: '#764ba2' }}>🚀 使用流程</h3>}
            className="feature-card"
          >
            <Timeline
              items={[
                {
                  dot: <RocketOutlined style={{ fontSize: 16 }} />,
                  children: (
                    <div>
                      <Link to="/resume">
                        <strong>第一步：上传简历</strong>
                      </Link>
                      <p style={{ margin: '4px 0 0 0', color: '#666', fontSize: 12 }}>
                        上传你的简历文件，系统将进行智能解析
                      </p>
                    </div>
                  ),
                },
                {
                  dot: <UserOutlined style={{ fontSize: 16 }} />,
                  children: (
                    <div>
                      <Link to="/profile">
                        <strong>第二步：查看画像</strong>
                      </Link>
                      <p style={{ margin: '4px 0 0 0', color: '#666', fontSize: 12 }}>
                        系统根据简历自动构建你的职业画像
                      </p>
                    </div>
                  ),
                },
                {
                  dot: <BarChartOutlined style={{ fontSize: 16 }} />,
                  children: (
                    <div>
                      <Link to="/matching">
                        <strong>第三步：岗位匹配</strong>
                      </Link>
                      <p style={{ margin: '4px 0 0 0', color: '#666', fontSize: 12 }}>
                        获得与目标岗位的匹配度分析
                      </p>
                    </div>
                  ),
                },
                {
                  dot: <NodeIndexOutlined style={{ fontSize: 16 }} />,
                  children: (
                    <div>
                      <Link to="/career">
                        <strong>第四步：规划路径</strong>
                      </Link>
                      <p style={{ margin: '4px 0 0 0', color: '#666', fontSize: 12 }}>
                        获得个性化的职业发展路径建议
                      </p>
                    </div>
                  ),
                },
                {
                  dot: <FileTextOutlined style={{ fontSize: 16 }} />,
                  children: (
                    <div>
                      <Link to="/report">
                        <strong>第五步：生成报告</strong>
                      </Link>
                      <p style={{ margin: '4px 0 0 0', color: '#666', fontSize: 12 }}>
                        导出完整的职业规划分析报告
                      </p>
                    </div>
                  ),
                },
              ]}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={[24, 24]} style={{ marginTop: 48 }}>
        <Col xs={24}>
          <Card 
            title={<h3 style={{ margin: 0 }}>💡 如何使用本系统</h3>}
          >
            <ol style={{ lineHeight: 2 }}>
              <li>进入"简历上传"页面，上传你的简历文件（支持 PDF、Word、TXT 格式）</li>
              <li>系统将使用 AI 技术解析你的简历，提取关键信息如技能、学历、经验等</li>
              <li>在"学生画像"页面查看系统为你生成的职业画像和能力评估</li>
              <li>切换到"岗位匹配"页面，查看与你意向岗位的匹配度分析</li>
              <li>在"职业规划"页面获得个性化的职业发展路径和建议</li>
              <li>最后，生成并下载你的职业规划分析报告</li>
            </ol>
          </Card>
        </Col>
      </Row>
    </div>
  )
}

export default Home