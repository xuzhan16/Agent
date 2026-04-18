import { Routes, Route } from 'react-router-dom'
import { Layout, Button } from 'antd'
import { MenuFoldOutlined, MenuUnfoldOutlined } from '@ant-design/icons'
import Sidebar from './components/Sidebar'
import Home from './pages/Home'
import ResumeUpload from './pages/ResumeUpload'
import StudentProfile from './pages/StudentProfile'
import JobProfile from './pages/JobProfile'
import JobMatching from './pages/JobMatching'
import CareerPath from './pages/CareerPath'
import JobPathGraph from './pages/JobPathGraph'
import Report from './pages/Report'
import SharedReport from './pages/SharedReport'
import AIAssistant from './pages/AIAssistant'
import './App.css'
import { useState } from 'react'

const { Header, Content } = Layout

function App() {
  const [collapsed, setCollapsed] = useState(false)

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sidebar collapsed={collapsed} />
      <Layout>
        <Header 
          style={{ 
            background: 'linear-gradient(135deg, #3b82f6 0%, #8b5cf6 100%)',
            padding: '0 24px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            boxShadow: '0 8px 24px rgba(59, 130, 246, 0.15)',
            borderBottom: '1px solid rgba(255,255,255,0.12)',
          }}
        >
          <div style={{ color: 'white', fontSize: 20, fontWeight: 700 }}>
            🚀 AI 职业规划系统
          </div>
          <Button 
            type="text" 
            style={{ color: 'white', fontSize: 18 }}
            onClick={() => setCollapsed(!collapsed)}
          >
            {collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
          </Button>
        </Header>
        <Content style={{ margin: '24px 16px', padding: 24, background: 'transparent', minHeight: 'calc(100vh - 64px)' }}>
          <div style={{ maxWidth: 1400, margin: '0 auto' }}>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/resume" element={<ResumeUpload />} />
              <Route path="/profile" element={<StudentProfile />} />
              <Route path="/job-profile" element={<JobProfile />} />
              <Route path="/matching" element={<JobMatching />} />
              <Route path="/career" element={<CareerPath />} />
              <Route path="/job-path-graph" element={<JobPathGraph />} />
              <Route path="/report" element={<Report />} />
              <Route path="/ai-assistant" element={<AIAssistant />} />
              <Route path="/report/view" element={<SharedReport />} />
            </Routes>
          </div>
        </Content>
      </Layout>
    </Layout>
  )
}

export default App
