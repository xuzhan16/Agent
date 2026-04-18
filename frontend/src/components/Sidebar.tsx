import { Menu, Layout } from 'antd'
import {
  UploadOutlined,
  UserOutlined,
  BarChartOutlined,
  NodeIndexOutlined,
  ApartmentOutlined,
  FileTextOutlined,
  HomeOutlined,
  MessageOutlined,
  ProfileOutlined,
} from '@ant-design/icons'
import { Link, useLocation } from 'react-router-dom'

const { Sider } = Layout

interface SidebarProps {
  collapsed?: boolean
}

const Sidebar = ({ collapsed = false }: SidebarProps) => {
  const location = useLocation()

  const menuItems = [
    {
      key: '/',
      icon: <HomeOutlined />,
      label: <Link to="/">首页</Link>,
    },
    {
      key: '/resume',
      icon: <UploadOutlined />,
      label: <Link to="/resume">简历上传</Link>,
    },
    {
      key: '/profile',
      icon: <UserOutlined />,
      label: <Link to="/profile">学生画像</Link>,
    },
    {
      key: '/job-profile',
      icon: <ProfileOutlined />,
      label: <Link to="/job-profile">岗位画像</Link>,
    },
    {
      key: '/matching',
      icon: <BarChartOutlined />,
      label: <Link to="/matching">岗位匹配</Link>,
    },
    {
      key: '/career',
      icon: <NodeIndexOutlined />,
      label: <Link to="/career">职业规划</Link>,
    },
    {
      key: '/job-path-graph',
      icon: <ApartmentOutlined />,
      label: <Link to="/job-path-graph">岗位路径图谱</Link>,
    },
    {
      key: '/report',
      icon: <FileTextOutlined />,
      label: <Link to="/report">报告生成</Link>,
    },
    {
      key: '/ai-assistant',
      icon: <MessageOutlined />,
      label: <Link to="/ai-assistant">AI 助手</Link>,
    },
  ]

  return (
    <Sider
      collapsible
      collapsed={collapsed}
      style={{
        background: 'linear-gradient(180deg, #0f172a 0%, #08101f 100%)',
        boxShadow: '2px 0 20px rgba(15, 23, 42, 0.18)',
      }}
    >
      <div
        style={{
          height: 72,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: 'white',
          fontSize: collapsed ? 20 : 16,
          fontWeight: 700,
          padding: '0 16px',
          overflow: 'hidden',
          whiteSpace: 'nowrap',
          textAlign: 'center',
        }}
      >
        {collapsed ? '📘' : 'Career Planning'}
      </div>
      <Menu
        theme="dark"
        mode="inline"
        selectedKeys={[location.pathname]}
        items={menuItems}
        style={{
          background: 'transparent',
          border: 'none',
          paddingTop: 12,
        }}
      />
    </Sider>
  )
}

export default Sidebar
