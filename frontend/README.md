# AI职业规划系统前端

基于 React + TypeScript + Ant Design 开发的AI职业规划系统前端界面。

## 功能特性

- 🔄 简历智能解析
- 👤 学生画像构建
- 📊 岗位匹配可视化
- 🗺️ 职业路径规划
- 📄 报告自动生成

## 技术栈

- **前端框架**: React 18 + TypeScript
- **UI组件**: Ant Design
- **状态管理**: Zustand
- **路由**: React Router
- **HTTP客户端**: Axios
- **构建工具**: Vite

## 快速开始

### 安装依赖
```bash
cd frontend
npm install
```

### 开发环境运行
```bash
npm run dev
```

### 构建生产版本
```bash
npm run build
```

### 预览生产版本
```bash
npm run preview
```

## 项目结构

```
frontend/
├── public/                 # 静态资源
├── src/
│   ├── components/         # 通用组件
│   ├── pages/             # 页面组件
│   ├── services/          # API服务
│   ├── store/             # 状态管理
│   ├── types/             # TypeScript类型定义
│   ├── utils/             # 工具函数
│   ├── App.tsx            # 应用主组件
│   ├── main.tsx           # 应用入口
│   └── index.css          # 全局样式
├── package.json
├── tsconfig.json
├── vite.config.ts
└── README.md
```

## 开发指南

### 添加新页面
1. 在 `src/pages/` 下创建页面组件
2. 在 `src/App.tsx` 中添加路由
3. 在 `src/components/Sidebar.tsx` 中添加菜单项

### API集成
在 `src/services/api.ts` 中定义API接口，使用 `src/store/index.ts` 管理状态。

### 组件开发
遵循以下原则：
- 使用 TypeScript 进行类型检查
- 使用 Ant Design 组件库
- 保持组件的可复用性
- 遵循单一职责原则

## 部署

### 开发环境
```bash
npm run dev
```
访问 http://localhost:3000

### 生产环境
```bash
npm run build
npm run preview
```

## 后端集成

前端通过 `/api` 代理与后端通信，确保后端服务运行在 `http://localhost:8000`。

## 贡献指南

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建 Pull Request

## 许可证

本项目采用 MIT 许可证。