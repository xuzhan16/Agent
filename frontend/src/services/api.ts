import axios from 'axios'
import {
  StudentInfo,
  StudentProfileResult,
  JobMatchResult,
  CareerPathResult,
  ApiResponse,
  JobProfileAssetsData,
  JobPathGraphResponse,
  ReportDetail,
  PipelineStatus,
  AIContextSummaryData,
  AIChatData,
} from '../types'

const api = axios.create({
  baseURL: '/api',
  timeout: 300000, // 修改为5分钟，因为后台执行完整流水线需要经过 6 次 LLM 推理
})

// 请求拦截器
api.interceptors.request.use(
  (config) => {
    // 可以在这里添加认证token等
    return config
  },
  (error) => {
    return Promise.reject(error)
  }
)

// 响应拦截器
api.interceptors.response.use(
  (response) => {
    return response.data
  },
  (error) => {
    console.error('API Error:', error)
    return Promise.reject(error)
  }
)

// API接口定义
export const careerApi = {
  // 简历解析
  parseResume: (file: File): Promise<ApiResponse<StudentInfo>> => {
    const formData = new FormData()
    formData.append('resume', file)
    return api.post('/resume/parse', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    })
  },

  // 文本手动录入简历
  parseManualResume: (resumeText: string): Promise<ApiResponse<StudentInfo>> => {
    return api.post('/resume/manual', {
      resume_text: resumeText,
    })
  },

  // 学生画像查询（状态接口，不消费请求体）
  buildStudentProfile: (): Promise<ApiResponse<StudentProfileResult>> => {
    return api.get('/student/profile')
  },

  // 岗位匹配查询（状态接口，不消费请求体）
  matchJobs: (): Promise<ApiResponse<JobMatchResult[]>> => {
    return api.get('/job/match')
  },

  // 岗位画像资产查询（全局岗位资产，不依赖学生状态）
  getJobProfileAssets: (): Promise<ApiResponse<JobProfileAssetsData>> => {
    return api.get('/job/profile-assets')
  },

  // 用户确认目标岗位对应的本地标准岗位
  confirmTargetJob: (data: {
    requested_job_name: string
    confirmed_standard_job_name: string
  }): Promise<ApiResponse<{
    target_job_confirmation?: unknown
    target_job_profile_assets?: unknown
    target_job_match?: unknown
  }>> => {
    return api.post('/job/confirm-target', data)
  },

  // 全量岗位路径知识图谱
  getJobPathGraph: (): Promise<ApiResponse<JobPathGraphResponse>> => {
    return api.get('/job-path-graph/all')
  },

  // 职业路径查询（状态接口，不消费请求体）
  planCareerPath: (): Promise<ApiResponse<CareerPathResult>> => {
    return api.get('/career/path')
  },

  // 生成报告
  generateReport: (data: {
    student_info: StudentInfo
    student_profile: StudentProfileResult
    job_matches: JobMatchResult[]
    career_path: CareerPathResult
    report_format?: string
  }): Promise<ApiResponse<string>> => {
    return api.post('/report/generate', data)
  },

  // 获取已生成报告内容
  getReport: (): Promise<ApiResponse<string>> => {
    return api.get('/report')
  },

  // 获取报告详情
  getReportDetail: (): Promise<ApiResponse<ReportDetail>> => {
    return api.get('/report/detail')
  },

  // 保存编辑后的报告
  updateReport: (reportText: string): Promise<ApiResponse<string>> => {
    return api.post('/report/update', {
      report_text: reportText,
    })
  },

  // 获取共享报告内容
  getSharedReport: (fileName?: string): Promise<ApiResponse<string>> => {
    return api.get('/report/shared', { params: { file_name: fileName } })
  },

  // 查询流水线实时进度
  getPipelineStatus: (): Promise<ApiResponse<PipelineStatus>> => {
    return api.get('/pipeline/status')
  },

  // AI 上下文摘要
  getAIContextSummary: (): Promise<ApiResponse<AIContextSummaryData>> => {
    return api.get('/ai/context-summary')
  },

  // AI 对话
  chatWithAI: (data: {
    message: string
    conversation_id?: string
    web_search_enabled?: boolean
  }): Promise<ApiResponse<AIChatData>> => {
    return api.post('/ai/chat', data)
  },

  // 直接下载已生成报告文件
  downloadReportUrl: (fileName: string) => {
    return `${api.defaults.baseURL}/report/download?file_name=${encodeURIComponent(fileName)}`
  },
}

export default api
