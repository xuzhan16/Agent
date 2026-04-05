import axios from 'axios'
import { StudentInfo, JobProfile, JobMatchResult, CareerPathResult, ApiResponse } from '../types'

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

  // 学生画像构建
  buildStudentProfile: (studentInfo: StudentInfo): Promise<ApiResponse<JobProfile>> => {
    return api.post('/student/profile', studentInfo)
  },

  // 岗位匹配
  matchJobs: (studentProfile: JobProfile): Promise<ApiResponse<JobMatchResult[]>> => {
    return api.post('/job/match', studentProfile)
  },

  // 职业路径规划
  planCareerPath: (studentProfile: JobProfile, jobMatches: JobMatchResult[]): Promise<ApiResponse<CareerPathResult>> => {
    return api.post('/career/path', {
      student_profile: studentProfile,
      job_matches: jobMatches,
    })
  },

  // 生成报告
  generateReport: (data: {
    student_info: StudentInfo
    student_profile: JobProfile
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

  // 获取共享报告内容
  getSharedReport: (fileName?: string): Promise<ApiResponse<string>> => {
    return api.get('/report/shared', { params: { file_name: fileName } })
  },

  // 直接下载已生成报告文件
  downloadReportUrl: (fileName: string) => {
    return `${api.defaults.baseURL}/report/download?file_name=${encodeURIComponent(fileName)}`
  },
}

export default api