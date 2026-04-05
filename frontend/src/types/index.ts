// 学生信息类型定义
export interface StudentInfo {
  name: string
  gender: string
  phone: string
  email: string
  school: string
  major: string
  degree: string
  graduation_year: string
  position?: string
  education?: string
  experience?: string
  skills: string[]
  certificates: string[]
  project_experience: ProjectExperience[]
  internship_experience: InternshipExperience[]
}

export interface ProjectExperience {
  project_name: string
  role: string
  description: string
}

export interface InternshipExperience {
  company_name: string
  position: string
  description: string
}

// 岗位画像类型
export interface JobProfile {
  standard_job_name: string
  job_category: string
  required_degree: string
  preferred_majors: string[]
  required_skills: string[]
}

// 岗位匹配结果
export interface JobMatchResult {
  job_name: string
  match_score: number
  match_level: string
  reasons: string[]
}

// 职业路径规划结果
export interface CareerPathResult {
  primary_target_job: string
  secondary_target_jobs: string[]
  goal_positioning: string
  goal_reason: string
  direct_path: string[]
  transition_path: string[]
  long_term_path: string[]
  path_strategy: string
  short_term_plan: string[]
  mid_term_plan: string[]
  risk_and_gap: string[]
  fallback_strategy: string
}

// API响应类型
export interface ApiResponse<T> {
  success: boolean
  data: T
  message?: string
}