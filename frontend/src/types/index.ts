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

export interface StudentProfileResult {
  complete_score: number
  competitiveness_score: number
  score_level: string
  soft_skills: string[]
  strengths: string[]
  weaknesses: string[]
  missing_dimensions: string[]
  summary: string
  potential_profile?: {
    growth_level?: string
    preferred_directions?: string[]
    domain_tags?: string[]
    basis_score?: number
    reason?: string
  }
  rule_score_result?: {
    completeness_detail?: {
      basic_info_score?: number
      education_score?: number
      skill_score?: number
      project_score?: number
      internship_score?: number
      qualification_score?: number
      intention_score?: number
    }
    competitiveness_detail?: {
      education_base_score?: number
      skill_base_score?: number
      tool_base_score?: number
      project_base_score?: number
      internship_base_score?: number
      qualification_base_score?: number
      occupation_focus_score?: number
      domain_bonus_score?: number
    }
  }
  profile_input_payload?: {
    basic_info?: {
      name?: string
      school?: string
      major?: string
      degree?: string
      graduation_year?: string
    }
    normalized_profile?: {
      hard_skills?: string[]
      tool_skills?: string[]
      occupation_hints?: string[]
      domain_tags?: string[]
    }
    explicit_profile?: {
      certificates?: string[]
      project_experience?: ProjectExperience[]
      internship_experience?: InternshipExperience[]
    }
    practice_profile?: {
      project_count?: number
      internship_count?: number
    }
  }
}

export interface JobProfileSummary {
  standard_job_name: string
  job_category?: string
  degree_requirement?: string
  major_requirement?: string[]
  hard_skills?: string[]
  tools_or_tech_stack?: string[]
}

// 岗位匹配结果
export interface JobMatchResult {
  job_name: string
  match_score: number
  match_level: string
  reasons: string[]
  company?: string
  salary?: string
  strengths?: string[]
  weaknesses?: string[]
  improvement_suggestions?: string[]
  recommendation?: string
  analysis_summary?: string
  dimension_scores?: {
    basic_requirement_score?: number
    vocational_skill_score?: number
    professional_quality_score?: number
    development_potential_score?: number
  }
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
  target_selection_reason?: string[]
  path_selection_reason?: string[]
}

export interface ReportDetail {
  file_name: string
  report_title: string
  report_summary: string
  report_text: string
  report_sections: Array<{
    section_title: string
    section_content: string
  }>
  edit_suggestions: string[]
  completeness_check: {
    is_complete?: boolean
    missing_sections?: string[]
  }
}

export interface PipelineStatus {
  status: 'idle' | 'running' | 'completed' | 'failed' | string
  current_step: number
  total_steps: number
  step_name: string
  error?: string | null
  updated_at?: string
}

export interface AIContextSummaryData {
  summary: string
  loaded_files: string[]
  missing_files: string[]
}

export interface AIChatData {
  conversation_id: string
  answer: string
  source: string
  context_summary: string
  used_context_sources: string[]
  loaded_files: string[]
  missing_files: string[]
}

// API响应类型
export interface ApiResponse<T> {
  success: boolean
  data: T
  status?: string
  source?: string
  last_updated?: string
  message?: string
}
