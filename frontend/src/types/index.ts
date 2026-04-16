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

export type RiskLevel = 'high_match' | 'risk' | 'no_match' | string

export interface JobNameResolution {
  requested_job_name?: string
  resolved_standard_job_name?: string
  asset_found?: boolean
  resolution_method?: string
  resolution_confidence?: number
  candidate_jobs?: string[]
  matched_alias?: string
}

export interface RequirementDistributionItem {
  rank?: number
  name?: string
  count?: number
  ratio?: number
}

export interface RequirementDistributions {
  degree_distribution?: RequirementDistributionItem[]
  major_distribution?: RequirementDistributionItem[]
  certificate_distribution?: RequirementDistributionItem[]
  no_certificate_requirement_ratio?: number
}

export interface CoreJobProfile extends RequirementDistributions {
  standard_job_name?: string
  sample_count?: number
  job_category?: string
  job_level_summary?: string
  display_order?: number
  selection_reason?: string
  mainstream_degree?: string
  mainstream_majors_summary?: string | string[]
  mainstream_cert_summary?: string | string[]
  top_skills?: string[]
  degree_gate?: string
  major_gate_set?: string[]
  must_have_certificates?: string[]
  preferred_certificates?: string[]
  hard_skills?: string[]
  tools_or_tech_stack?: string[]
  required_knowledge_points?: string[]
  preferred_knowledge_points?: string[]
  source_quality?: Record<string, number | string>
}

export interface JobProfileAssetsSummary {
  core_job_count?: number
  standard_job_count?: number
  sample_count?: number
  generated_at?: string
}

export interface JobProfileAssetsData {
  summary?: JobProfileAssetsSummary
  core_job_profiles?: CoreJobProfile[]
}

export interface TargetJobProfileAssets extends RequirementDistributions {
  requested_job_name?: string
  standard_job_name?: string
  resolved_standard_job_name?: string
  asset_found?: boolean
  resolution_method?: string
  resolution_confidence?: number
  asset_resolution?: JobNameResolution
  evaluation_status?: string
  message?: string
  sample_count?: number
  job_category?: string
  job_level_summary?: string
  mainstream_degree?: string
  mainstream_degree_ratio?: number
  mainstream_majors?: string[]
  mainstream_certificates?: string[]
  degree_gate?: string
  major_gate_set?: string[]
  must_have_certificates?: string[]
  preferred_certificates?: string[]
  required_knowledge_points?: string[]
  preferred_knowledge_points?: string[]
  source_quality?: Record<string, number | string>
}

export interface HardInfoDisplay {
  degree?: {
    student_value?: string
    mainstream_requirement?: string
    mainstream_ratio?: number
    qualified_ratio?: number
    higher_requirement_ratio?: number
    risk_level?: RiskLevel
    message?: string
  }
  major?: {
    student_value?: string
    mainstream_majors?: string[]
    matched_ratio?: number
    risk_level?: RiskLevel
    message?: string
  }
  certificate?: {
    student_values?: string[]
    must_have_certificates?: string[]
    preferred_certificates?: string[]
    matched_ratio?: number
    risk_level?: RiskLevel
    message?: string
  }
}

export interface HardInfoEvaluation {
  degree?: {
    student_value?: string
    job_gate?: string
    pass?: boolean
    reason?: string
  }
  major?: {
    student_value?: string
    job_gate_set?: string[]
    pass?: boolean
    reason?: string
  }
  certificate?: {
    student_values?: string[]
    must_have_certificates?: string[]
    preferred_certificates?: string[]
    pass?: boolean
    reason?: string
  }
  all_pass?: boolean
}

export interface SkillKnowledgeMatch {
  required_knowledge_points?: string[]
  preferred_knowledge_points?: string[]
  student_knowledge_points?: string[]
  matched_knowledge_points?: string[]
  missing_knowledge_points?: string[]
  knowledge_point_accuracy?: number
  pass?: boolean
  risk_level?: RiskLevel
}

export interface ContestEvaluation {
  hard_info_pass?: boolean
  skill_accuracy_pass?: boolean
  contest_match_success?: boolean
}

export interface TargetJobMatch {
  job_name?: string
  asset_job_name?: string
  match_type?: string
  asset_found?: boolean
  evaluation_status?: string
  message?: string
  job_name_resolution?: JobNameResolution
  sample_count?: number
  overall_match_score?: number
  rule_match_score?: number
  asset_match_score?: number
  display_match_score?: number
  score_source?: string
  score_explanation?: string
  requirement_distributions?: RequirementDistributions
  hard_info_display?: HardInfoDisplay
  hard_info_evaluation?: HardInfoEvaluation
  skill_knowledge_match?: SkillKnowledgeMatch
  contest_evaluation?: ContestEvaluation
  risk_level?: RiskLevel
}

export interface RecommendedJobMatch extends TargetJobMatch {
  recommendation_reason?: string
}

export interface RecommendationRankingItem {
  rank?: number
  job_name?: string
  overall_match_score?: number
  display_match_score?: number
  hard_info_pass?: boolean
  knowledge_point_accuracy?: number
  risk_level?: RiskLevel
  recommendation_reason?: string
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
  target_job_match?: TargetJobMatch
  recommended_job_match?: RecommendedJobMatch
  recommendation_ranking?: RecommendationRankingItem[]
  core_job_profiles?: CoreJobProfile[]
  target_job_profile_assets?: TargetJobProfileAssets
  match_input_payload?: {
    job_profile?: {
      raw_job_profile_result?: {
        core_job_profiles?: CoreJobProfile[]
        target_job_profile_assets?: TargetJobProfileAssets
      }
    }
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
