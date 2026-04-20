// 学生信息类型定义
export interface StudentInfo {
  name: string
  gender: string
  phone: string
  email: string
  school: string
  school_level?: string
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

export interface EmploymentAbilityDimensionItem {
  score?: number
  level?: string
  evidence?: string[]
}

export interface StudentProfileResult {
  complete_score: number
  competitiveness_score: number
  score_level: string
  soft_skills: string[]
  employment_ability_profile?: Record<string, EmploymentAbilityDimensionItem>
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
      school_level?: string
      major?: string
      degree?: string
      graduation_year?: string
    }
    normalized_education?: {
      degree?: string
      school?: string
      school_level?: string
      major_raw?: string
      major_std?: string
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

export interface TargetJobCandidate {
  standard_job_name?: string
  candidate_score?: number
  sample_count?: number
  job_category?: string
  mainstream_degree?: string
  mainstream_majors?: string[]
  mainstream_certificates?: string[]
  top_skills?: string[]
  required_knowledge_points?: string[]
  preferred_knowledge_points?: string[]
  match_reason?: string
  is_core_job?: boolean
}

export interface JobNameResolution {
  resolution_status?: 'resolved' | 'needs_confirmation' | 'unresolved' | string
  requested_job_name?: string
  resolved_standard_job_name?: string
  confirmed_standard_job_name?: string
  asset_found?: boolean
  resolution_method?: string
  resolution_confidence?: number
  candidate_jobs?: Array<string | TargetJobCandidate>
  candidate_job_names?: string[]
  matched_alias?: string
  confirmation_source?: string
  message?: string
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

export interface JobAbilityRequirement {
  dimension?: string
  label?: string
  score?: number
  level?: string
  keywords?: string[]
  evidence_ratio?: number
  evidence_count?: number
  description?: string
}

export interface JobAbilityRadarItem {
  dimension?: string
  key?: string
  score?: number
}

export interface JobAbilitySourceQuality {
  soft_skill_coverage?: number
  practice_coverage?: number
  hard_skill_coverage?: number
  confidence?: number
  [key: string]: number | string | undefined
}

export interface AbilityMatchDimension {
  dimension?: string
  label?: string
  student_score?: number
  student_level?: string
  job_required_score?: number
  job_required_level?: string
  gap?: number
  risk_level?: RiskLevel
  student_evidence?: string[]
  job_evidence?: string[]
  job_keywords?: string[]
  message?: string
}

export interface AbilityMatch {
  evaluation_status?: string
  overall_ability_match_score?: number | null
  dimensions?: AbilityMatchDimension[]
  main_strengths?: string[]
  main_risks?: string[]
  message?: string
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
  ability_requirements?: Record<string, JobAbilityRequirement>
  ability_radar?: JobAbilityRadarItem[]
  ability_source_quality?: JobAbilitySourceQuality
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
  confirmed_standard_job_name?: string
  resolution_status?: string
  asset_found?: boolean
  resolution_method?: string
  resolution_confidence?: number
  asset_resolution?: JobNameResolution
  evaluation_status?: string
  candidate_jobs?: Array<string | TargetJobCandidate>
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
  ability_requirements?: Record<string, JobAbilityRequirement>
  ability_radar?: JobAbilityRadarItem[]
  ability_source_quality?: JobAbilitySourceQuality
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
    pass?: boolean | null
    reason?: string
  }
  major?: {
    student_value?: string
    job_gate_set?: string[]
    pass?: boolean | null
    reason?: string
  }
  certificate?: {
    student_values?: string[]
    must_have_certificates?: string[]
    preferred_certificates?: string[]
    pass?: boolean | null
    reason?: string
  }
  all_pass?: boolean | null
}

export interface SkillKnowledgeMatch {
  required_knowledge_points?: string[]
  preferred_knowledge_points?: string[]
  student_knowledge_points?: string[]
  matched_knowledge_points?: string[]
  missing_knowledge_points?: string[]
  knowledge_point_accuracy?: number | null
  pass?: boolean | null
  risk_level?: RiskLevel
  message?: string
}

export interface ContestEvaluation {
  hard_info_pass?: boolean | null
  skill_accuracy_pass?: boolean | null
  contest_match_success?: boolean | null
}

export interface TargetJobMatch {
  job_name?: string
  asset_job_name?: string
  match_type?: string
  asset_found?: boolean
  resolution_status?: string
  evaluation_status?: string
  message?: string
  job_name_resolution?: JobNameResolution
  candidate_jobs?: Array<string | TargetJobCandidate>
  sample_count?: number
  overall_match_score?: number | null
  rule_match_score?: number | null
  asset_match_score?: number | null
  display_match_score?: number | null
  score_source?: string
  score_explanation?: string
  requirement_distributions?: RequirementDistributions
  hard_info_display?: HardInfoDisplay
  hard_info_evaluation?: HardInfoEvaluation
  skill_knowledge_match?: SkillKnowledgeMatch
  ability_match?: AbilityMatch
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
  ability_match_score?: number
  risk_level?: RiskLevel
  recommendation_reason?: string
}

export interface TargetJobConfirmation {
  requested_job_name?: string
  confirmed_standard_job_name?: string
  confirmation_source?: string
  confirmed_at?: string
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
  target_job_confirmation?: TargetJobConfirmation
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

export interface RepresentativePromotionPath {
  source_job?: string
  promote_targets?: string[]
  edge_count?: number
  source?: string
  selection_reason?: string
}

export interface JobPathGraphNode {
  id: string
  label?: string
  node_type?: string
  job_category?: string
  job_level?: string
  degree_requirement?: string
  major_requirement?: string[]
  occurrence_count?: number | string
  is_isolated?: boolean
}

export interface JobPathGraphEdge {
  id: string
  source: string
  target: string
  relation: 'PROMOTE_TO' | 'TRANSFER_TO' | string
  label?: string
  edge_type?: 'promotion' | 'transfer' | string
  source_name?: string
  target_name?: string
  reason?: string
  confidence?: number | string
}

export interface JobPathGraphStats {
  job_node_count?: number
  promote_edge_count?: number
  transfer_edge_count?: number
  total_edge_count?: number
}

export interface JobPathGraphResponse {
  graph_status?: 'available' | 'empty' | 'unavailable' | string
  source?: 'neo4j' | 'csv_fallback' | 'none' | string
  graph_scope?: 'curated' | 'all' | string
  raw_node_count?: number
  raw_edge_count?: number
  filtered_node_count?: number
  filtered_edge_count?: number
  filter_notes?: string[]
  stats?: JobPathGraphStats
  nodes?: JobPathGraphNode[]
  edges?: JobPathGraphEdge[]
  message?: string
}

// 职业路径规划结果
export interface CareerPathResult {
  primary_target_job: string
  user_target_job?: string
  system_recommended_job?: string
  primary_plan_job?: string
  target_job_role?: string
  recommended_job_role?: string
  goal_decision_source?: string
  goal_decision_confidence?: string
  goal_decision_reason?: string[]
  goal_decision_context?: Record<string, unknown>
  llm_goal_decision_explanation?: {
    decision_reason_summary?: string
    why_recommended_job?: string
    why_target_job_not_primary?: string
    how_to_balance_target_and_recommended?: string
    short_term_focus?: string[]
    mid_term_focus?: string[]
    risk_notes?: string[]
  }
  secondary_target_jobs: string[]
  goal_positioning: string
  goal_reason: string
  direct_path: string[]
  transition_path: string[]
  long_term_path: string[]
  path_strategy: string
  target_path_data_status?: string
  target_path_data_message?: string
  representative_promotion_paths?: RepresentativePromotionPath[]
  representative_path_count?: number
  representative_path_status?: string
  representative_path_message?: string
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

export interface AIResultCard {
  type?: string
  company_name?: string
  city?: string
  job_title?: string
  standard_job_name?: string
  salary_range?: string
  industry?: string
  company_size?: string
  company_type?: string
  reason?: string
  match_reason?: string
}

export interface AIResultTable {
  title?: string
  columns?: string[]
  rows?: Array<Record<string, unknown>>
}

export interface AISqlEvidence {
  enabled?: boolean
  data_source?: string
  db_table?: string
  data_file?: string
  sql_source?: string
  generated_sql?: string
  row_count?: number
  error?: string
}

export interface AIIntentResult {
  intent?: string
  confidence?: number
  query_domain?: string
  reason?: string
  should_use_sql?: boolean
  should_use_path_graph?: boolean
  should_use_semantic?: boolean
  should_use_profile_context?: boolean
}

export interface AIPathGraphEvidence {
  enabled?: boolean
  path_graph_status?: string
  source?: string
  stats?: JobPathGraphStats
  matched_edges?: Array<{
    source_job?: string
    target_job?: string
    relation?: string
    label?: string
    source?: string
  }>
  summary_text?: string
  message?: string
}

export interface AIChatEvidence {
  context_chunks?: Array<Record<string, unknown>>
  semantic_hits?: Array<Record<string, unknown>>
  sql?: AISqlEvidence
  path_graph?: AIPathGraphEvidence
}

export interface AIChatData {
  conversation_id: string
  intent?: string
  intent_result?: AIIntentResult
  answer: string
  source: string
  context_summary: string
  used_context_sources: string[]
  local_sources_used?: string[]
  loaded_files: string[]
  missing_files: string[]
  result_cards?: AIResultCard[]
  result_table?: AIResultTable
  summary_stats?: Record<string, unknown>
  evidence?: AIChatEvidence
  sql_debug?: AISqlEvidence
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
