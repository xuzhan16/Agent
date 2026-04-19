import { Tag } from 'antd'

interface DataSourceTagsProps {
  sources?: string[]
  offline?: boolean
  className?: string
}

const labelMap: Record<string, string> = {
  jobs_db: 'SQLite jobs.db',
  csv_fallback: 'CSV fallback',
  student_profile: '学生画像',
  job_match: '人岗匹配',
  career_path: '职业路径',
  career_report: '职业报告',
  report_data: '报告摘要',
  semantic_kb: '岗位语义知识库',
  semantic_context: '岗位语义知识库',
  job_path_graph: '岗位路径图谱',
  path_graph: '岗位路径图谱',
  sql_query: '结构化查询',
  neo4j: 'Neo4j',
  offline_mode: '未联网',
}

const colorMap: Record<string, string> = {
  jobs_db: 'blue',
  csv_fallback: 'orange',
  student_profile: 'cyan',
  job_match: 'purple',
  career_path: 'geekblue',
  career_report: 'magenta',
  report_data: 'magenta',
  semantic_kb: 'green',
  semantic_context: 'green',
  job_path_graph: 'cyan',
  path_graph: 'volcano',
  sql_query: 'blue',
  neo4j: 'volcano',
  offline_mode: 'gold',
}

const DataSourceTags = ({ sources = [], offline = true, className = '' }: DataSourceTagsProps) => {
  const normalized = Array.from(new Set([...(sources || []), ...(offline ? ['offline_mode'] : [])]))

  return (
    <div className={`product-data-source-tags ${className}`.trim()}>
      {normalized.map((source) => (
        <Tag key={source} color={colorMap[source] || 'blue'}>
          {labelMap[source] || source}
        </Tag>
      ))}
    </div>
  )
}

export default DataSourceTags
