# 赛题提交前 Smoke Check

- 生成时间：2026-04-18T17:13:41+08:00
- PASS：14
- WARN：0
- FAIL：0

| 状态 | 检查项 | 说明 |
| --- | --- | --- |
| PASS | 文件资产检查 | 关键资产文件存在且非空 |
| PASS | 10 个核心岗位资产 | 核心岗位数量：10 |
| PASS | 七维能力画像资产 | 岗位能力画像数量：49 |
| PASS | SQLite 标准岗位名非空率 | job_detail 行数 2638，standard_job_name 非空 2638（100.0%） |
| PASS | job_market_view 统一查询视图 | job_market_view 可查询 |
| PASS | SQLite 市场事实查询 | 北京公司、Java/前端岗位、薪资字段可查询 |
| PASS | Neo4j CSV fallback | CSV 晋升关系 298 条，转岗关系 279 条 |
| PASS | 精选岗位路径图谱 | 精选图谱晋升关系 75 条，总关系 144 条 |
| PASS | 主链路状态文件 | 主链路状态文件存在 |
| PASS | 岗位画像输出结构 | core_job_profiles=10 |
| PASS | 人岗匹配赛题结构 | 检查 hard_info_evaluation / skill_knowledge_match / ability_match |
| PASS | 职业路径不造伪路径 | 未发现典型伪路径话术 |
| PASS | 报告赛题字段覆盖 | 报告中命中字段：学历, 专业, 证书, 知识点, 七维能力, 能力 |
| PASS | 前端关键页面文件 | 前端关键页面存在 |
