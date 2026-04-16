# 基于 AI 的大学生职业规划智能体

当前仓库已经收口为单一主线结构：

- 前端：`E:\Agent\frontend`
- 后端：`E:\Agent\Agent-main`

这份 README 只写当前仓库真实可用的运行方式，重点回答三件事：

1. 前端和后端分别怎么启动
2. 后端怎么配置真实大模型
3. 岗位底库什么时候需要重建，什么时候可以直接复用

## 1. 项目结构

```text
E:\Agent
├─ frontend/                 # React + Vite 前端
├─ Agent-main/               # FastAPI 后端 + 主流程 + 岗位底库处理
│  ├─ api_server.py          # 后端 HTTP 入口
│  ├─ main_pipeline.py       # 6 段主流程编排入口
│  ├─ job_data_pipeline.py   # 岗位底库一键处理入口
│  ├─ llm_interface_layer/   # 统一大模型接口层
│  ├─ job_data/              # 岗位数据清洗 / 去重 / 抽取 / 导出
│  ├─ outputs/
│  │  ├─ sql/jobs.db         # SQLite 岗位底库
│  │  ├─ neo4j/              # Neo4j 导入 CSV
│  │  └─ intermediate/       # 中间产物
│  └─ docs/neo4j-docker.md   # Neo4j 导入说明
└─ README.md
```

## 2. 主流程说明

后端当前的完整链路是：

```text
resume_parse
-> student_profile
-> job_profile
-> job_match
-> career_path_plan
-> career_report
```

前端实际使用方式是：

1. 前端上传简历到 `/api/resume/parse`
2. 后端在这一步直接跑完整条 6 段流水线
3. 结果写入 `E:\Agent\Agent-main\student_api_state.json`
4. 前端后续页面再调用：
   - `/api/student/profile`
   - `/api/job/match`
   - `/api/career/path`
   - `/api/report`
5. 这些接口主要是从当前状态文件里读取结果并返回

也就是说：

- 大模型真正的集中调用发生在 `resume/parse` 触发的整条后端流水线里
- 后面几个接口主要是读状态，不会每一步都重新跑整条链路

## 3. 目标岗位逻辑

1. 优先使用简历解析结果里的 `target_job_intention`
2. 如果简历没有明确目标岗位，则在 `student_profile` 阶段从规则生成的 `occupation_hints` 中取第一个候选方向兜底
3. 如果仍然无法确定，则前端显示“未明确目标岗位”

注意：

- 这套逻辑只影响“当前上传的简历”
- 当前后端仍使用单一状态文件 `student_api_state.json`
- 因此更适合单人演示或单会话调试，不适合多用户同时共用一个后端实例

## 4. 运行环境

建议环境：

- Python 3.10 或 3.11
- Node.js 18+
- npm 9+
- Windows PowerShell

推荐端口：

- 前端：`3000`
- 后端：`8000`
- Neo4j Browser：`7474`
- Neo4j Bolt：`7687`

## 5. 第一次安装

### 5.1 安装前端依赖

```powershell
Set-Location E:\Agent\frontend
npm install
```

### 5.2 安装后端依赖

根目录的 `requirements.txt` 只覆盖了 FastAPI 的基础依赖，不足以支撑当前整个后端链路。  
建议直接安装下面这组依赖：

```powershell
Set-Location E:\Agent
python -m pip install fastapi "uvicorn[standard]" python-multipart pandas pypdf neo4j xlrd openpyxl
```

依赖说明：

- `fastapi` / `uvicorn` / `python-multipart`：后端 API
- `pandas`：岗位底库处理、岗位画像聚合
- `pypdf`：PDF 简历解析
- `neo4j`：连接图数据库，可选但推荐
- `xlrd` / `openpyxl`：读取岗位 Excel 数据，可选但推荐

## 6. 运行前准备：岗位底库与 Neo4j

这一步建议放在“安装依赖之后、启动后端之前”。

原因是：主链路中的 `job_profile`、`career_path_plan` 会尝试读取 SQLite 和 Neo4j 图谱上下文；如果 Neo4j 没有导入数据，后端不会崩溃，但会降级为 SQL / 语义知识库 / fallback 结果，岗位路径和图谱关系会变弱。

### 6.1 岗位底库产物

当前仓库已经带有可复用的岗位底库产物：

- `E:\Agent\Agent-main\outputs\sql\jobs.db`
- `E:\Agent\Agent-main\outputs\neo4j\*.csv`
- `E:\Agent\Agent-main\outputs\intermediate\*.csv`
- `E:\Agent\Agent-main\outputs\match_assets\*.json`

如果你只是本机演示、前后端联调、上传简历跑职业规划，通常不需要重新跑 `job_data_pipeline.py`。

### 6.2 Neo4j 是否必须启动

不是必须，但推荐启动。

当前后端逻辑是：

- SQLite：岗位事实、薪资、公司、城市、行业、样本统计
- Neo4j：岗位结构、技能关系、学历关系、专业关系、晋升路径、转岗路径
- JSON + embedding：语义知识检索、相似岗位召回、解释增强

如果 Neo4j 没启动或没导入数据：

- 后端不会直接崩掉
- `query_neo4j()` 会返回空结果
- 主链路会继续使用 SQL、后处理资产、语义知识库和 fallback
- 但不能认为此时已经真正用上了 Neo4j 图数据库

### 6.3 启动并导入 Neo4j

后端目录为：

```powershell
Set-Location E:\Agent\Agent-main
```

如果是第一次导入，并且 `.infra/neo4j/data` 目录还是空的，可以运行：

```powershell
.\scripts\neo4j\import-graph.ps1
```

脚本会自动：

1. 读取 `outputs/neo4j/*.csv`
2. 创建 `.env.neo4j`
3. 创建本地 Neo4j 数据目录
4. 使用 `neo4j-admin database import` 导入 CSV
5. 启动 Docker 容器
6. 使用 `cypher-shell` 做一次验证查询

### 6.4 空库或重新生成 CSV 后必须强制重导

如果你曾经直接执行过：

```powershell
docker compose --env-file .env.neo4j -f docker-compose.neo4j.yml up -d
```

Neo4j 可能已经创建了一个空数据库。此时普通导入脚本会看到 `.infra/neo4j/data` 已存在，然后跳过导入，并提示：

```text
Existing local Neo4j data detected, skip reimport. Use -ForceReimport to rebuild from CSV.
```

这种情况下必须执行：

```powershell
Set-Location E:\Agent\Agent-main
.\scripts\neo4j\import-graph.ps1 -ForceReimport
```

正常导入时，日志里应该出现：

```text
[neo4j-setup] Force reimport enabled, clearing existing local Neo4j data
[neo4j-setup] Importing CSV files into a fresh Neo4j database
```

如果没有看到 `Importing CSV files into a fresh Neo4j database`，就说明没有真正导入图谱数据。

### 6.5 日常启停 Neo4j

导入完成后，日常只需要启动容器：

```powershell
Set-Location E:\Agent\Agent-main
docker compose --env-file .env.neo4j -f docker-compose.neo4j.yml up -d
```

停止容器：

```powershell
Set-Location E:\Agent\Agent-main
docker compose --env-file .env.neo4j -f docker-compose.neo4j.yml down
```

访问地址：

- Neo4j Browser：[http://localhost:7474](http://localhost:7474)
- Bolt：`bolt://localhost:7687`

登录账号：

- 用户名：`neo4j`
- 密码：默认已设置为 `12345678`，也可以读取 `E:\Agent\Agent-main\.env.neo4j` 中的 `NEO4J_PASSWORD`

### 6.6 验证 Neo4j 是否真的有数据

在 Neo4j Browser 中执行：

```cypher
MATCH (n)
RETURN labels(n)[0] AS label, count(n) AS cnt
ORDER BY cnt DESC;
```

正常应该看到 `Job`、`Skill`、`Degree`、`Major`、`Industry` 等节点。

当前仓库已验证过的一次成功导入结果示例：

```text
Industry: 816
Job: 412
Skill: 400
Major: 75
Degree: 8
```

再执行：

```cypher
MATCH ()-[r]->()
RETURN type(r) AS rel_type, count(r) AS cnt
ORDER BY cnt DESC;
```

正常应该看到：

- `REQUIRES_SKILL`
- `REQUIRES_DEGREE`
- `PREFERS_MAJOR`
- `BELONGS_TO_INDUSTRY`
- `PROMOTE_TO`
- `TRANSFER_TO`

当前仓库已验证过的一次成功导入关系结果示例：

```text
BELONGS_TO_INDUSTRY: 1486
REQUIRES_SKILL: 441
PROMOTE_TO: 298
TRANSFER_TO: 279
PREFERS_MAJOR: 105
REQUIRES_DEGREE: 87
```

也可以在 PowerShell 中验证：

```powershell
Set-Location E:\Agent\Agent-main

$envMap = @{}
Get-Content .env.neo4j | ForEach-Object {
  $line = $_.Trim()
  if ($line -and -not $line.StartsWith('#') -and $line.Contains('=')) {
    $parts = $line.Split('=', 2)
    $envMap[$parts[0]] = $parts[1]
  }
}

$user = $envMap['NEO4J_USERNAME']
$password = $envMap['NEO4J_PASSWORD']

docker exec agent-neo4j cypher-shell -u $user -p $password `
  "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt ORDER BY cnt DESC;"
```

如果节点数为 0，说明当前 Neo4j 是空库，需要重新执行：

```powershell
.\scripts\neo4j\import-graph.ps1 -ForceReimport
```

### 6.7 让后端连接 Neo4j

`main_pipeline.py` 默认读取环境变量：

- `NEO4J_URI`
- `NEO4J_USERNAME`
- `NEO4J_PASSWORD`

注意：`.env.neo4j` 不会被 `main_pipeline.py` 自动加载。启动后端或命令行跑主链路前，建议在当前 PowerShell 会话中设置：

```powershell
Set-Location E:\Agent\Agent-main

$envMap = @{}
Get-Content .env.neo4j | ForEach-Object {
  $line = $_.Trim()
  if ($line -and -not $line.StartsWith('#') -and $line.Contains('=')) {
    $parts = $line.Split('=', 2)
    $envMap[$parts[0]] = $parts[1]
  }
}

$env:NEO4J_URI = "bolt://localhost:7687"
$env:NEO4J_USERNAME = $envMap['NEO4J_USERNAME']
$env:NEO4J_PASSWORD = $envMap['NEO4J_PASSWORD']
```

然后再启动后端或运行主链路。

如果你只想使用当前默认密码，也可以直接写：

```powershell
Set-Location E:\Agent\Agent-main

$env:NEO4J_URI = "bolt://localhost:7687"
$env:NEO4J_USERNAME = "neo4j"
$env:NEO4J_PASSWORD = "12345678"
```

### 6.8 快速确认后端能查到 Neo4j

```powershell
Set-Location E:\Agent\Agent-main

conda run -n DL python -c "from db_helper import query_neo4j; import os; print(query_neo4j(os.environ['NEO4J_URI'], os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD'], 'MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt ORDER BY cnt DESC LIMIT 10'))"
```

如果返回 `Job`、`Skill` 等计数，说明后端可以真实查询 Neo4j。

如果返回 `[]` 或打印 `Neo4j 查询失败`，说明后端没有真正连上 Neo4j，此时主链路会降级运行。

## 7. 启动方式

当前仅保留真实大模型模式（会消耗真实 token）。

---

## 8. 真实大模型模式

### 8.1 配置模型

当前后端的大模型配置优先级是：

1. `E:\Agent\Agent-main\llm_interface_layer\local_llm_config.py`
2. 环境变量
3. 代码内默认值

也就是说，如果 `local_llm_config.py` 里填了值，它会优先于环境变量。

你需要配置三个值：

- `LOCAL_LLM_API_BASE_URL`
- `LOCAL_LLM_MODEL`
- `LOCAL_LLM_API_KEY`

示例：

```python
# E:\Agent\Agent-main\llm_interface_layer\local_llm_config.py

LOCAL_LLM_API_BASE_URL = "https://your-openai-compatible-endpoint/v1"
LOCAL_LLM_MODEL = "your-model-name"
LOCAL_LLM_API_KEY = "your-api-key"
```

注意：

- 当前 `llm_client.py` 走的是 OpenAI 兼容的 `/chat/completions` 协议
- 如果你想改用环境变量，请先把 `local_llm_config.py` 里的值清空，否则环境变量不会覆盖它
- 不要把真实 API Key 提交到 Git 仓库

### 8.2 启动后端

```powershell
cd E:\Agent\Agent-main
python -m uvicorn api_server:app --reload --host 127.0.0.1 --port 8000
```

启动后可以访问：

- API 文档：[http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

### 8.3 启动前端

```powershell
cd E:\Agent\frontend
npm run dev
```

前端地址：

- [http://localhost:3000](http://localhost:3000)

说明：

- 前端的 Vite 代理已经配置好，会把 `/api` 请求转发到 `http://localhost:8000`
- 相关配置在 `E:\Agent\frontend\vite.config.ts`

### 8.4 实际使用步骤

1. 打开前端页面
2. 上传一份 `txt / docx / pdf` 简历
3. 后端开始执行整条流水线
4. 等待前端返回解析结果、匹配结果、路径规划和报告

当前前端 API 超时已经调到 5 分钟，位置在：

- `E:\Agent\frontend\src\services\api.ts`

当前后端还额外提供了一个“岗位底库处理接口”：

- `POST /api/data/process`

它不会影响学生侧主链路，主要用于重新构建：

- `outputs/intermediate/*.csv`
- `outputs/sql/jobs.db`
- `outputs/neo4j/*.csv`

### 8.5 真实模型模式下的重要产物

运行后你会看到这些文件更新：

- `E:\Agent\Agent-main\student_api_state.json`
- `E:\Agent\Agent-main\final_report.md`
- `E:\Agent\Agent-main\outputs\cache\llm\*.json`

说明：

- `student_api_state.json`：当前这次前后端联调使用的主状态文件
- `final_report.md`：最后导出的 Markdown 报告
- `outputs/cache/llm`：相同 prompt 的本地缓存，用于减少重复 token 消耗

---

## 9. 岗位底库是否需要重建

通常不需要。

当前仓库里已经带有这些可直接复用的底库产物：

- `E:\Agent\Agent-main\outputs\sql\jobs.db`
- `E:\Agent\Agent-main\outputs\neo4j\*.csv`
- `E:\Agent\Agent-main\outputs\intermediate\*.csv`

如果你只是想跑前后端演示、联调大模型、看职业规划结果，直接复用现有底库即可。

只有在下面这些情况才需要重建：

- 你更换了原始岗位 Excel
- 你修改了岗位清洗规则
- 你修改了岗位去重逻辑
- 你修改了岗位抽取逻辑
- 你修改了岗位晋升路径 / 转岗路径的离线抽取逻辑
- 你想重新生成 SQLite / Neo4j 底库

---

## 10. 重建岗位底库（可选）

以下命令都在 `E:\Agent\Agent-main` 下执行。

### 10.1 一键重建岗位底库（推荐）

现在项目已经支持统一入口：

- 脚本入口：[job_data_pipeline.py](/E:/Agent/Agent-main/job_data_pipeline.py)
- 后端接口：`POST /api/data/process`

推荐优先用脚本入口，因为：

- 日志最完整
- 出错最好排查
- 更适合本机重建底库

直接执行：

```powershell
Set-Location E:\Agent\Agent-main
python .\job_data_pipeline.py `
  --input .\20260226105856_457.xls
```

这条命令会自动串行执行：

1. `data_cleaning.py`
2. `job_dedup.py`
3. `job_extract.py`
4. `export_to_sql.py`
5. `export_to_neo4j.py`

输出结果会统一写入：

- `E:\Agent\Agent-main\outputs\intermediate`
- `E:\Agent\Agent-main\outputs\sql\jobs.db`
- `E:\Agent\Agent-main\outputs\neo4j`

注意：

- 当前岗位晋升路径 / 转岗路径已经前移到离线阶段，主要在 `job_extract.py` 中由大模型分析并沉淀
- 因此如果你想让新的路径图谱真正生效，至少需要重新跑到 `job_extract.py`

### 10.2 通过后端接口触发（可选）

如果你不想在终端里手动执行脚本，也可以在启动后端后，请求：

```text
POST http://127.0.0.1:8000/api/data/process
```

示例：

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/data/process" `
  -Method Post `
  -ContentType "application/json" `
  -Body (@{
    input_file = "20260226105856_457.xls"
    intermediate_dir = "outputs/intermediate"
    sql_db_path = "outputs/sql/jobs.db"
    neo4j_output_dir = "outputs/neo4j"
    max_workers = 4
    group_sample_size = 3
  } | ConvertTo-Json)
```

这个接口适合：

- 后面想在前端或管理端加“一键重建岗位底库”按钮
- 想通过 HTTP 方式统一触发数据处理

如果你只是自己本机重建数据，仍然建议优先使用 `job_data_pipeline.py`。

### 10.3 分步重建（高级用法）

如果你需要逐步排查某一层数据问题，再按下面的单脚本顺序执行。

### 10.3.1 数据清洗

```powershell
Set-Location E:\Agent\Agent-main
python .\job_data\data_cleaning.py `
  --input .\20260226105856_457.xls `
  --output .\outputs\intermediate\jobs_cleaned.csv
```

### 10.3.2 岗位去重 / 标准岗位归一

```powershell
python .\job_data\job_dedup.py `
  --input .\outputs\intermediate\jobs_cleaned.csv `
  --output-data .\outputs\intermediate\jobs_dedup_result.csv `
  --output-mapping .\outputs\intermediate\job_name_mapping.csv `
  --output-pairs .\outputs\intermediate\job_dedup_pairs.csv
```

### 10.3.3 岗位画像抽取与路径关系离线抽取

这一步会调用大模型。

```powershell
python .\job_data\job_extract.py `
  --input .\outputs\intermediate\jobs_dedup_result.csv `
  --output .\outputs\intermediate\jobs_extracted_full.csv `
  --max-workers 4 `
  --group-sample-size 3
```

说明：

- `--max-workers` 越大越快，但更容易触发限流
- `--group-sample-size` 越大，单次 prompt 越长，token 消耗越多
- 当前岗位的 `vertical_paths`、`transfer_paths`、`path_relation_details` 主要在这一步离线生成
- 后面的 SQLite / Neo4j 导出只是把这里已经生成的路径知识写入底库

### 10.3.4 导出到 SQLite

```powershell
python .\job_data\export_to_sql.py `
  --input .\outputs\intermediate\jobs_extracted_full.csv `
  --db-path .\outputs\sql\jobs.db
```

### 10.3.5 导出 Neo4j CSV

```powershell
python .\job_data\export_to_neo4j.py `
  --input .\outputs\intermediate\jobs_extracted_full.csv `
  --output-dir .\outputs\neo4j
```

### 10.3.6 重新导入 Neo4j

```powershell
Set-Location E:\Agent\Agent-main
.\scripts\neo4j\import-graph.ps1 -ForceReimport
```

---

## 11. 当前大模型调用方式总结

当前后端统一通过下面这层调用大模型：

- `E:\Agent\Agent-main\llm_interface_layer\llm_service.py`

调用链是：

```text
业务模块
-> llm_service.py
-> context_builder.py
-> prompt_manager.py
-> llm_client.py
-> OpenAI-compatible /chat/completions
```

目前已经具备：

- task 级 prompt 压缩
- task 级输出 token 预算
- prompt hash 本地缓存

## 12. 常见问题

### 12.1 前端能打开，但上传简历后一直报错

优先检查：

1. 后端是否已经启动在 `8000`
2. 模型配置是否正确
3. 若使用真实模型，API Key 是否有效

### 12.2 PDF 简历解析失败

请安装：

```powershell
python -m pip install pypdf
```

### 12.3 Neo4j 没启动会不会影响运行

不会完全阻塞运行，但会影响：

- 岗位技能要求补充
- 转岗路径
- 晋升路径

### 12.4 为什么报告结果会覆盖上一次结果

因为当前前后端联调用的是单一状态文件：

- `E:\Agent\Agent-main\student_api_state.json`

这适合单用户演示，不适合多用户并发部署。

## 13. 最短跑通路径

如果你只想最快跑起来，推荐按下面顺序：

1. 配置真实大模型 API
2. 启动 Neo4j
3. 启动后端
4. 启动前端
5. 上传简历生成真实结果

---

如果你后续还要把这个项目改成“多用户隔离状态 + 前端可选目标岗位 + 生产部署”，建议在当前 README 的基础上再单独补一份部署文档，而不要把部署细节和本地运行说明混在一起。
