# Neo4j Docker 导入说明

后端主目录为 `Agent-main/`。以下命令都应在 `E:\Agent\Agent-main` 目录下执行。

## 1. 准备数据
岗位图谱 CSV 已导出到 `outputs/neo4j/`。

## 2. 初始化并导入
在后端目录执行：

```powershell
.\scripts\neo4j\import-graph.ps1
```

脚本会自动：
1. 创建 `.env.neo4j`
2. 生成 Neo4j 登录密码
3. 把 `outputs/neo4j/*.csv` 导入到本地 Neo4j 数据目录
4. 启动 Docker 容器
5. 用 `cypher-shell` 验证导入结果

## 3. 强制重新导入
如果你重新跑了数据处理并想覆盖现有图谱：

```powershell
.\scripts\neo4j\import-graph.ps1 -ForceReimport
```

## 4. 启停服务
启动：

```powershell
docker compose --env-file .env.neo4j -f docker-compose.neo4j.yml up -d
```

停止：

```powershell
docker compose --env-file .env.neo4j -f docker-compose.neo4j.yml down
```

## 5. 访问方式
- Browser: `http://localhost:7474`
- Bolt: `bolt://localhost:7687`

## 6. 前端连接参数
后续网页项目可直接使用：

```env
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=从 .env.neo4j 中读取
```
