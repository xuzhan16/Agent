# 下载报告功能诊断指南

## 快速诊断步骤

### 1️⃣ 打开浏览器开发者工具
- **Windows**: 按 `F12` 或 `Ctrl+Shift+I`
- **Mac**: 按 `Cmd+Option+I`
- 切换到 **Console** 选项卡

### 2️⃣ 清空控制台
在控制台输入：
```javascript
console.clear()
```

### 3️⃣ 生成报告
1. 进入"报告生成"页面
2. 点击"生成报告"按钮
3. **在控制台中观察日志输出**

你应该看到以下日志：
```
[Report] handleGenerateReport called
[Report] studentInfo: {...}
[Report] studentProfile: {...}
[Report] jobMatches length: 3
[Report] careerPath: {...}
[Report] Sending generateReport request with payload: {...}
[Report] generateReport response: {success: true, data: "career_planning_report.txt", ...}
[Report] Report generated successfully, filename: career_planning_report.txt
[Report] getReport response: {success: true, data: "# 大学生职业规划分析报告\n\n..."}
[Report] Report content loaded, length: 2345
```

### 4️⃣ 尝试下载报告
1. 确保 **下载格式** 选择为 `PDF`
2. 点击 **"下载报告"** 按钮
3. **在控制台中观察日志输出**

你应该看到以下日志：
```
[Report] downloadReport called, reportFileName: career_planning_report.txt
[Report] Starting download with format: pdf
[Report] Content length: 2345
[Report] Generating PDF
[Report] PDF generated, size: 12345
[Report] Triggering download: career_planning_report.pdf
[Report] Download triggered successfully
[Report] Download cleanup completed
```

## 常见问题排查

### ❌ 问题 1：生成报告时出错
**症状**：点击"生成报告"后没有反应或显示错误

**排查步骤**：
1. 查看是否完成了所有前置步骤（简历解析 → 学生画像 → 岗位匹配 → 职业规划）
2. 检查控制台是否有红色错误信息
3. 查看后端日志（运行 simple_server.py 的终端）

**解决方案**：
- 重新填写简历和返回上一步重新执行
- 检查后端服务是否正常 (`netstat -ano | findstr :8000`)

---

### ❌ 问题 2：生成成功但下载按钮灰色
**症状**：报告生成成功，但"下载报告"按钮仍为灰色（禁用状态）

**排查步骤**：
1. 查看 `reportFileName` 是否为空：
```javascript
// 在控制台可以查看页面状态（需要访问 React 状态，比较困难）
```
2. 检查生成报告的响应中 `data` 字段是否为空

**解决方案**：
- 重新生成报告
- 如果问题持续，检查后端 `/api/report/generate` 端点

---

### ❌ 问题 3：点击下载无反应
**症状**：点击"下载报告"按钮，按钮显示"下载中..."然后恢复，但没有文件下载

**排查步骤**：
1. 检查控制台是否有以下日志：
   - `[Report] downloadReport called`
   - `[Report] Triggering download`
2. 查看是否有红色错误信息
3. 检查浏览器下载设置（可能被拦截）

**解决方案**：
- 检查浏览器下载是否被阻止
- 查看 Windows 防火墙/杀毒软件是否拦截了下载
- 尝试不同的下载格式（PDF → HTML → TXT）

---

### ❌ 问题 4：下载的 PDF 无法打开
**症状**：下载成功但 PDF 文件无法用 PDF 阅读器打开

**排查步骤**：
1. 查看下载的文件大小是否为 0 字节
2. 用文本编辑器打开（如 Notepad），查看是否有内容
3. 检查控制台是否显示 `PDF generated, size: 12345`

**解决方案**：
- 尝试使用 `HTML` 或 `TXT` 格式
- 检查报告内容是否正确生成

---

## 网络请求排查

### 检查 API 调用

在浏览器开发者工具中：
1. 切换到 **Network** 选项卡
2. 重新生成报告
3. 查看是否有 `/api/report/generate` 的请求
4. 检查响应状态码和内容

**正常响应示例**：
```json
{
  "success": true,
  "data": "career_planning_report.txt",
  "message": "报告生成成功"
}
```

---

## 完整的调试过程示例

```javascript
// 1. 打开控制台，清空日志
console.clear()

// 2. 生成报告 - 观察所有 [Report] 日志
// 应该看到约 10 条日志

// 3. 点击下载 - 观察所有下载相关日志  
// 应该看到约 8 条日志

// 4. 如果没有看到日志，说明函数未被调用
// 检查按钮是否真的被点击了
```

---

## 获取完整的错误堆栈

如果出现错误，复制整个错误信息并分享：

```javascript
// 错误会显示详细的堆栈跟踪，例如：
// Error: Failed to create Object URL
//   at downloadReport (Report.tsx:123)
//   at HTMLButtonElement.onclick
```

---

## 后端日志检查

查看运行 `simple_server.py` 的终端，检查是否有任何错误信息关于报告生成或下载。

---

**需要帮助？** 将以上诊断步骤的结果告诉我，我可以更准确地定位问题。
