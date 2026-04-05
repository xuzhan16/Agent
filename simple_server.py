#!/usr/bin/env python3
"""
简单的 HTTP 服务器替代方案，用于测试前端 API 调用
运行方法: python simple_server.py
然后访问 http://localhost:8000/api/resume/parse 等接口
"""

import json
import http.server
import socketserver
from urllib.parse import urlparse, parse_qs
import cgi

PORT = 8000

class MockAPIHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/api/resume/parse':
            self.handle_resume_parse()
        elif self.path == '/api/student/profile':
            self.handle_student_profile()
        elif self.path == '/api/job/match':
            self.handle_job_match()
        elif self.path == '/api/career/path':
            self.handle_career_path()
        elif self.path == '/api/report/generate':
            self.handle_report_generate()
        else:
            self.send_error(404, "Endpoint not found")

    def do_GET(self):
        if self.path.startswith('/api/report/download'):
            self.handle_report_download()
        elif self.path.startswith('/api/report/shared'):
            self.handle_shared_report()
        elif self.path == '/api/report':
            self.handle_get_report()
        else:
            self.send_error(404, "Endpoint not found")

    def handle_resume_parse(self):
        """模拟简历解析接口"""
        # 读取请求体（如果是 multipart/form-data）
        content_type = self.headers.get('Content-Type', '')
        if 'multipart/form-data' in content_type:
            # 简单处理，实际项目中需要更复杂的 multipart 解析
            pass

        response = {
            "success": True,
            "data": {
                "name": "张三",
                "gender": "男",
                "phone": "13800000000",
                "email": "student@example.com",
                "school": "某某大学",
                "major": "计算机科学与技术",
                "degree": "本科",
                "graduation_year": "2026",
                "position": "数据分析师",
                "education": "本科，计算机科学与技术",
                "experience": "参与过课程推荐系统开发与数据分析实习",
                "skills": ["Python", "SQL", "数据分析", "可视化", "机器学习"],
                "certificates": ["CET-6"],
                "project_experience": [
                    {
                        "project_name": "课程推荐系统",
                        "role": "开发",
                        "description": "负责数据处理与模型实验",
                    }
                ],
                "internship_experience": [
                    {
                        "company_name": "某科技公司",
                        "position": "数据分析实习生",
                        "description": "参与报表分析和数据清洗",
                    }
                ],
            }
        }

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    def handle_student_profile(self):
        """模拟学生画像构建接口"""
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        student_info = json.loads(post_data.decode('utf-8'))

        response = {
            "success": True,
            "data": {
                "standard_job_name": "数据分析师",
                "job_category": "数据分析",
                "required_degree": "本科",
                "preferred_majors": [student_info.get('major', '计算机科学'), "统计学", "数学"],
                "required_skills": ["Python", "SQL", "数据分析", "可视化"],
            }
        }

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    def handle_job_match(self):
        """模拟岗位匹配接口"""
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        profile = json.loads(post_data.decode('utf-8'))

        response = {
            "success": True,
            "data": [
                {
                    "job_name": "数据分析师",
                    "company": "量化科技",
                    "match_score": 92.1,
                    "match_level": "A-",
                    "reasons": ["核心技能匹配", "教育背景符合", "项目经验相关"],
                },
                {
                    "job_name": "产品数据分析师",
                    "company": "智慧教育",
                    "match_score": 87.3,
                    "match_level": "B+",
                    "reasons": ["SQL技能强", "数据分析经验", "岗位需求相符"],
                },
                {
                    "job_name": "机器学习工程师",
                    "company": "AI 创新",
                    "match_score": 78.4,
                    "match_level": "C+",
                    "reasons": ["Python熟练", "机器学习基础", "需要更多实习经验"],
                },
            ]
        }

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    def handle_career_path(self):
        """模拟职业路径规划接口"""
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        request_data = json.loads(post_data.decode('utf-8'))

        response = {
            "success": True,
            "data": {
                "primary_target_job": "数据分析师",
                "secondary_target_jobs": ["商业分析师", "BI分析师", "数据产品经理"],
                "goal_positioning": "在补强关键技能后，争取数据分析师岗位",
                "goal_reason": "目前技能与目标岗位高匹配度，且发展路径清晰",
                "direct_path": ["数据分析师", "高级数据分析师", "数据总监"],
                "transition_path": ["初级数据分析员", "数据分析师"],
                "long_term_path": ["数据分析师", "高级数据分析师", "数据总监", "数据副总"],
                "path_strategy": "先补强项目与工具能力，后冲击目标岗位，再逐步晋升",
                "short_term_plan": ["完成两个数据分析项目", "学习Tableau/Power BI", "优化简历与作品集"],
                "mid_term_plan": ["获取数据分析相关实习", "参加数据建模比赛", "提升数据可视化能力"],
                "risk_and_gap": ["实习经验不足", "BI工具掌握有限", "行业经验需要补强"],
                "fallback_strategy": "若直接冲刺数据分析师受阻，可先进入商业分析师或BI分析师岗位积累经验",
            }
        }

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    def handle_report_generate(self):
        """模拟报告生成接口 - 生成优化格式的报告"""
        import os
        from datetime import datetime

        # 获取当前时间
        current_time = datetime.now().strftime("%Y年%m月%d日")

        # 生成优化格式的报告内容 - 纯文本格式，去掉Markdown标记
        report_content = f"""大学生职业规划分析报告

{'=' * 50}

报告生成时间：{current_time}
报告版本：V1.0
分析模型：AI智能匹配算法

{'=' * 50}

学生基本信息

姓名：张三
学校：某某大学
专业：计算机科学与技术
学历：本科
毕业年份：2026
联系方式：13800000000 / student@example.com
目标岗位：数据分析师

{'-' * 50}

能力与经验评估

核心技能掌握情况：
- Python - 熟练
- SQL - 熟练
- 数据分析 - 良好
- 可视化 - 良好
- 机器学习 - 基础

项目经历：
项目名称：课程推荐系统开发项目
角色：主要开发者
职责：数据处理与模型实验
技术栈：Python, Pandas, Scikit-learn
成果：提升推荐准确率15%

实习经历：
公司：某科技公司 - 数据分析实习生
时间：2025.07 - 2025.12
职责：参与报表分析和数据清洗
技能提升：SQL查询优化、数据可视化

{'-' * 50}

岗位匹配分析结果

最佳匹配岗位：数据分析师

匹配维度分析：
技能匹配：95/100 (A+) - Python、SQL技能完全符合岗位要求
教育背景：90/100 (A) - 计算机专业背景高度匹配
项目经验：88/100 (B+) - 相关数据分析项目经验丰富
综合评分：92.1 (A-) - 推荐指数：★★★★★

匹配优势：
- 核心技能完全匹配
- 专业背景高度相关
- 项目经验直接对口

{'-' * 30}

其他推荐岗位：

2. 产品数据分析师 - 智慧教育
匹配分数：87.3 (B+)
优势：SQL技能突出，数据分析经验丰富
建议：可作为备选发展方向

3. 机器学习工程师 - AI创新
匹配分数：78.4 (C+)
优势：Python基础扎实，算法思维良好
建议：需要补充更多机器学习实战经验

{'-' * 50}

职业发展路径规划

核心发展策略：
目标定位：在补强关键技能后，争取数据分析师岗位
成功概率：85% (基于当前技能匹配度)

职业路径推荐：

直接路径（推荐指数：★★★★☆）
初级数据分析师 → 数据分析师 → 高级数据分析师 → 数据总监
     ↑               ↑              ↑              ↑
   0-1年           1-3年          3-5年         5-8年

过渡路径（推荐指数：★★★☆☆）
商业分析师/BI分析师 → 数据分析师 → 高级数据分析师
         ↑                    ↑              ↑
      0-1年                1-3年          3-5年

长期发展路径：
数据分析师 → 高级数据分析师 → 数据总监 → 数据副总/CTO
     ↑              ↑              ↑            ↑
   1-3年          3-5年          5-8年       8-12年

{'-' * 50}

阶段性行动计划

短期计划 (1-3个月)：
必做项目：
- 完成2个数据分析实战项目
- 掌握Tableau/Power BI可视化工具
- 优化简历和作品集展示

技能提升：
- 参加SQL进阶培训
- 学习数据建模方法
- 练习数据分析案例

中期计划 (3-6个月)：
实习就业：
- 申请数据分析相关实习岗位
- 参加校园招聘会
- 准备技术面试

能力拓展：
- 参加数据建模比赛
- 提升数据可视化能力
- 学习行业分析方法

长期规划 (6-12个月)：
- 积累2年以上相关工作经验
- 考取相关专业证书
- 建立个人技术博客/作品集

{'-' * 50}

风险评估与应对策略

主要风险点：

高风险：实习经验不足 - 当前状态：仅6个月实习 - 应对策略：主动申请更多实习机会
中风险：BI工具掌握有限 - 当前状态：基础掌握 - 应对策略：专项培训+项目实践
中风险：行业经验欠缺 - 当前状态：缺乏深度 - 应对策略：参加行业活动+学习

备选策略：
如果直接冲刺数据分析师受阻：
1. 商业分析师路径：从业务分析入手，逐步转向技术分析
2. BI分析师路径：专注数据可视化和报表开发
3. 数据工程师路径：加强数据处理和ETL技能

{'-' * 50}

专业建议与指导

核心建议：

1. 技能强化优先级：
最高优先级：
- SQL查询优化
- 数据可视化
- 业务理解能力

中等优先级：
- Python数据分析
- 统计学基础
- 机器学习算法

普通优先级：
- 大数据技术
- 云平台技能
- 专业认证

2. 简历优化建议：
- 突出项目成果：量化展示项目贡献
- 技能标签化：清晰列出技术栈
- 成果导向：用数据说话

3. 求职策略：
- 目标明确：优先数据分析师岗位
- 广撒网：同时关注相关职位
- 主动出击：多参加招聘会和技术交流

后续行动计划：

第1个月：
- 完成简历优化
- 启动项目实践
- 报名技能培训

第2-3个月：
- 投递实习申请
- 准备技术面试
- 完善作品集

第4-6个月：
- 争取实习机会
- 参加行业活动
- 持续技能提升

成功关键指标：
- 掌握3+数据分析工具
- 完成5+实战项目
- 获得1+相关实习经历
- 通过3+技术面试

{'-' * 50}

联系与支持

职业规划顾问：AI智能分析系统
报告有效期：6个月
建议复盘周期：每3个月更新一次分析

相信自己，你已经具备了成功的基础条件！通过系统性的努力，你一定能在数据分析领域找到理想的职位。

{'=' * 50}

本报告由AI智能分析系统生成，仅供参考。如需深度咨询，请联系专业职业规划师。"""

        # 保存报告到文件
        os.makedirs('career_path_plan/outputs/reports', exist_ok=True)
        report_path = 'career_path_plan/outputs/reports/career_planning_report.txt'

        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            print(f"[report_generate] 报告已生成并保存到: {report_path}")
        except Exception as e:
            print(f"[report_generate] 保存报告失败: {e}")

        response = {
            "success": True,
            "data": "career_planning_report.pdf"
        }

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    def handle_get_report(self):
        """获取生成的报告文本内容"""
        import os
        
        report_path = 'career_path_plan/outputs/reports/career_planning_report.txt'
        
        # 检查文件是否存在
        if not os.path.exists(report_path):
            response = {
                "success": False,
                "message": f"找不到报告文件，请先生成报告"
            }
        else:
            try:
                # 读取报告文件内容
                with open(report_path, 'r', encoding='utf-8') as f:
                    report_content = f.read()
                response = {
                    "success": True,
                    "data": report_content
                }
                print(f"[get_report] 成功读取报告，内容长度: {len(report_content)}")
            except Exception as e:
                print(f"[get_report] 读取报告错误: {e}")
                response = {
                    "success": False,
                    "message": f"读取报告文件失败: {str(e)}"
                }

        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    def handle_shared_report(self):
        """获取共享报告内容"""
        import os
        
        report_path = 'career_path_plan/outputs/reports/career_planning_report.txt'
        response = {
            "success": False,
            "message": "找不到共享报告文件，请先生成报告"
        }

        if os.path.exists(report_path):
            try:
                with open(report_path, 'r', encoding='utf-8') as f:
                    report_content = f.read()
                response = {
                    "success": True,
                    "data": report_content
                }
                print(f"[shared_report] 成功读取共享报告，内容长度: {len(report_content)}")
            except Exception as e:
                print(f"[shared_report] 读取报告错误: {e}")
                response = {
                    "success": False,
                    "message": f"读取共享报告失败: {str(e)}"
                }

        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    def handle_report_download(self):
        """处理报告下载请求"""
        import os
        from urllib.parse import urlparse, parse_qs

        try:
            # 解析查询参数
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)
            file_name = query_params.get('file_name', ['career_planning_report.txt'])[0]

            # 报告文件路径 - 相对于 simple_server.py 的位置
            base_dir = os.path.dirname(os.path.abspath(__file__))
            report_file_path = os.path.join(base_dir, 'career_path_plan', 'outputs', 'reports', 'career_planning_report.txt')

            print(f"[下载] 请求文件: {file_name}")
            print(f"[下载] 实际路径: {report_file_path}")
            print(f"[下载] 文件存在: {os.path.exists(report_file_path)}")

            if not os.path.exists(report_file_path):
                print(f"[下载] 错误：文件不存在")
                self.send_response(404)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                error_msg = f"找不到报告文件。路径: {report_file_path}"
                self.wfile.write(error_msg.encode('utf-8'))
                return

            # 读取文件内容 - 尝试多种编码
            content = None
            encodings = ['utf-8-sig', 'utf-8', 'gbk', 'utf-16']
            
            for encoding in encodings:
                try:
                    with open(report_file_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    print(f"[下载] 成功以 {encoding} 编码读取文件")
                    break
                except (UnicodeDecodeError, UnicodeError) as e:
                    print(f"[下载] {encoding} 编码失败: {str(e)}")
                    continue

            if content is None:
                # 如果文本编码都失败，以二进制模式读取
                print(f"[下载] 切换到二进制模式读取")
                with open(report_file_path, 'rb') as f:
                    raw_bytes = f.read()
                # 尝试解码
                for encoding in encodings:
                    try:
                        content = raw_bytes.decode(encoding)
                        print(f"[下载] 二进制解码成功: {encoding}")
                        break
                    except:
                        pass
                
                if content is None:
                    # 最后的办法：忽略错误字符
                    content = raw_bytes.decode('utf-8', errors='ignore')
                    print(f"[下载] 使用容错解码")

            print(f"[下载] 成功读取文件，大小: {len(content)} 字符")

            # 设置响应头
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Content-Disposition', f'attachment; filename="{file_name}"')
            self.send_header('Content-Length', str(len(content.encode('utf-8'))))
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type')
            self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
            self.end_headers()

            # 发送文件内容
            self.wfile.write(content.encode('utf-8'))
            print(f"[下载] 文件已发送完毕")

        except Exception as e:
            print(f"[下载] 异常: {type(e).__name__}: {str(e)}")
            import traceback
            traceback.print_exc()
            try:
                self.send_response(500)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                error_msg = f"服务器错误: {str(e)}"
                self.wfile.write(error_msg.encode('utf-8', errors='ignore'))
            except:
                pass


    def do_OPTIONS(self):
        """处理预检请求"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

if __name__ == "__main__":
    with socketserver.TCPServer(("", PORT), MockAPIHandler) as httpd:
        print(f"Mock API Server running on port {PORT}")
        print("Available endpoints:")
        print("  POST /api/resume/parse")
        print("  POST /api/student/profile")
        print("  POST /api/job/match")
        print("  POST /api/career/path")
        print("  POST /api/report/generate")
        print("  GET  /api/report")
        print("\nPress Ctrl+C to stop")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped")