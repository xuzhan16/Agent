"""
local_llm_config.py

本地调试专用的大模型配置文件。
说明：
1. 只建议放在你自己的本机开发环境；
2. 不建议提交到 Git 仓库；
3. 如果这里填写了值，config.py 会优先读取这里的配置。
"""

LOCAL_LLM_API_BASE_URL = "https://api-inference.modelscope.cn/v1"
LOCAL_LLM_MODEL = "deepseek-ai/DeepSeek-V3.2"

# 请把你的真实 API Key 填在这里。
LOCAL_LLM_API_KEY = "ms-eedc3818-45b6-4c66-b909-934307b0697c"
