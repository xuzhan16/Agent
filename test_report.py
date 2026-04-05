import requests
import json

# 测试完整流程
print('=== 职业规划报告生成测试 ===')

# 1. 生成报告
print('\n1. 生成报告...')
response = requests.post('http://localhost:8000/api/report/generate',
                        json={'test': 'data'},
                        headers={'Content-Type': 'application/json'})

print(f'状态码: {response.status_code}')
if response.status_code == 200:
    data = response.json()
    print(f'响应: {data}')
    if data.get('success'):
        filename = data['data']
        print(f'✅ 报告生成成功: {filename}')
    else:
        message = data.get('message', '未知错误')
        print(f'❌ 报告生成失败: {message}')
else:
    print(f'❌ 请求失败: {response.status_code}')

# 2. 获取报告内容
print('\n2. 获取报告内容...')
response2 = requests.get('http://localhost:8000/api/report')
print(f'状态码: {response2.status_code}')

if response2.status_code == 200:
    data2 = response2.json()
    if data2.get('success'):
        content = data2['data']
        print(f'✅ 报告内容获取成功，长度: {len(content)} 字符')

        # 检查内容是否包含优化格式的元素
        checks = [
            ('标题', '# 📊 大学生职业规划分析报告' in content),
            ('表格', '| 项目 | 内容 |' in content),
            ('表情符号', '✅' in content),
            ('结构化内容', '## 👤 学生基本信息' in content),
            ('职业规划', '## 📈 岗位匹配分析结果' in content),
        ]

        print('\n内容格式检查:')
        for name, result in checks:
            status = '✅' if result else '❌'
            print(f'  {status} {name}: {"通过" if result else "失败"}')

        # 显示报告开头部分
        print('\n报告开头预览:')
        lines = content.split('\n')[:20]
        for i, line in enumerate(lines, 1):
            print(f'{i:2d}: {line}')

    else:
        message2 = data2.get('message', '未知错误')
        print(f'❌ 获取失败: {message2}')
else:
    print(f'❌ 请求失败: {response2.status_code}')

print('\n=== 测试完成 ===')