import requests
import json

# 测试新的纯文本格式报告
print('=== 测试纯文本格式报告 ===')

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

        # 检查是否去掉了Markdown格式符号
        checks = [
            ('无Markdown标题', not content.startswith('#')),
            ('无星号强调', '**' not in content),
            ('无表情符号', '📊' not in content and '👤' not in content),
            ('无表格分隔符', '|---' not in content),
            ('纯文本格式', content.split('\n')[0] == '大学生职业规划分析报告'),
        ]

        print('\n纯文本格式检查:')
        for name, result in checks:
            status = '✅' if result else '❌'
            print(f'  {status} {name}: {"通过" if result else "失败"}')

        # 显示报告开头部分
        print('\n报告开头预览:')
        lines = content.split('\n')[:15]
        for i, line in enumerate(lines, 1):
            print(f'{i:2d}: {line}')

    else:
        message2 = data2.get('message', '未知错误')
        print(f'❌ 获取失败: {message2}')
else:
    print(f'❌ 请求失败: {response2.status_code}')

print('\n=== 测试完成 ===')