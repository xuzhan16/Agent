// 工具函数

// 格式化匹配分数
export const formatMatchScore = (score: number): string => {
  return `${(score * 100).toFixed(1)}%`
}

// 获取匹配等级对应的颜色
export const getMatchLevelColor = (level: string): string => {
  switch (level.toLowerCase()) {
    case 'a+':
    case 'a':
      return '#52c41a' // 绿色
    case 'b+':
    case 'b':
      return '#1890ff' // 蓝色
    case 'c+':
    case 'c':
      return '#faad14' // 橙色
    case 'd+':
    case 'd':
      return '#ff4d4f' // 红色
    default:
      return '#d9d9d9' // 灰色
  }
}

// 格式化日期
export const formatDate = (date: string): string => {
  return new Date(date).toLocaleDateString('zh-CN')
}

// 截断文本
export const truncateText = (text: string, maxLength: number): string => {
  if (text.length <= maxLength) return text
  return text.substring(0, maxLength) + '...'
}

// 验证邮箱格式
export const isValidEmail = (email: string): boolean => {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
  return emailRegex.test(email)
}

// 验证手机号格式
export const isValidPhone = (phone: string): boolean => {
  const phoneRegex = /^1[3-9]\d{9}$/
  return phoneRegex.test(phone)
}