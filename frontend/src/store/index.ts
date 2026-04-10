import { create } from 'zustand'
import { StudentInfo, StudentProfileResult, JobMatchResult, CareerPathResult } from '../types'

interface CareerState {
  // 学生信息
  studentInfo: StudentInfo | null
  setStudentInfo: (info: StudentInfo) => void

  // 学生画像
  studentProfile: StudentProfileResult | null
  setStudentProfile: (profile: StudentProfileResult) => void

  // 岗位匹配结果
  jobMatches: JobMatchResult[]
  setJobMatches: (matches: JobMatchResult[]) => void

  // 职业路径规划结果
  careerPath: CareerPathResult | null
  setCareerPath: (path: CareerPathResult) => void

  // 加载状态
  loading: boolean
  setLoading: (loading: boolean) => void

  // 错误信息
  error: string | null
  setError: (error: string | null) => void

  // 重置状态
  reset: () => void
}

const initialState = {
  studentInfo: null,
  studentProfile: null,
  jobMatches: [],
  careerPath: null,
  loading: false,
  error: null,
}

export const useCareerStore = create<CareerState>((set) => ({
  ...initialState,

  setStudentInfo: (info) => set({ studentInfo: info }),
  setStudentProfile: (profile) => set({ studentProfile: profile }),
  setJobMatches: (matches) => set({ jobMatches: matches }),
  setCareerPath: (path) => set({ careerPath: path }),
  setLoading: (loading) => set({ loading }),
  setError: (error) => set({ error }),

  reset: () => set(initialState),
}))
