import type { ReactNode } from 'react'
import { Steps, Tag } from 'antd'

export type WorkflowStepStatus = 'finish' | 'process' | 'wait' | 'error'

export interface WorkflowStepItem {
  title: ReactNode
  description?: ReactNode
  status?: WorkflowStepStatus
  tag?: ReactNode
}

interface WorkflowStepperProps {
  steps: WorkflowStepItem[]
  current?: number
  compact?: boolean
  className?: string
}

const WorkflowStepper = ({ steps, current = 0, compact = false, className = '' }: WorkflowStepperProps) => (
  <div className={`product-workflow-stepper ${compact ? 'compact' : ''} ${className}`.trim()}>
    <Steps
      current={current}
      responsive
      items={steps.map((step) => ({
        title: (
          <span className="product-workflow-title">
            {step.title}
            {step.tag ? <Tag className="product-workflow-tag">{step.tag}</Tag> : null}
          </span>
        ),
        description: step.description,
        status: step.status,
      }))}
    />
  </div>
)

export default WorkflowStepper
