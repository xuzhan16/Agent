import { useDeferredValue, useEffect, useMemo, useRef, useState } from 'react'
import G6 from '@antv/g6'
import {
  Alert,
  Button,
  Card,
  Col,
  Empty,
  Input,
  Radio,
  Row,
  Space,
  Spin,
  Statistic,
  Tag,
  Tooltip,
} from 'antd'
import {
  AimOutlined,
  ApartmentOutlined,
  BranchesOutlined,
  ReloadOutlined,
  SearchOutlined,
  ShareAltOutlined,
} from '@ant-design/icons'
import { careerApi } from '../services/api'
import type { JobPathGraphEdge, JobPathGraphNode, JobPathGraphResponse } from '../types'
import '../styles/JobPathGraph.css'

type RelationFilter = 'all' | 'PROMOTE_TO' | 'TRANSFER_TO'
type GraphScope = 'curated' | 'all'
type Selection =
  | { type: 'node'; item: JobPathGraphNode }
  | { type: 'edge'; item: JobPathGraphEdge }
  | null

const safeNodes = (value?: JobPathGraphNode[] | null) => (Array.isArray(value) ? value : [])
const safeEdges = (value?: JobPathGraphEdge[] | null) => (Array.isArray(value) ? value : [])
const safeText = (value?: string | number | null) => (value === undefined || value === null || value === '' ? '暂无数据' : String(value))
const percentText = (value?: string | number | null) => (value === undefined || value === null || value === '' ? '' : String(value))

const relationMeta = {
  PROMOTE_TO: {
    label: '晋升',
    color: '#22c55e',
    tagColor: 'green',
    lineDash: undefined as number[] | undefined,
  },
  TRANSFER_TO: {
    label: '转岗',
    color: '#f59e0b',
    tagColor: 'orange',
    lineDash: [6, 5] as number[] | undefined,
  },
}

const getRelationMeta = (relation?: string) => {
  if (relation === 'PROMOTE_TO' || relation === 'TRANSFER_TO') {
    return relationMeta[relation]
  }
  return {
    label: relation || '关系',
    color: '#64748b',
    tagColor: 'default',
    lineDash: undefined,
  }
}

const buildNodeRelationSummary = (edges: JobPathGraphEdge[]) => {
  const summary = new Map<string, {
    inEdges: JobPathGraphEdge[]
    outEdges: JobPathGraphEdge[]
    promoteOut: number
    transferOut: number
  }>()

  edges.forEach((edge) => {
    const source = edge.source
    const target = edge.target
    if (!summary.has(source)) {
      summary.set(source, { inEdges: [], outEdges: [], promoteOut: 0, transferOut: 0 })
    }
    if (!summary.has(target)) {
      summary.set(target, { inEdges: [], outEdges: [], promoteOut: 0, transferOut: 0 })
    }
    const sourceSummary = summary.get(source)
    const targetSummary = summary.get(target)
    sourceSummary?.outEdges.push(edge)
    targetSummary?.inEdges.push(edge)
    if (edge.relation === 'PROMOTE_TO') {
      if (sourceSummary) sourceSummary.promoteOut += 1
    }
    if (edge.relation === 'TRANSFER_TO') {
      if (sourceSummary) sourceSummary.transferOut += 1
    }
  })

  return summary
}

const getNodeColor = (nodeId: string, relationSummary: ReturnType<typeof buildNodeRelationSummary>) => {
  const summary = relationSummary.get(nodeId)
  if (!summary) {
    return {
      fill: '#f8fafc',
      stroke: '#cbd5e1',
    }
  }
  if (summary.promoteOut > 0) {
    return {
      fill: '#eff6ff',
      stroke: '#6366f1',
    }
  }
  if (summary.transferOut > 0) {
    return {
      fill: '#ecfeff',
      stroke: '#06b6d4',
    }
  }
  return {
    fill: '#f0f9ff',
    stroke: '#93c5fd',
  }
}

const buildGraphData = (
  nodes: JobPathGraphNode[],
  edges: JobPathGraphEdge[],
  searchText: string,
) => {
  const relationSummary = buildNodeRelationSummary(edges)
  const normalizedSearch = searchText.trim().toLowerCase()
  const connectedNodeIds = new Set(edges.flatMap((edge) => [edge.source, edge.target]))
  const graphNodes = nodes
    .filter((node) => connectedNodeIds.has(node.id))
    .map((node) => {
      const isSearchHit = normalizedSearch
        ? `${node.id} ${node.label || ''}`.toLowerCase().includes(normalizedSearch)
        : false
      const colors = getNodeColor(node.id, relationSummary)
      return {
        ...node,
        id: node.id,
        label: node.label || node.id,
        size: [148, 40],
        style: {
          radius: 12,
          fill: isSearchHit ? '#fef9c3' : colors.fill,
          stroke: isSearchHit ? '#f59e0b' : colors.stroke,
          lineWidth: isSearchHit ? 2.5 : 1.5,
          shadowColor: 'rgba(15, 23, 42, 0.12)',
          shadowBlur: isSearchHit ? 16 : 8,
        },
        labelCfg: {
          style: {
            fill: '#0f172a',
            fontSize: 12,
            fontWeight: 600,
          },
        },
      }
    })

  const graphEdges = edges.map((edge) => {
    const meta = getRelationMeta(edge.relation)
    const isSearchRelated = normalizedSearch
      ? `${edge.source} ${edge.target}`.toLowerCase().includes(normalizedSearch)
      : false
    return {
      ...edge,
      id: edge.id,
      source: edge.source,
      target: edge.target,
      label: meta.label,
      style: {
        stroke: meta.color,
        lineWidth: isSearchRelated ? 2.5 : 1.4,
        lineDash: meta.lineDash,
        endArrow: {
          path: G6.Arrow.triangle(8, 10, 0),
          fill: meta.color,
        },
      },
      labelCfg: {
        autoRotate: true,
        style: {
          fill: meta.color,
          fontSize: 11,
          background: {
            fill: '#ffffff',
            padding: [2, 4, 2, 4],
            radius: 4,
          },
        },
      },
    }
  })

  return {
    nodes: graphNodes,
    edges: graphEdges,
  }
}

const GraphLegend = () => (
  <div className="job-path-legend">
    <div><span className="legend-dot legend-promotion" /> 有晋升出边岗位</div>
    <div><span className="legend-dot legend-transfer" /> 有转岗出边岗位</div>
    <div><span className="legend-line legend-promote-line" /> 晋升关系</div>
    <div><span className="legend-line legend-transfer-line" /> 转岗关系</div>
  </div>
)

const NodeDetail = ({
  node,
  edges,
}: {
  node: JobPathGraphNode
  edges: JobPathGraphEdge[]
}) => {
  const incoming = edges.filter((edge) => edge.target === node.id)
  const outgoing = edges.filter((edge) => edge.source === node.id)

  return (
    <div className="graph-detail-content">
      <Tag color="blue">岗位节点</Tag>
      <h3>{node.label || node.id}</h3>
      <p><b>岗位类别：</b>{safeText(node.job_category)}</p>
      <p><b>岗位层级：</b>{safeText(node.job_level)}</p>
      <p><b>学历要求：</b>{safeText(node.degree_requirement)}</p>
      <p><b>专业要求：</b>{(node.major_requirement || []).length > 0 ? node.major_requirement?.join('、') : '暂无数据'}</p>
      <div className="detail-stat-row">
        <span>出边关系 <b>{outgoing.length}</b></span>
        <span>入边关系 <b>{incoming.length}</b></span>
      </div>
      <div className="detail-mini-list">
        <h4>相关出边</h4>
        {outgoing.length > 0 ? outgoing.slice(0, 8).map((edge) => {
          const meta = getRelationMeta(edge.relation)
          return <Tag key={edge.id} color={meta.tagColor}>{meta.label} → {edge.target}</Tag>
        }) : <span className="muted-text">暂无出边</span>}
      </div>
      <div className="detail-mini-list">
        <h4>相关入边</h4>
        {incoming.length > 0 ? incoming.slice(0, 8).map((edge) => {
          const meta = getRelationMeta(edge.relation)
          return <Tag key={edge.id} color={meta.tagColor}>{edge.source} → {meta.label}</Tag>
        }) : <span className="muted-text">暂无入边</span>}
      </div>
    </div>
  )
}

const EdgeDetail = ({ edge }: { edge: JobPathGraphEdge }) => {
  const meta = getRelationMeta(edge.relation)
  return (
    <div className="graph-detail-content">
      <Tag color={meta.tagColor}>{meta.label}</Tag>
      <h3>{edge.source_name || edge.source}</h3>
      <div className="edge-arrow-detail">→</div>
      <h3>{edge.target_name || edge.target}</h3>
      <p><b>关系类型：</b>{edge.relation}</p>
      <p><b>关系说明：</b>{safeText(edge.reason)}</p>
      <p><b>置信度：</b>{percentText(edge.confidence) || '暂无数据'}</p>
      <p className="muted-text">该关系来自 Neo4j 或本地图谱 CSV 中真实存在的路径关系，不由前端或 LLM 生成。</p>
    </div>
  )
}

const PathList = ({
  title,
  edges,
}: {
  title: string
  edges: JobPathGraphEdge[]
}) => (
  <Card className="job-path-list-card" title={title}>
    <div className="path-list-scroll">
      {edges.length > 0 ? edges.slice(0, 60).map((edge) => {
        const meta = getRelationMeta(edge.relation)
        return (
          <div className="path-list-item" key={edge.id}>
            <Tag color={meta.tagColor}>{meta.label}</Tag>
            <span>{edge.source}</span>
            <span className="path-list-arrow">→</span>
            <span>{edge.target}</span>
          </div>
        )
      }) : (
        <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无关系" />
      )}
    </div>
  </Card>
)

const JobPathGraph = () => {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const graphRef = useRef<any>(null)
  const [graphData, setGraphData] = useState<JobPathGraphResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [graphScope, setGraphScope] = useState<GraphScope>('curated')
  const [relationFilter, setRelationFilter] = useState<RelationFilter>('all')
  const [searchText, setSearchText] = useState('')
  const deferredSearchText = useDeferredValue(searchText)
  const [selection, setSelection] = useState<Selection>(null)

  const loadGraph = () => {
    setLoading(true)
    setError('')
    careerApi.getJobPathGraph(graphScope)
      .then((response) => {
        if (response.success) {
          setGraphData(response.data || null)
        } else {
          setError(response.message || '岗位路径图谱读取失败')
          setGraphData(response.data || null)
        }
      })
      .catch((err) => {
        console.error('[JobPathGraph] load failed:', err)
        setError('岗位路径图谱接口请求失败，请确认后端服务是否启动。')
      })
      .finally(() => {
        setLoading(false)
      })
  }

  useEffect(() => {
    loadGraph()
  }, [graphScope])

  const nodes = safeNodes(graphData?.nodes)
  const allEdges = safeEdges(graphData?.edges)
  const filteredEdges = useMemo(() => {
    if (relationFilter === 'all') {
      return allEdges
    }
    return allEdges.filter((edge) => edge.relation === relationFilter)
  }, [allEdges, relationFilter])

  const visibleNodeIds = useMemo(() => new Set(filteredEdges.flatMap((edge) => [edge.source, edge.target])), [filteredEdges])
  const filteredNodes = useMemo(() => nodes.filter((node) => visibleNodeIds.has(node.id)), [nodes, visibleNodeIds])

  const g6Data = useMemo(
    () => buildGraphData(filteredNodes, filteredEdges, deferredSearchText),
    [filteredNodes, filteredEdges, deferredSearchText],
  )

  const selectedNode = selection?.type === 'node' ? selection.item : null
  const selectedEdge = selection?.type === 'edge' ? selection.item : null

  useEffect(() => {
    if (!containerRef.current) {
      return
    }

    if (graphRef.current) {
      graphRef.current.destroy()
      graphRef.current = null
    }

    if (!g6Data.nodes.length || !g6Data.edges.length) {
      return
    }

    const graph = new G6.Graph({
      container: containerRef.current,
      width: containerRef.current.clientWidth || 760,
      height: Math.max(containerRef.current.clientHeight || 620, 560),
      fitView: true,
      fitViewPadding: 30,
      animate: true,
      layout: {
        type: 'force',
        preventOverlap: true,
        linkDistance: 180,
        nodeStrength: -90,
        edgeStrength: 0.08,
      },
      modes: {
        default: ['drag-canvas', 'zoom-canvas', 'drag-node'],
      },
      defaultNode: {
        type: 'rect',
        size: [148, 40],
      },
      defaultEdge: {
        type: 'quadratic',
      },
      nodeStateStyles: {
        selected: {
          stroke: '#2563eb',
          lineWidth: 3,
          shadowBlur: 18,
          shadowColor: 'rgba(37, 99, 235, 0.25)',
        },
        dim: {
          opacity: 0.24,
        },
      },
      edgeStateStyles: {
        selected: {
          lineWidth: 3,
          shadowBlur: 12,
          shadowColor: 'rgba(15, 23, 42, 0.18)',
        },
        dim: {
          opacity: 0.18,
        },
      },
    })

    graph.data(g6Data)
    graph.render()
    graph.fitView(30)

    graph.on('node:click', (evt: any) => {
      const item = evt.item
      const model = item?.getModel?.()
      if (!model) return
      graph.getNodes().forEach((nodeItem: any) => graph.clearItemStates(nodeItem))
      graph.getEdges().forEach((edgeItem: any) => graph.clearItemStates(edgeItem))
      graph.setItemState(item, 'selected', true)
      graph.focusItem(item, true, {
        easing: 'easeCubic',
        duration: 400,
      })
      setSelection({ type: 'node', item: model as JobPathGraphNode })
    })

    graph.on('edge:click', (evt: any) => {
      const item = evt.item
      const model = item?.getModel?.()
      if (!model) return
      graph.getNodes().forEach((nodeItem: any) => graph.clearItemStates(nodeItem))
      graph.getEdges().forEach((edgeItem: any) => graph.clearItemStates(edgeItem))
      graph.setItemState(item, 'selected', true)
      setSelection({ type: 'edge', item: model as JobPathGraphEdge })
    })

    graph.on('canvas:click', () => {
      graph.getNodes().forEach((nodeItem: any) => graph.clearItemStates(nodeItem))
      graph.getEdges().forEach((edgeItem: any) => graph.clearItemStates(edgeItem))
      setSelection(null)
    })

    const handleResize = () => {
      if (!containerRef.current || graph.get('destroyed')) return
      graph.changeSize(containerRef.current.clientWidth || 760, Math.max(containerRef.current.clientHeight || 620, 560))
      graph.fitView(30)
    }
    window.addEventListener('resize', handleResize)
    graphRef.current = graph

    return () => {
      window.removeEventListener('resize', handleResize)
      graph.destroy()
      graphRef.current = null
    }
  }, [g6Data])

  const handleResetView = () => {
    setSearchText('')
    setRelationFilter('all')
    setSelection(null)
    if (graphRef.current) {
      graphRef.current.getNodes().forEach((nodeItem: any) => graphRef.current.clearItemStates(nodeItem))
      graphRef.current.getEdges().forEach((edgeItem: any) => graphRef.current.clearItemStates(edgeItem))
      graphRef.current.fitView(30)
    }
  }

  const graphStatus = graphData?.graph_status || 'unavailable'
  const source = graphData?.source || 'none'
  const scope = graphData?.graph_scope || graphScope
  const stats = graphData?.stats || {}
  const promotionEdges = filteredEdges.filter((edge) => edge.relation === 'PROMOTE_TO')
  const transferEdges = filteredEdges.filter((edge) => edge.relation === 'TRANSFER_TO')

  return (
    <div className="job-path-graph-page">
      <section className="job-path-hero">
        <div>
          <Tag color={source === 'neo4j' ? 'blue' : source === 'csv_fallback' ? 'gold' : 'default'}>
            数据来源：{source === 'neo4j' ? 'Neo4j' : source === 'csv_fallback' ? 'CSV fallback' : '暂无'}
          </Tag>
          <Tag color={scope === 'curated' ? 'green' : 'purple'}>
            {scope === 'curated' ? '精选计算机岗位图谱' : '全部原始图谱'}
          </Tag>
          <h1><ApartmentOutlined /> 岗位路径知识图谱</h1>
          <p>
            基于 Neo4j 中真实 PROMOTE_TO / TRANSFER_TO 关系构建。默认精选图谱用于赛题展示，过滤明显非计算机或低质量岗位名；全部图谱保留原始 Neo4j 数据。
          </p>
        </div>
        <Button icon={<ReloadOutlined />} onClick={loadGraph} loading={loading}>
          重新加载
        </Button>
      </section>

      {error && (
        <Alert className="job-path-alert" message="图谱加载异常" description={error} type="error" showIcon />
      )}

      {graphData?.message && (
        <Alert
          className="job-path-alert"
          message={graphStatus === 'available' ? '岗位路径图谱已加载' : '岗位路径图谱暂无可展示关系'}
          description={graphData.message}
          type={graphStatus === 'available' ? 'success' : 'warning'}
          showIcon
        />
      )}

      {graphData?.filter_notes?.length ? (
        <Alert
          className="job-path-alert"
          message="图谱展示口径说明"
          description={graphData.filter_notes.join('；')}
          type="info"
          showIcon
        />
      ) : null}

      <Row gutter={[16, 16]} className="job-path-stats">
        <Col xs={12} md={6}>
          <Card><Statistic title="岗位节点数" value={stats.job_node_count || 0} prefix={<AimOutlined />} /></Card>
        </Col>
        <Col xs={12} md={6}>
          <Card><Statistic title="晋升关系数" value={stats.promote_edge_count || 0} prefix={<BranchesOutlined />} /></Card>
        </Col>
        <Col xs={12} md={6}>
          <Card><Statistic title="转岗关系数" value={stats.transfer_edge_count || 0} prefix={<ShareAltOutlined />} /></Card>
        </Col>
        <Col xs={12} md={6}>
          <Card><Statistic title="总关系数" value={stats.total_edge_count || 0} /></Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} className="job-path-stats">
        <Col xs={12} md={6}>
          <Card><Statistic title="原始节点数" value={graphData?.raw_node_count || stats.job_node_count || 0} /></Card>
        </Col>
        <Col xs={12} md={6}>
          <Card><Statistic title="原始关系数" value={graphData?.raw_edge_count || stats.total_edge_count || 0} /></Card>
        </Col>
        <Col xs={12} md={6}>
          <Card><Statistic title="当前展示节点" value={graphData?.filtered_node_count || stats.job_node_count || 0} /></Card>
        </Col>
        <Col xs={12} md={6}>
          <Card><Statistic title="当前展示关系" value={graphData?.filtered_edge_count || stats.total_edge_count || 0} /></Card>
        </Col>
      </Row>

      <Spin spinning={loading}>
        <div className="job-path-workspace">
          <aside className="graph-control-panel">
            <h3>图谱筛选</h3>
            <Input
              allowClear
              prefix={<SearchOutlined />}
              placeholder="搜索岗位名称"
              value={searchText}
              onChange={(event) => setSearchText(event.target.value)}
            />
            <div className="control-group">
              <span>展示范围</span>
              <Radio.Group
                value={graphScope}
                onChange={(event) => {
                  setGraphScope(event.target.value)
                  setSelection(null)
                }}
                buttonStyle="solid"
              >
                <Radio.Button value="curated">精选图谱</Radio.Button>
                <Radio.Button value="all">全部原始图谱</Radio.Button>
              </Radio.Group>
            </div>
            <div className="control-group">
              <span>关系类型</span>
              <Radio.Group
                value={relationFilter}
                onChange={(event) => setRelationFilter(event.target.value)}
                buttonStyle="solid"
              >
                <Radio.Button value="all">全部</Radio.Button>
                <Radio.Button value="PROMOTE_TO">晋升</Radio.Button>
                <Radio.Button value="TRANSFER_TO">转岗</Radio.Button>
              </Radio.Group>
            </div>
            <Button block icon={<ReloadOutlined />} onClick={handleResetView}>
              重置视图
            </Button>
            <GraphLegend />
          </aside>

          <main className="graph-canvas-card">
            {g6Data.nodes.length > 0 && g6Data.edges.length > 0 ? (
              <div ref={containerRef} className="graph-canvas" />
            ) : (
              <div className="graph-empty">
                <Empty
                  description={graphStatus === 'available' ? '当前筛选条件下暂无关系' : '暂无岗位路径图谱数据'}
                />
              </div>
            )}
          </main>

          <aside className="graph-detail-panel">
            <h3>详情面板</h3>
            {!selection && (
              <div className="detail-placeholder">
                <ApartmentOutlined />
                <p>点击图谱中的岗位节点或关系边查看详情。</p>
              </div>
            )}
            {selectedNode && <NodeDetail node={selectedNode} edges={filteredEdges} />}
            {selectedEdge && <EdgeDetail edge={selectedEdge} />}
          </aside>
        </div>
      </Spin>

      <Row gutter={[16, 16]} className="job-path-lists">
        <Col xs={24} lg={12}>
          <PathList title="晋升路径列表" edges={promotionEdges} />
        </Col>
        <Col xs={24} lg={12}>
          <PathList title="转岗路径列表" edges={transferEdges} />
        </Col>
      </Row>

      <Card className="job-path-note-card">
        <Space direction="vertical" size={8}>
          <b>可信边界说明</b>
          <span>
            本页面只展示 Neo4j 或本地 Neo4j 导入 CSV 中真实存在的路径关系，不使用 LLM 生成路径，也不代表每个用户目标岗位都存在对应晋升路径。
          </span>
          <Tooltip title="CareerPath 页面仍负责当前用户目标岗位路径状态；本页面负责查看全局岗位路径事实。">
            <Tag color="blue">全局图谱事实展示</Tag>
          </Tooltip>
        </Space>
      </Card>
    </div>
  )
}

export default JobPathGraph
