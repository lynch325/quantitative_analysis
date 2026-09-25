import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { Database, Download, Layers, Pencil, Plus, Search, Tag, Trash2, X } from 'lucide-react'
import {
  addPoolItems,
  createPool,
  deletePool,
  exportCsvUrl,
  fetchConditions,
  fetchPoolItems,
  fetchPoolSources,
  fetchPools,
  fetchSectors,
  fetchTradeDates,
  fetchWarehouseStatus,
  previewImport,
  removePoolItems,
  renamePool,
  type ImportKind,
} from '../api/stockPool'
import { Card, EmptyState, PageHeader, SkeletonRows } from '../components/ui'

const KINDS: { key: ImportKind; label: string }[] = [
  { key: 'sector', label: '数仓板块' },
  { key: 'condition', label: '数仓条件' },
  { key: 'sql', label: '自定义 SQL' },
  { key: 'paste', label: '复制粘贴' },
]

/** 后端哨兵值：未标注来源（空字符串在接口里是「不过滤」） */
const UNLABELED = '__none__'

const SECTOR_SORTS: { key: string; label: string }[] = [
  { key: 'name-asc', label: '名称 A→Z' },
  { key: 'count-desc', label: '成分多→少' },
  { key: 'count-asc', label: '成分少→多' },
]

function pctText(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '--'
  return `${value > 0 ? '+' : ''}${value.toFixed(2)}%`
}

function pctClass(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return 'num'
  if (value > 0) return 'num text-danger'
  if (value < 0) return 'num text-success'
  return 'num'
}

export default function StockPoolPage() {
  const qc = useQueryClient()
  const [activeId, setActiveId] = useState<number | null>(null)
  const [keyword, setKeyword] = useState('')
  const [source, setSource] = useState('')
  const [withQuote, setWithQuote] = useState(false)
  const [checked, setChecked] = useState<string[]>([])
  const [notice, setNotice] = useState<string | null>(null)

  const [panelOpen, setPanelOpen] = useState(false)
  const [kind, setKind] = useState<ImportKind>('sector')
  const [sectorCode, setSectorCode] = useState('')
  const [sectorType, setSectorType] = useState('行业')
  const [sectorKeyword, setSectorKeyword] = useState('')
  const [sectorSort, setSectorSort] = useState('name-asc')
  const [condition, setCondition] = useState('涨停')
  const [date, setDate] = useState('')
  const [extra, setExtra] = useState('5')
  const [sql, setSql] = useState('')
  const [pasteText, setPasteText] = useState('')
  const [previewCodes, setPreviewCodes] = useState<string[]>([])

  const poolsQuery = useQuery({ queryKey: ['stock-pool', 'pools'], queryFn: fetchPools })
  const pools = poolsQuery.data ?? []
  const poolId = activeId ?? pools[0]?.id ?? null
  const activeName = pools.find((p) => p.id === poolId)?.name ?? ''

  const itemsQuery = useQuery({
    queryKey: ['stock-pool', 'items', poolId, keyword, source, withQuote],
    queryFn: () =>
      fetchPoolItems(poolId as number, {
        keyword,
        source,
        quote: withQuote ? '1' : undefined,
      }),
    enabled: poolId !== null,
  })
  const items = itemsQuery.data?.items ?? []

  const sourcesQuery = useQuery({
    queryKey: ['stock-pool', 'sources', poolId],
    queryFn: () => fetchPoolSources(poolId as number),
    enabled: poolId !== null,
  })
  const sources = sourcesQuery.data ?? []
  const sourceTotal = sources.reduce((sum, s) => sum + s.count, 0)

  const warehouseQuery = useQuery({
    queryKey: ['stock-pool', 'warehouse'],
    queryFn: fetchWarehouseStatus,
    staleTime: 60_000,
  })
  // 板块一次全量拉取（744 条、后端聚合仅 0.02s），类型/搜索/排序都在前端做，
  // 避免切类型就打一次接口
  const sectorsQuery = useQuery({
    queryKey: ['stock-pool', 'sectors'],
    queryFn: () => fetchSectors(),
    enabled: panelOpen && kind === 'sector',
    staleTime: 5 * 60_000,
  })
  const datesQuery = useQuery({
    queryKey: ['stock-pool', 'dates'],
    queryFn: () => fetchTradeDates(30),
    enabled: panelOpen && kind === 'condition',
  })
  const conditionsQuery = useQuery({
    queryKey: ['stock-pool', 'conditions'],
    queryFn: fetchConditions,
    enabled: panelOpen && kind === 'condition',
  })

  const allSectors = sectorsQuery.data ?? []
  const sectorTypes = useMemo(() => {
    const set = new Set(allSectors.map((s) => s.type || '未分类'))
    return Array.from(set).sort((a, b) => a.localeCompare(b, 'zh-Hans-CN'))
  }, [allSectors])
  const visibleSectors = useMemo(() => {
    const kw = sectorKeyword.trim().toLowerCase()
    const list = allSectors.filter(
      (s) => (s.type || '未分类') === sectorType &&
        (!kw || s.name.toLowerCase().includes(kw) || s.code.toLowerCase().includes(kw)),
    )
    const sorted = [...list]
    if (sectorSort === 'count-desc') sorted.sort((a, b) => b.count - a.count)
    else if (sectorSort === 'count-asc') sorted.sort((a, b) => a.count - b.count)
    else sorted.sort((a, b) => a.name.localeCompare(b.name, 'zh-Hans-CN'))
    return sorted
  }, [allSectors, sectorType, sectorKeyword, sectorSort])

  const sectorLabel = allSectors.find((s) => s.code === sectorCode)?.name ?? ''

  useEffect(() => {
    if (!date && datesQuery.data?.length) setDate(datesQuery.data[0])
  }, [date, datesQuery.data])

  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ['stock-pool'] })
  }

  const createMut = useMutation({
    mutationFn: (name: string) => createPool(name),
    onSuccess: () => {
      invalidate()
      setNotice('已新建股票池')
    },
    onError: (e: Error) => setNotice(e.message),
  })

  const renameMut = useMutation({
    mutationFn: ({ id, name }: { id: number; name: string }) => renamePool(id, name),
    onSuccess: () => {
      invalidate()
      setNotice('已重命名')
    },
    onError: (e: Error) => setNotice(e.message),
  })

  const deleteMut = useMutation({
    mutationFn: (id: number) => deletePool(id),
    onSuccess: () => {
      setActiveId(null)
      invalidate()
      setNotice('已删除股票池')
    },
    onError: (e: Error) => setNotice(e.message),
  })

  const removeMut = useMutation({
    mutationFn: (codes: string[]) => removePoolItems(poolId as number, codes),
    onSuccess: (res) => {
      setChecked([])
      invalidate()
      setNotice(`已移除 ${res.removed} 只`)
    },
    onError: (e: Error) => setNotice(e.message),
  })

  const addMut = useMutation({
    mutationFn: ({ codes, src }: { codes: string[]; src: string }) =>
      addPoolItems(poolId as number, codes, src),
    onSuccess: (res) => {
      invalidate()
      setPanelOpen(false)
      setPreviewCodes([])
      setNotice(`已加入 ${res.added} 只`)
    },
    onError: (e: Error) => setNotice(e.message),
  })

  const previewMut = useMutation({
    mutationFn: () =>
      previewImport({
        kind,
        sector_code: kind === 'sector' ? sectorCode : undefined,
        condition: kind === 'condition' ? condition : undefined,
        date: kind === 'condition' ? date : undefined,
        extra: kind === 'condition' ? Number(extra) : undefined,
        sql: kind === 'sql' ? sql : undefined,
        text: kind === 'paste' ? pasteText : undefined,
      }),
    onSuccess: (res) => setPreviewCodes(res.codes),
    onError: (e: Error) => {
      setPreviewCodes([])
      setNotice(e.message)
    },
  })

  const toggleCheck = (code: string) => {
    setChecked((prev) => (prev.includes(code) ? prev.filter((c) => c !== code) : [...prev, code]))
  }

  const sourceLabelOf = () => {
    if (kind === 'sector') return sectorLabel || '数仓板块'
    if (kind === 'condition') return `${condition} ${date}`
    if (kind === 'sql') return '自定义SQL'
    return '粘贴'
  }

  return (
    <>
      <PageHeader
        title="股票池"
        subtitle="把数仓板块、条件、自定义 SQL 或粘贴的代码沉淀成命名分组，可导出为 CSV"
      />

      {notice && (
        <div className="panel pool-notice">
          <div className="panel-body d-flex justify-content-between align-items-center">
            <span>{notice}</span>
            <button type="button" className="btn btn-sm btn-outline-secondary" onClick={() => setNotice(null)}>
              关闭
            </button>
          </div>
        </div>
      )}

      {warehouseQuery.data && !warehouseQuery.data.available && (
        <div className="panel pool-notice">
          <div className="panel-body">
            ⚠️ 数仓不可用（{warehouseQuery.data.path}），导入源相关功能暂时不可用，已建股票池仍可查看与导出。
          </div>
        </div>
      )}

      <div className="pool-layout">
        <Card title="股票池">
          <div className="pool-toolbar">
            <button
              type="button"
              className="btn btn-sm btn-primary"
              onClick={() => {
                const name = window.prompt('新建股票池名称')
                if (name) createMut.mutate(name.trim())
              }}
            >
              <Plus size={14} /> 新建
            </button>
            <button
              type="button"
              className="btn btn-sm btn-outline-secondary"
              disabled={!poolId}
              onClick={() => {
                if (!poolId) return
                const name = window.prompt('重命名', activeName)
                if (name) renameMut.mutate({ id: poolId, name: name.trim() })
              }}
            >
              <Pencil size={14} /> 重命名
            </button>
            <button
              type="button"
              className="btn btn-sm btn-outline-danger"
              disabled={!poolId}
              onClick={() => {
                if (!poolId) return
                if (window.confirm(`删除股票池「${activeName}」及其全部成分？`)) deleteMut.mutate(poolId)
              }}
            >
              <Trash2 size={14} /> 删除
            </button>
          </div>

          {poolsQuery.isLoading ? (
            <SkeletonRows rows={4} />
          ) : pools.length === 0 ? (
            <EmptyState icon="🗂️" title="还没有股票池" description="点「新建」创建一个" />
          ) : (
            <>
              <ul className="pool-list">
                {pools.map((pool) => (
                  <li key={pool.id}>
                    <button
                      type="button"
                      className={`pool-item ${pool.id === poolId ? 'active' : ''}`}
                      onClick={() => {
                        setActiveId(pool.id)
                        setChecked([])
                        setSource('')  // 换池重置来源：来源名不跨池通用
                      }}
                    >
                      <Layers size={14} />
                      <span className="name">{pool.name}</span>
                      <span className="count">{pool.count ?? 0}</span>
                    </button>
                  </li>
                ))}
              </ul>

              {sources.length > 0 && (
                <div className="pool-source-block">
                  <div className="pool-subtitle">来源</div>
                  <ul className="pool-list">
                    <li>
                      <button
                        type="button"
                        className={`pool-item ${source === '' ? 'active' : ''}`}
                        onClick={() => setSource('')}
                      >
                        <Layers size={13} />
                        <span className="name">全部来源</span>
                        <span className="count">{sourceTotal}</span>
                      </button>
                    </li>
                    {sources.map((s) => (
                      <li key={s.name}>
                        <button
                          type="button"
                          className={`pool-item ${source === s.name ? 'active' : ''}`}
                          onClick={() => setSource(s.name)}
                        >
                          <Tag size={13} />
                          <span className="name">{s.name === UNLABELED ? '（未标注）' : s.name}</span>
                          <span className="count">{s.count}</span>
                        </button>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </>
          )}
        </Card>

        <Card title={activeName ? `成分 · ${activeName}` : '成分'}>
          <div className="pool-toolbar">
            <button type="button" className="btn btn-sm btn-primary" disabled={!poolId} onClick={() => setPanelOpen((v) => !v)}>
              <Database size={14} /> 导入
            </button>
            <button type="button" className="btn btn-sm btn-outline-secondary" disabled={!poolId} onClick={() => window.open(exportCsvUrl(poolId as number), '_blank')}>
              <Download size={14} /> 导出 CSV
            </button>
            <button
              type="button"
              className="btn btn-sm btn-outline-danger"
              disabled={checked.length === 0}
              onClick={() => removeMut.mutate(checked)}
            >
              <Trash2 size={14} /> 移除选中（{checked.length}）
            </button>
            <div className="pool-search-box">
              <Search size={13} />
              <input
                className="form-control form-control-sm"
                placeholder="搜索代码/名称/行业"
                value={keyword}
                onChange={(e) => setKeyword(e.target.value)}
              />
            </div>
            <label className="pool-check">
              <input type="checkbox" checked={withQuote} onChange={(e) => setWithQuote(e.target.checked)} />
              显示行情
            </label>
          </div>

          {panelOpen && poolId && (
            <div className="pool-import">
              <div className="pool-import-head">
                <div className="pool-chips">
                  {KINDS.map((k) => (
                    <button
                      key={k.key}
                      type="button"
                      className={`pool-chip ${kind === k.key ? 'active' : ''}`}
                      onClick={() => {
                        setKind(k.key)
                        setPreviewCodes([])
                      }}
                    >
                      {k.label}
                    </button>
                  ))}
                </div>
                <button type="button" className="btn btn-sm btn-outline-secondary pool-close" onClick={() => setPanelOpen(false)}>
                  <X size={14} />
                </button>
              </div>

              {kind === 'sector' && (
                <>
                  <div className="pool-chips pool-chips-sub">
                    {sectorTypes.map((t) => (
                      <button
                        key={t}
                        type="button"
                        className={`pool-chip ${sectorType === t ? 'active' : ''}`}
                        onClick={() => {
                          setSectorType(t)
                          setSectorCode('')
                          setPreviewCodes([])
                        }}
                      >
                        {t}
                      </button>
                    ))}
                  </div>
                  <div className="pool-sector-tools">
                    <div className="pool-search-box">
                      <Search size={13} />
                      <input
                        className="form-control form-control-sm"
                        placeholder="搜索板块名称/代码"
                        value={sectorKeyword}
                        onChange={(e) => setSectorKeyword(e.target.value)}
                      />
                    </div>
                    <select className="form-select form-select-sm" value={sectorSort} onChange={(e) => setSectorSort(e.target.value)}>
                      {SECTOR_SORTS.map((s) => (
                        <option key={s.key} value={s.key}>
                          {s.label}
                        </option>
                      ))}
                    </select>
                    <span className="pool-hint">{visibleSectors.length} 个</span>
                  </div>
                  {sectorsQuery.isLoading ? (
                    <SkeletonRows rows={3} />
                  ) : (
                    <div className="pool-sector-list">
                      {visibleSectors.length === 0 ? (
                        <div className="pool-empty-hint">无匹配板块</div>
                      ) : (
                        visibleSectors.map((s) => (
                          <button
                            key={s.code}
                            type="button"
                            className={`pool-sector-item ${s.code === sectorCode ? 'active' : ''}`}
                            onClick={() => {
                              setSectorCode(s.code)
                              setPreviewCodes([])
                            }}
                          >
                            <span className="name">{s.name}</span>
                            <span className="count">{s.count}</span>
                          </button>
                        ))
                      )}
                    </div>
                  )}
                </>
              )}

              {kind === 'condition' && (
                <div className="d-flex gap-2 flex-wrap">
                  <select className="form-select form-select-sm" value={date} onChange={(e) => setDate(e.target.value)}>
                    <option value="">交易日</option>
                    {(datesQuery.data ?? []).map((d) => (
                      <option key={d} value={d}>
                        {d}
                      </option>
                    ))}
                  </select>
                  <select className="form-select form-select-sm" value={condition} onChange={(e) => setCondition(e.target.value)}>
                    {(conditionsQuery.data ?? []).map((c) => (
                      <option key={c.name} value={c.name}>
                        {c.name}
                      </option>
                    ))}
                  </select>
                  <input
                    className="form-control form-control-sm pool-extra"
                    placeholder="阈值"
                    value={extra}
                    onChange={(e) => setExtra(e.target.value)}
                  />
                </div>
              )}

              {kind === 'sql' && (
                <textarea
                  className="form-control form-control-sm"
                  rows={3}
                  placeholder="SELECT stock_code FROM dim_stock WHERE ..."
                  value={sql}
                  onChange={(e) => setSql(e.target.value)}
                />
              )}

              {kind === 'paste' && (
                <textarea
                  className="form-control form-control-sm"
                  rows={3}
                  placeholder="粘贴任意含 6 位代码的文本"
                  value={pasteText}
                  onChange={(e) => setPasteText(e.target.value)}
                />
              )}

              <div className="pool-import-foot">
                <button
                  type="button"
                  className="btn btn-sm btn-outline-secondary"
                  onClick={() => previewMut.mutate()}
                  disabled={previewMut.isPending || (kind === 'sector' && !sectorCode)}
                >
                  {previewMut.isPending ? '预览中…' : '预览'}
                </button>
                <button
                  type="button"
                  className="btn btn-sm btn-primary"
                  disabled={previewCodes.length === 0 || addMut.isPending}
                  onClick={() => addMut.mutate({ codes: previewCodes, src: sourceLabelOf() })}
                >
                  导入 {previewCodes.length} 只
                </button>
                {previewCodes.length > 0 && (
                  <span className="pool-hint">来源将标记为「{sourceLabelOf()}」</span>
                )}
              </div>
            </div>
          )}

          {itemsQuery.isLoading ? (
            <SkeletonRows rows={6} />
          ) : items.length === 0 ? (
            <EmptyState icon="📭" title="暂无成分" description="点「导入」添加" />
          ) : (
            <div className="pool-table-wrap">
              <table className="table table-sm align-middle">
                <thead>
                  <tr>
                    <th style={{ width: 32 }} />
                    <th>代码</th>
                    <th>名称</th>
                    <th>子行业</th>
                    <th>来源</th>
                    <th>加入时间</th>
                    {withQuote && <th className="num">现价</th>}
                    {withQuote && <th className="num">涨跌幅</th>}
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {items.map((it) => (
                    <tr key={it.id}>
                      <td>
                        <input type="checkbox" checked={checked.includes(it.ts_code)} onChange={() => toggleCheck(it.ts_code)} />
                      </td>
                      <td>
                        <Link to={`/stock/${it.ts_code}`}>{it.ts_code}</Link>
                      </td>
                      <td>{it.name || '--'}</td>
                      <td className="text-muted">{it.industry || '--'}</td>
                      <td>{it.source || '（未标注）'}</td>
                      <td className="text-muted">{(it.add_at || '').replace('T', ' ').slice(0, 19)}</td>
                      {withQuote && <td className="num">{it.last_price ?? '--'}</td>}
                      {withQuote && <td className={pctClass(it.pct_chg)}>{pctText(it.pct_chg)}</td>}
                      <td>
                        <button type="button" className="btn btn-sm btn-outline-danger" onClick={() => removeMut.mutate([it.ts_code])}>
                          移除
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>
      </div>
    </>
  )
}
