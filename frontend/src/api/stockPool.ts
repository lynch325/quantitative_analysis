import { apiDelete, apiGet, apiPost, apiPut } from './client'

export interface StockPool {
  id: number
  name: string
  note: string
  created_at: string | null
  count?: number
}

export interface StockPoolItem {
  id: number
  pool_id: number
  ts_code: string
  name?: string
  industry?: string
  source: string
  add_at: string | null
  last_price?: number | null
  pct_chg?: number | null
}

export interface SectorItem {
  code: string
  name: string
  type: string
  count: number
}

export interface ConditionItem {
  name: string
  desc: string
  need_extra: boolean
}

export interface WarehouseStatus {
  path: string
  available: boolean
}

export function fetchPools(): Promise<StockPool[]> {
  return apiGet<StockPool[]>('/stock-pool/pools')
}

export function createPool(name: string, note = ''): Promise<StockPool> {
  return apiPost<StockPool>('/stock-pool/pools', { name, note })
}

export function renamePool(id: number, name: string): Promise<StockPool> {
  return apiPut<StockPool>(`/stock-pool/pools/${id}`, { name })
}

export function deletePool(id: number): Promise<{ deleted: number }> {
  return apiDelete<{ deleted: number }>(`/stock-pool/pools/${id}`)
}

export function fetchPoolItems(
  id: number,
  params?: { keyword?: string; source?: string; quote?: '1' },
): Promise<{ items: StockPoolItem[]; total: number }> {
  return apiGet<{ items: StockPoolItem[]; total: number }>(`/stock-pool/pools/${id}/items`, params)
}

export function addPoolItems(id: number, codes: string[], source = ''): Promise<{ added: number }> {
  return apiPost<{ added: number }>(`/stock-pool/pools/${id}/items`, { codes, source })
}

/** 删除成分走 query 传参（项目的 apiDelete 封装不便带 body） */
export function removePoolItems(id: number, codes: string[]): Promise<{ removed: number }> {
  const query = encodeURIComponent(codes.join(','))
  return apiDelete<{ removed: number }>(`/stock-pool/pools/${id}/items?codes=${query}`)
}

export interface PoolSource {
  /** 来源名；'__none__' 表示未标注来源（空字符串在接口里是「不过滤」） */
  name: string
  count: number
}

export function fetchPoolSources(id: number): Promise<PoolSource[]> {
  return apiGet<PoolSource[]>(`/stock-pool/pools/${id}/sources`)
}

export function fetchWarehouseStatus(): Promise<WarehouseStatus> {
  return apiGet<WarehouseStatus>('/stock-pool/warehouse/status')
}

export function fetchSectors(type?: string): Promise<SectorItem[]> {
  return apiGet<SectorItem[]>('/stock-pool/warehouse/sectors', type ? { type } : undefined)
}

export function fetchTradeDates(limit = 30): Promise<string[]> {
  return apiGet<string[]>('/stock-pool/warehouse/trade-dates', { limit })
}

export function fetchConditions(): Promise<ConditionItem[]> {
  return apiGet<ConditionItem[]>('/stock-pool/conditions')
}

export type ImportKind = 'sector' | 'condition' | 'sql' | 'paste'

export function previewImport(payload: {
  kind: ImportKind
  sector_code?: string
  condition?: string
  date?: string
  extra?: number
  sql?: string
  text?: string
}): Promise<{ codes: string[]; total: number }> {
  return apiPost<{ codes: string[]; total: number }>('/stock-pool/warehouse/preview', payload, 60_000)
}

export function exportCsvUrl(id: number): string {
  return `/api/stock-pool/pools/${id}/export`
}
