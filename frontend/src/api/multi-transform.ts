/** 多规则批量转换数据集接口封装：批次、规则版本、质检、复核、血缘与跨映射对照。 */
import { request } from './client'

export type MultiVersion = {
  id: number
  批次编号: string
  规则ID: number
  规则编号: string
  规则版本: string
  status: string
  已完成行数: number
  源记录数: number
  成功行数: number
  失败行数: number
  待复核数: number
  质检阻断数: number
  中断原因: string
}

export type BatchSummary = {
  id: number
  批次编号: string
  来源批次引用: string
  源记录数: number
  规则数: number
  status: string
  版本数: number
  可领取版本数: number
  阻断版本数: number
  提交时间: string
}

export type AvailabilityItem = {
  version_id: number
  规则编号: string
  规则版本: string
  status: string
  usable: boolean
  waiting_reason: string
  已完成行数: number
  源记录数: number
  成功行数: number
  失败行数: number
  待复核数: number
}

export type LineageEntry = {
  target_field: string
  source_path: string
  source_value: unknown
  function: string
  output_value: unknown
  status: string
  error: string
}

export type ActionResponse = {
  ok: boolean
  message?: string
  interrupted?: boolean
  [key: string]: unknown
}

export type CompareResult = {
  ok: boolean
  message?: string
  批次编号: string
  源行号: number
  源记录: Record<string, unknown>
  差异字段数: number
  字段差异: {
    field: string
    规则A值: unknown
    规则B值: unknown
    规则A存在: boolean
    规则B存在: boolean
    一致: boolean
  }[]
  来源差异: Record<string, unknown>[]
  规则A: { 规则ID: number; 规则编号: string; 版本ID: number; status: string }
  规则B: { 规则ID: number; 规则编号: string; 版本ID: number; status: string }
}

export type LineageResult = {
  ok: boolean
  message?: string
  版本ID: number
  规则编号: string
  源行号: number
  转换状态: string
  源记录: Record<string, unknown>
  目标数据: Record<string, unknown>
  字段血缘: LineageEntry[]
  错误信息: string[]
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  const payload = (await response.json()) as T & { ok?: boolean; message?: string }
  if (!response.ok) throw new Error(payload.message ?? `接口返回 ${response.status}`)
  return payload
}

export function submitBatch(body: {
  batch_no: string
  rule_ids: number[]
  source_records: Record<string, unknown>[]
  source_batch_ref?: string
}) {
  return call<ActionResponse>('/api/multi-transform/batches', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function listBatches() {
  return call<{ ok: boolean; total: number; items: BatchSummary[] }>(
    '/api/multi-transform/batches',
  )
}

export function getBatch(batchNo: string) {
  return call<{ ok: boolean; batch: BatchSummary; versions: MultiVersion[] }>(
    `/api/multi-transform/batches/${encodeURIComponent(batchNo)}`,
  )
}

export function runBatch(batchNo: string) {
  return call<ActionResponse>(`/api/multi-transform/batches/${encodeURIComponent(batchNo)}/run`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
}

export function resumeBatch(batchNo: string) {
  return call<ActionResponse>(`/api/multi-transform/batches/${encodeURIComponent(batchNo)}/resume`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
}

export function getAvailability(batchNo: string) {
  return call<{
    ok: boolean
    批次编号: string
    批次状态: string
    usable_versions: AvailabilityItem[]
    waiting_versions: AvailabilityItem[]
    versions: AvailabilityItem[]
  }>(`/api/multi-transform/batches/${encodeURIComponent(batchNo)}/availability`)
}

export function compareRules(
  batchNo: string,
  ruleAId: number,
  ruleBId: number,
  sourceRowNo: number,
) {
  const params = new URLSearchParams({
    rule_a_id: String(ruleAId),
    rule_b_id: String(ruleBId),
    source_row_no: String(sourceRowNo),
  })
  return call<CompareResult>(
    `/api/multi-transform/batches/${encodeURIComponent(batchNo)}/compare?${params}`,
  )
}

export function qualityCheck(versionId: number) {
  return call<ActionResponse>(`/api/multi-transform/versions/${versionId}/quality-check`, {
    method: 'POST',
  })
}

export function publishVersion(versionId: number) {
  return call<ActionResponse>(`/api/multi-transform/versions/${versionId}/publish`, {
    method: 'POST',
  })
}

export function repairVersion(versionId: number) {
  return call<ActionResponse>(`/api/multi-transform/versions/${versionId}/repair`, {
    method: 'POST',
  })
}

export function getLineage(versionId: number, sourceRowNo: number) {
  return call<LineageResult>(
    `/api/multi-transform/versions/${versionId}/lineage?source_row_no=${sourceRowNo}`,
  )
}

export function listReviews(status?: string) {
  const query = status ? `?status=${encodeURIComponent(status)}` : ''
  return call<{ ok: boolean; total: number; items: Record<string, unknown>[] }>(
    `/api/multi-transform/reviews${query}`,
  )
}

export function resolveReview(reviewId: number, remark = '') {
  return call<ActionResponse>(`/api/multi-transform/reviews/${reviewId}/resolve`, {
    method: 'POST',
    body: JSON.stringify({ remark }),
  })
}
