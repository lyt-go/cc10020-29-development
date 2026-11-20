<template>
  <section class="page multi-transform-page">
    <header class="page-head">
      <div>
        <h2>多规则批量转换数据集</h2>
        <p class="page-desc">
          同批源记录一次选择多套转换规则，逐版本生成成品；支持断点续做、分版本质检阻断、
          字段血缘回查与两套映射按源行对照。重复提交同批同源数据只保留一个版本成品。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-submit" type="button" @click="openSubmit">
          新建多规则批次
        </button>
        <button class="btn" type="button" @click="reload">刷新</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">来源批次</span>
        <strong class="stat-value">{{ batches.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">可领取版本</span>
        <strong class="stat-value">{{ totalUsable }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">阻断待复核</span>
        <strong class="stat-value">{{ totalBlocked }}</strong>
      </article>
    </div>

    <p v-if="message" :class="['flash', messageOk ? 'flash-ok' : 'error-text']">{{ message }}</p>

    <table class="data-table" data-testid="batch-table">
      <thead>
        <tr>
          <th>批次编号</th>
          <th>源记录数</th>
          <th>规则数</th>
          <th>版本概况</th>
          <th>批次状态</th>
          <th>提交时间</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="batch in batches" :key="batch.id" :data-testid="`batch-${batch.批次编号}`">
          <td>{{ batch.批次编号 }}</td>
          <td>{{ batch.源记录数 }}</td>
          <td>{{ batch.规则数 }}</td>
          <td>
            <span class="tag tag-ok">可领取 {{ batch.可领取版本数 }}</span>
            <span class="tag tag-block">阻断 {{ batch.阻断版本数 }}</span>
            <span class="tag">共 {{ batch.版本数 }} 版本</span>
          </td>
          <td>
            <span :class="['status-pill', `pill-${batch.status}`]">{{ batch.status }}</span>
          </td>
          <td>{{ formatTime(batch.提交时间) }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="selectBatch(batch.批次编号)">版本明细</button>
            <button class="link" type="button" @click="runOrResume(batch.批次编号, batch.status)">
              {{ batch.status === '转换中断' ? '断点续做' : '执行转换' }}
            </button>
          </td>
        </tr>
        <tr v-if="!batches.length">
          <td colspan="7" class="empty-state">暂无转换批次，点击「新建多规则批次」开始</td>
        </tr>
      </tbody>
    </table>

    <div v-if="selectedBatchNo" class="detail-panel" data-testid="version-panel">
      <div class="detail-head">
        <h3>批次 {{ selectedBatchNo }} 的规则版本与可用情况</h3>
        <button class="btn ghost" type="button" @click="selectedBatchNo = ''">关闭</button>
      </div>

      <div class="avail-columns">
        <div class="avail-col">
          <h4 class="avail-title avail-ok">可领取版本（{{ availability?.usable_versions.length ?? 0 }}）</h4>
          <ul v-if="availability" class="avail-list">
            <li v-for="item in availability.usable_versions" :key="item.version_id">
              <strong>{{ item.规则编号 }}（{{ item.status }}）</strong>
              <span class="muted">{{ item.waiting_reason }}</span>
            </li>
            <li v-if="!availability.usable_versions.length" class="muted">暂无可用版本</li>
          </ul>
        </div>
        <div class="avail-col">
          <h4 class="avail-title avail-wait">等待中的版本（{{ availability?.waiting_versions.length ?? 0 }}）</h4>
          <ul v-if="availability" class="avail-list">
            <li v-for="item in availability.waiting_versions" :key="item.version_id">
              <strong>{{ item.规则编号 }}（{{ item.status }}）</strong>
              <span class="muted">{{ item.waiting_reason }}</span>
            </li>
            <li v-if="!availability.waiting_versions.length" class="muted">全部版本均可领取</li>
          </ul>
        </div>
      </div>

      <table class="data-table">
        <thead>
          <tr>
            <th>版本ID</th>
            <th>规则编号</th>
            <th>进度（成功/失败/总数）</th>
            <th>版本状态</th>
            <th>待复核</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="version in versions" :key="version.id" :data-testid="`version-${version.id}`">
            <td>{{ version.id }}</td>
            <td>{{ version.规则编号 }}</td>
            <td>{{ version.成功行数 }} / {{ version.失败行数 }} / {{ version.源记录数 }}</td>
            <td>
              <span :class="['status-pill', `pill-${version.status}`]">{{ version.status }}</span>
              <span v-if="version.中断原因" class="muted block-reason">{{ version.中断原因 }}</span>
            </td>
            <td>{{ version.待复核数 }}</td>
            <td class="row-actions">
              <button
                v-if="canQualityCheck(version.status)"
                class="link"
                type="button"
                @click="doQualityCheck(version.id)"
              >
                分版本质检
              </button>
              <button
                v-if="version.status === '质检通过'"
                class="link"
                type="button"
                @click="doPublish(version.id)"
              >
                发布版本
              </button>
              <button
                v-if="canRepair(version.status) && version.失败行数 > 0"
                class="link"
                type="button"
                @click="doRepair(version.id)"
              >
                失败行补做
              </button>
              <button class="link" type="button" @click="openLineage(version)">
                字段血缘
              </button>
              <button
                v-if="versions.length >= 2"
                class="link"
                type="button"
                @click="openCompare(version)"
              >
                跨映射对照
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 新建批次弹窗 -->
    <div v-if="showSubmit" class="modal-backdrop" data-testid="submit-modal">
      <form class="modal" @submit.prevent="doSubmit">
        <h3>新建多规则转换批次</h3>
        <label class="create-field">
          <span>来源批次号</span>
          <input v-model="submitForm.batch_no" data-testid="submit-batch-no" required />
        </label>
        <label class="create-field">
          <span>选择转换规则（多选，仅列出「已生效」规则）</span>
          <select v-model="submitForm.rule_ids" data-testid="submit-rules" multiple size="5" class="multi-select">
            <option v-for="rule in activeRules" :key="rule.id" :value="rule.id">
              {{ rule.规则编号 }}（ID:{{ rule.id }}）｜{{ rule.源Schema }} → {{ rule.目标Schema }}
            </option>
          </select>
        </label>
        <label class="create-field">
          <span>源记录（JSON 数组，每行一条）</span>
          <textarea
            v-model="submitForm.source_json"
            class="json-input"
            rows="8"
            data-testid="submit-source"
            placeholder='[{"id":"A1","name":"张三","amount":"100"}]'
            required
          ></textarea>
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showSubmit = false">取消</button>
          <button class="btn primary" type="submit" :disabled="submitting">
            {{ submitting ? '提交中…' : '提交并生成版本' }}
          </button>
        </div>
      </form>
    </div>

    <!-- 血缘弹窗 -->
    <div v-if="lineageData" class="modal-backdrop" data-testid="lineage-modal">
      <div class="modal modal-wide">
        <h3>字段血缘回查｜{{ lineageData.规则编号 }} 源行 {{ lineageData.源行号 }}（{{ lineageData.转换状态 }}）</h3>
        <p class="muted">
          源记录：{{ display(lineageData.源记录) }}
        </p>
        <table class="data-table">
          <thead>
            <tr>
              <th>目标字段</th>
              <th>来源路径</th>
              <th>实际源值</th>
              <th>转换函数</th>
              <th>成品值</th>
              <th>状态</th>
              <th>错误</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="entry in lineageData.字段血缘" :key="entry.target_field">
              <td>{{ entry.target_field }}</td>
              <td>{{ entry.source_path }}</td>
              <td>{{ display(entry.source_value) }}</td>
              <td>{{ entry.function }}</td>
              <td>{{ display(entry.output_value) }}</td>
              <td>
                <span :class="['status-pill', entry.status === 'success' ? 'pill-质检通过' : 'pill-质检阻断']">
                  {{ lineageStatusText(entry.status) }}
                </span>
              </td>
              <td class="error-text">{{ entry.error || '—' }}</td>
            </tr>
          </tbody>
        </table>
        <div class="modal-actions">
          <label class="create-field inline">
            <span>源行号</span>
            <input v-model.number="lineageRowNo" type="number" min="1" class="row-input" />
          </label>
          <button class="btn" type="button" @click="loadLineage()">查询该行</button>
          <button class="btn ghost" type="button" @click="lineageData = null">关闭</button>
        </div>
      </div>
    </div>

    <!-- 跨映射对照弹窗 -->
    <div v-if="compareForm.open" class="modal-backdrop" data-testid="compare-modal">
      <div class="modal modal-wide">
        <h3>两套映射按源行对照</h3>
        <div class="modal-actions">
          <label class="create-field inline">
            <span>规则A</span>
            <select v-model.number="compareForm.rule_a_id">
              <option v-for="version in versions" :key="`a-${version.id}`" :value="version.规则ID">
                {{ version.规则编号 }}
              </option>
            </select>
          </label>
          <label class="create-field inline">
            <span>规则B</span>
            <select v-model.number="compareForm.rule_b_id">
              <option v-for="version in versions" :key="`b-${version.id}`" :value="version.规则ID">
                {{ version.规则编号 }}
              </option>
            </select>
          </label>
          <label class="create-field inline">
            <span>源行号</span>
            <input v-model.number="compareForm.source_row_no" type="number" min="1" class="row-input" />
          </label>
          <button class="btn primary" type="button" @click="doCompare">核对差异</button>
          <button class="btn ghost" type="button" @click="compareForm.open = false">关闭</button>
        </div>
        <p v-if="compareResult" class="muted">
          源记录：{{ display(compareResult.源记录) }} ｜ 差异字段数：{{ compareResult.差异字段数 }}
        </p>
        <table v-if="compareResult" class="data-table">
          <thead>
            <tr>
              <th>字段</th>
              <th>规则A值（{{ compareResult.规则A.规则编号 }}）</th>
              <th>规则B值（{{ compareResult.规则B.规则编号 }}）</th>
              <th>是否一致</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="diff in compareResult.字段差异" :key="diff.field">
              <td>{{ diff.field }}</td>
              <td>{{ display(diff.规则A值) }}</td>
              <td>{{ display(diff.规则B值) }}</td>
              <td>
                <span :class="['status-pill', diff.一致 ? 'pill-质检通过' : 'pill-质检阻断']">
                  {{ diff.一致 ? '一致' : '差异' }}
                </span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import { listEntries } from '@/api/backend-service'
import {
  compareRules,
  getAvailability,
  getBatch,
  getLineage,
  listBatches,
  publishVersion,
  qualityCheck,
  repairVersion,
  resumeBatch,
  runBatch,
  submitBatch,
  type AvailabilityItem,
  type BatchSummary,
  type CompareResult,
  type LineageEntry,
  type MultiVersion,
} from '@/api/multi-transform'
import type { EntryRow } from '@/data/types'

const QUALIFIABLE = ['转换完成', '质检阻断', '质检通过']
const REPAIRABLE = ['转换完成', '质检阻断', '质检通过']

const batches = ref<BatchSummary[]>([])
const activeRules = ref<EntryRow[]>([])
const selectedBatchNo = ref('')
const versions = ref<MultiVersion[]>([])
const availability = ref<{
  usable_versions: AvailabilityItem[]
  waiting_versions: AvailabilityItem[]
} | null>(null)
const message = ref('')
const messageOk = ref(true)

const showSubmit = ref(false)
const submitting = ref(false)
const submitForm = reactive({
  batch_no: '',
  rule_ids: [] as number[],
  source_json: '',
})

const lineageData = ref<{
  规则编号: string
  源行号: number
  转换状态: string
  源记录: Record<string, unknown>
  字段血缘: LineageEntry[]
  versionId: number
} | null>(null)
const lineageRowNo = ref(1)

const compareForm = reactive({
  open: false,
  rule_a_id: 0,
  rule_b_id: 0,
  source_row_no: 1,
})
const compareResult = ref<CompareResult | null>(null)

const totalUsable = computed(() =>
  batches.value.reduce((sum, item) => sum + item.可领取版本数, 0),
)
const totalBlocked = computed(() =>
  batches.value.reduce((sum, item) => sum + item.阻断版本数, 0),
)

function flash(text: string, ok = true) {
  message.value = text
  messageOk.value = ok
}

function display(value: unknown): string {
  if (value === null || value === undefined || value === '') return '—'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function formatTime(value?: string): string {
  return value ? value.replace('T', ' ').slice(0, 19) : '—'
}

function canQualityCheck(status: string): boolean {
  return QUALIFIABLE.includes(status)
}

function canRepair(status: string): boolean {
  return REPAIRABLE.includes(status)
}

const LINEAGE_STATUS_TEXT: Record<string, string> = {
  success: '成功',
  defaulted: '默认值填充',
  skipped: '跳过',
  error: '失败',
}

function lineageStatusText(status: string): string {
  return LINEAGE_STATUS_TEXT[status] ?? status
}

async function reload() {
  try {
    const [batchPayload, rulePayload] = await Promise.all([
      listBatches(),
      listEntries('data_transform', { status: '已生效' }),
    ])
    batches.value = batchPayload.items
    activeRules.value = rulePayload.items
    if (selectedBatchNo.value) {
      await loadBatchDetail(selectedBatchNo.value)
    }
  } catch (error) {
    flash(error instanceof Error ? error.message : '数据加载失败', false)
  }
}

function openSubmit() {
  submitForm.batch_no = ''
  submitForm.rule_ids = []
  submitForm.source_json = ''
  showSubmit.value = true
}

async function doSubmit() {
  let sourceRecords: Record<string, unknown>[]
  try {
    const parsed = JSON.parse(submitForm.source_json)
    if (!Array.isArray(parsed) || parsed.length === 0) {
      throw new Error('源记录必须是非空 JSON 数组')
    }
    sourceRecords = parsed
  } catch (error) {
    flash(error instanceof Error ? `源记录 JSON 解析失败：${error.message}` : '源记录格式错误', false)
    return
  }
  if (!submitForm.rule_ids.length) {
    flash('请至少选择一套转换规则', false)
    return
  }
  submitting.value = true
  try {
    const result = await submitBatch({
      batch_no: submitForm.batch_no.trim(),
      rule_ids: submitForm.rule_ids,
      source_records: sourceRecords,
    })
    flash(result.message ?? '批次已提交', Boolean(result.ok))
    if (result.ok) {
      showSubmit.value = false
      selectedBatchNo.value = submitForm.batch_no.trim()
      await reload()
    }
  } catch (error) {
    flash(error instanceof Error ? error.message : '批次提交失败', false)
  } finally {
    submitting.value = false
  }
}

async function selectBatch(batchNo: string) {
  selectedBatchNo.value = batchNo
  await loadBatchDetail(batchNo)
}

async function loadBatchDetail(batchNo: string) {
  const [detail, avail] = await Promise.all([
    getBatch(batchNo),
    getAvailability(batchNo),
  ])
  versions.value = detail.versions
  availability.value = avail
}

async function runOrResume(batchNo: string, status: string) {
  try {
    const action = status === '转换中断' ? resumeBatch(batchNo) : runBatch(batchNo)
    const result = await action
    flash(result.message ?? '执行完成', Boolean(result.ok))
    await reload()
  } catch (error) {
    flash(error instanceof Error ? error.message : '执行失败', false)
  }
}

async function doQualityCheck(versionId: number) {
  try {
    const result = await qualityCheck(versionId)
    flash(result.message ?? '质检完成', Boolean(result.ok))
    await reload()
  } catch (error) {
    flash(error instanceof Error ? error.message : '质检失败', false)
  }
}

async function doPublish(versionId: number) {
  try {
    const result = await publishVersion(versionId)
    flash(result.message ?? '发布完成', Boolean(result.ok))
    await reload()
  } catch (error) {
    flash(error instanceof Error ? error.message : '发布失败', false)
  }
}

async function doRepair(versionId: number) {
  try {
    const result = await repairVersion(versionId)
    flash(result.message ?? '补做完成', Boolean(result.ok))
    await reload()
  } catch (error) {
    flash(error instanceof Error ? error.message : '补做失败', false)
  }
}

async function openLineage(version: MultiVersion) {
  lineageData.value = null
  lineageRowNo.value = 1
  await loadLineage(version.id)
}

async function loadLineage(versionId?: number) {
  const targetVersionId = versionId ?? lineageData.value?.versionId
  if (!targetVersionId) return
  try {
    const result = await getLineage(targetVersionId, lineageRowNo.value)
    if (!result.ok) throw new Error(result.message)
    lineageData.value = {
      versionId: targetVersionId,
      规则编号: result.规则编号,
      源行号: result.源行号,
      转换状态: result.转换状态,
      源记录: result.源记录,
      字段血缘: result.字段血缘,
    }
  } catch (error) {
    flash(error instanceof Error ? error.message : '血缘查询失败', false)
  }
}

async function openCompare(current: MultiVersion) {
  const other = versions.value.find((item) => item.id !== current.id)
  compareForm.open = true
  compareForm.rule_a_id = current.规则ID
  compareForm.rule_b_id = other?.规则ID ?? current.规则ID
  compareForm.source_row_no = 1
  compareResult.value = null
}

async function doCompare() {
  if (!selectedBatchNo.value) return
  try {
    const result = await compareRules(
      selectedBatchNo.value,
      compareForm.rule_a_id,
      compareForm.rule_b_id,
      compareForm.source_row_no,
    )
    if (!result.ok) throw new Error(result.message)
    compareResult.value = result
  } catch (error) {
    flash(error instanceof Error ? error.message : '对照失败', false)
  }
}

onMounted(reload)
</script>

<style scoped>
.multi-transform-page .tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 12px;
  background: #eef2f7;
  color: #475569;
  margin-right: 4px;
}
.tag-ok { background: #dcfce7; color: #15803d; }
.tag-block { background: #fee2e2; color: #b91c1c; }
.status-pill {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 12px;
  background: #e2e8f0;
  color: #334155;
  white-space: nowrap;
}
.pill-已发布,
.pill-质检通过 { background: #dcfce7; color: #15803d; }
.pill-质检阻断,
.pill-转换中断 { background: #fee2e2; color: #b91c1c; }
.pill-转换中,
.pill-待转换,
.pill-转换完成 { background: #e0f2fe; color: #0369a1; }
.detail-panel {
  margin-top: 18px;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 14px;
}
.detail-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.avail-columns {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
  margin: 10px 0;
}
.avail-col {
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px;
}
.avail-title { margin: 0 0 8px; font-size: 14px; }
.avail-ok { color: #15803d; }
.avail-wait { color: #b45309; }
.avail-list { list-style: none; margin: 0; padding: 0; }
.avail-list li {
  display: flex;
  flex-direction: column;
  gap: 2px;
  padding: 6px 0;
  border-bottom: 1px solid #f0f2f5;
  font-size: 13px;
}
.muted { color: var(--muted); font-size: 12px; }
.block-reason { margin-left: 6px; }
.flash {
  padding: 8px 12px;
  border-radius: 6px;
  background: #e0f2fe;
  color: #075985;
  font-size: 13px;
}
.flash-ok { background: #dcfce7; color: #15803d; }
.multi-select,
.json-input {
  width: 100%;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px;
  font-family: inherit;
}
.modal-wide { width: min(900px, 100%); }
.modal-actions {
  display: flex;
  gap: 10px;
  align-items: flex-end;
  margin-top: 12px;
  flex-wrap: wrap;
}
.inline { margin-bottom: 0; }
.row-input { width: 90px; }
</style>
