<script setup>
import { computed, onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'

const sources = ref([])
const sourceId = ref(null)
const target = ref('canonical')
const mode = ref('incremental')
const batchLimit = ref(2)
const batches = ref([])
const current = ref(null)
const intake = ref([])
const busy = ref(false)
const err = ref('')
const msg = ref('')

const stepTagClass = (s) =>
  s.status === 'success' ? 'ok' : s.status === 'failed' ? 'danger' : 'warn'

const report = computed(() => current.value?.report || null)

async function refreshBatches() {
  batches.value = await getJSON('/api/rpa/batches')
}

async function openBatch(id) {
  current.value = await getJSON(`/api/rpa/batches/${id}`)
}

async function refreshCurrent() {
  if (current.value) await openBatch(current.value.id)
}

async function loadIntake() {
  intake.value = await getJSON('/api/rpa/intake')
}

async function createQueue() {
  err.value = ''
  msg.value = ''
  busy.value = true
  try {
    const batch = await postJSON(`/api/sources/${sourceId.value}/rpa-queue`, {
      target: target.value,
    })
    await refreshBatches()
    await openBatch(batch.id)
    msg.value = `已生成批次 #${batch.id}：${batch.steps.length} 个待执行步骤`
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function execute(body) {
  if (!current.value) return
  err.value = ''
  msg.value = ''
  busy.value = true
  try {
    const result = await postJSON(
      `/api/rpa/batches/${current.value.id}/execute`,
      { mode: mode.value, ...body },
    )
    await openBatch(result.id)
    await refreshBatches()
    await loadIntake()
    msg.value = `批次 #${result.id} 已推进，当前状态 ${result.status}`
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

const nextPendingId = computed(() => {
  const step = current.value?.steps.find((s) => s.status === 'pending')
  return step ? step.id : null
})

async function promoteAllHeld() {
  err.value = ''
  msg.value = ''
  busy.value = true
  try {
    const r = await postJSON('/api/rpa/intake/promote', {
      batch_id: current.value?.id || null,
      mode: mode.value,
    })
    msg.value = `归并完成：写入主数据 ${r.promoted}，冲突 ${r.conflicts}，跳过 ${r.skipped}，失败 ${r.failed}`
    await refreshCurrent()
    await refreshBatches()
    await loadIntake()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

onMounted(async () => {
  sources.value = await getJSON('/api/sources')
  const delta = sources.value.find((s) => s.fixture_path === 'delta_rpa_clicks.jsonl')
  if (delta) sourceId.value = delta.id
  else if (sources.value.length) sourceId.value = sources.value[0].id
  await refreshBatches()
  if (batches.value.length) await openBatch(batches.value[0].id)
  await loadIntake()
})
</script>
<template>
  <div class="page">
    <h1>RPA 录入队列</h1>
    <p class="muted">
      本机模拟：把夹具逐行“填表单”生成步骤队列，再按步 / 按批执行入库。失败步不会冒充已入库；
      同键非键冲突仍进冲突隔离。无真实浏览器、无活网请求。
    </p>

    <div class="panel">
      <label>
        夹具源
        <select v-model.number="sourceId">
          <option v-for="s in sources" :key="s.id" :value="s.id">{{ s.id }}. {{ s.name }}</option>
        </select>
      </label>
      <label style="margin-left:1rem">
        执行去向
        <select v-model="target">
          <option value="canonical">直接归并入库</option>
          <option value="intake">先进摄入前队列</option>
        </select>
      </label>
      <button style="margin-left:1rem" :disabled="busy || !sourceId" @click="createQueue">
        生成步骤队列
      </button>
    </div>

    <p v-if="err" class="tag danger">{{ err }}</p>
    <p v-if="msg" class="tag ok">{{ msg }}</p>

    <div class="panel">
      <h3>批次列表（刷新后仍在）</h3>
      <table>
        <thead>
          <tr>
            <th>ID</th><th>源</th><th>去向</th><th>状态</th>
            <th>步骤总数</th><th>待执行</th><th>成功</th><th>失败</th><th>创建时间</th><th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="b in batches" :key="b.id">
            <td>#{{ b.id }}</td>
            <td>{{ b.source_name }}</td>
            <td class="mono">{{ b.target }}</td>
            <td><span class="tag" :class="b.status === 'done' ? 'ok' : 'warn'">{{ b.status }}</span></td>
            <td>{{ b.step_total }}</td>
            <td>{{ b.step_pending }}</td>
            <td>{{ b.step_success }}</td>
            <td>{{ b.step_failed }}</td>
            <td class="mono">{{ b.created_at }}</td>
            <td><button class="secondary" @click="openBatch(b.id)">查看步骤</button></td>
          </tr>
        </tbody>
      </table>
      <p v-if="!batches.length" class="muted">尚无批次，先生成一个步骤队列。</p>
    </div>

    <template v-if="current">
      <div class="panel">
        <h3>
          批次 #{{ current.id }} 执行台 · {{ current.source_name }} ·
          <span class="mono">{{ current.target }}</span> ·
          <span class="tag" :class="current.status === 'done' ? 'ok' : 'warn'">{{ current.status }}</span>
        </h3>
        <label style="margin-right:1rem">
          模式
          <select v-model="mode">
            <option value="incremental">incremental</option>
            <option value="full">full</option>
          </select>
        </label>
        <label style="margin-right:1rem">
          每批步数
          <input type="number" min="1" v-model.number="batchLimit" style="width:6rem" />
        </label>
        <button :disabled="busy || nextPendingId === null" @click="execute({ step_id: nextPendingId })">
          执行下一条待执行步
        </button>
        <button
          class="secondary"
          style="margin-left:0.5rem"
          :disabled="busy || nextPendingId === null"
          @click="execute({ limit: batchLimit })"
        >
          按批执行 {{ batchLimit }} 步
        </button>
        <button
          class="secondary"
          style="margin-left:0.5rem"
          :disabled="busy || nextPendingId === null"
          @click="execute({})"
        >
          全部执行
        </button>
        <button
          v-if="current.target === 'intake'"
          style="margin-left:0.5rem"
          :disabled="busy || !intake.some((r) => r.batch_id === current.id && r.status === 'held')"
          @click="promoteAllHeld"
        >
          摄入前队列归并入库
        </button>
      </div>

      <div class="panel" v-if="report">
        <h3>批次报告（数字全部源自步骤表，并与各库互证）</h3>
        <div class="tiles">
          <div class="tile"><div class="hero-num">{{ report.step_total }}</div><div class="muted">步骤总数</div></div>
          <div class="tile"><div class="hero-num">{{ report.pending }}</div><div class="muted">待执行</div></div>
          <div class="tile"><div class="hero-num">{{ report.success }}</div><div class="muted">成功步</div></div>
          <div class="tile"><div class="hero-num">{{ report.failed }}</div><div class="muted">失败步</div></div>
          <div class="tile"><div class="hero-num">{{ report.canonical_writes_total }}</div><div class="muted">写入主数据</div></div>
          <div class="tile"><div class="hero-num">{{ report.conflict_steps + report.promoted_conflicts }}</div><div class="muted">冲突步</div></div>
          <div class="tile"><div class="hero-num">{{ report.staging_rows }}</div><div class="muted">Staging 行</div></div>
          <div class="tile"><div class="hero-num">{{ report.conflict_rows }}</div><div class="muted">冲突库行</div></div>
          <div class="tile"><div class="hero-num">{{ report.intake_held_rows }}</div><div class="muted">摄入前暂存</div></div>
        </div>
        <p>
          对账结果：
          <span class="tag" :class="report.reconciles ? 'ok' : 'danger'">
            {{ report.reconciles ? '全部一致' : '不一致' }}
          </span>
          <span class="mono muted" style="margin-left:0.75rem">
            staging={{ report.staging_rows }} · conflicts={{ report.conflict_rows }} ·
            canonical_now={{ report.canonical_count_now }} ·
            held={{ report.intake_held_rows }} / promoted={{ report.intake_promoted_rows }}
          </span>
        </p>
        <ul class="mono muted" style="font-size:0.8rem">
          <li>步骤恒等式：{{ report.step_total }} = {{ report.pending }}(待执行) + {{ report.success }}(成功) + {{ report.failed }}(失败)</li>
          <li>
            成功步去向：{{ report.success }} = {{ report.canonical_writes_total }}(主数据写入)
            + {{ report.conflict_steps + report.promoted_conflicts }}(冲突隔离)
            + {{ report.intake_held }}(摄入前暂存) + {{ report.skipped }}(跳过)
          </li>
        </ul>
      </div>

      <div class="panel">
        <h3>步骤列表（执行指针按 seq 推进）</h3>
        <table>
          <thead>
            <tr>
              <th>#</th><th>business_key</th><th>状态</th><th>结果</th><th>错误</th>
              <th>run</th><th>staging</th><th>conflict</th><th>intake</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="s in current.steps" :key="s.id">
              <td>{{ s.seq }}</td>
              <td class="mono">{{ s.business_key || '—' }}</td>
              <td><span class="tag" :class="stepTagClass(s)">{{ s.status }}</span></td>
              <td class="mono">{{ s.result || '—' }}</td>
              <td class="mono danger-text">{{ s.error || '—' }}</td>
              <td>{{ s.run_id || '—' }}</td>
              <td>{{ s.staging_id || '—' }}</td>
              <td>{{ s.conflict_id || '—' }}</td>
              <td>{{ s.intake_id || '—' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>

    <div class="panel" v-if="intake.length">
      <h3>摄入前队列 intake_rows</h3>
      <table>
        <thead>
          <tr><th>ID</th><th>批次</th><th>步骤</th><th>key</th><th>状态</th><th>run</th><th>staging</th><th>conflict</th></tr>
        </thead>
        <tbody>
          <tr v-for="r in intake" :key="r.id">
            <td>{{ r.id }}</td>
            <td>#{{ r.batch_id }}</td>
            <td>{{ r.step_id }}</td>
            <td class="mono">{{ r.business_key }}</td>
            <td><span class="tag" :class="r.status === 'promoted' ? 'ok' : 'warn'">{{ r.status }}</span></td>
            <td>{{ r.run_id || '—' }}</td>
            <td>{{ r.staging_id || '—' }}</td>
            <td>{{ r.conflict_id || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
<style scoped>
.danger-text {
  color: var(--danger);
  max-width: 22rem;
  word-break: break-word;
}
</style>
