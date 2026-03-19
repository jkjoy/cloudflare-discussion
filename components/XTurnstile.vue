<script lang="ts" setup>
const props = defineProps<{
  siteKey: string
  action: string
}>()

const containerId = `turnstile-${Math.random().toString(36).slice(2, 10)}`
const widgetId = ref<string | null>(null)
let pendingResolve: ((token: string) => void) | null = null
let pendingReject: ((error: Error) => void) | null = null
let pendingExecution: Promise<string> | null = null

function clearPending() {
  pendingResolve = null
  pendingReject = null
  pendingExecution = null
}

function rejectPending(message: string) {
  pendingReject?.(new Error(message))
  clearPending()
}

async function waitForTurnstile() {
  if (window.turnstile) {
    return window.turnstile
  }

  return await new Promise<NonNullable<typeof window.turnstile>>((resolve, reject) => {
    const startedAt = Date.now()
    const timer = window.setInterval(() => {
      if (window.turnstile) {
        window.clearInterval(timer)
        resolve(window.turnstile)
        return
      }

      if (Date.now() - startedAt > 10000) {
        window.clearInterval(timer)
        reject(new Error('Turnstile 加载超时'))
      }
    }, 50)
  })
}

async function renderWidget() {
  if (widgetId.value) {
    return widgetId.value
  }

  await nextTick()
  const target = document.getElementById(containerId)
  if (!target) {
    throw new Error('Turnstile 容器不存在')
  }

  const turnstile = await waitForTurnstile()
  widgetId.value = turnstile.render(`#${containerId}`, {
    sitekey: props.siteKey,
    action: props.action,
    execution: 'execute',
    appearance: 'interaction-only',
    callback(token: string) {
      pendingResolve?.(token)
      clearPending()
    },
    'expired-callback'() {
      rejectPending('人机验证已过期，请重试')
    },
    'error-callback'() {
      rejectPending('人机验证失败，请重试')
    },
  })

  return widgetId.value
}

async function execute() {
  if (pendingExecution) {
    return await pendingExecution
  }

  const turnstile = await waitForTurnstile()
  const id = await renderWidget()
  turnstile.reset(id)

  pendingExecution = new Promise<string>((resolve, reject) => {
    pendingResolve = resolve
    pendingReject = reject
    try {
      turnstile.execute(`#${containerId}`)
    }
    catch (error) {
      clearPending()
      reject(error instanceof Error ? error : new Error('Turnstile 执行失败'))
    }
  })

  return await pendingExecution
}

function reset() {
  if (widgetId.value && window.turnstile) {
    window.turnstile.reset(widgetId.value)
  }
  clearPending()
}

onMounted(() => {
  renderWidget().catch(() => {})
})

onBeforeUnmount(() => {
  if (widgetId.value && window.turnstile) {
    window.turnstile.remove(widgetId.value)
  }
  clearPending()
})

defineExpose({
  execute,
  reset,
})
</script>

<template>
  <div :id="containerId" class="turnstile-slot" />
</template>

<style scoped>
.turnstile-slot {
  min-height: 0;
}
</style>
