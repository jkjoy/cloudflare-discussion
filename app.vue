<script setup lang="ts">
import type { SysConfigDTO } from './types'

const global = useGlobalConfig()
const { data: configData } = await useFetch('/api/config', {
  method: 'GET',
})

const sysConfig = (configData.value?.data as unknown as SysConfigDTO) ?? {} as SysConfigDTO
const version = configData.value?.version

global.value = { sysConfig, version: version || '' }

useHead({
  titleTemplate: `%s - ${sysConfig?.websiteName || 'Discussion'}`,
  meta: [
    { name: 'keywords', content: sysConfig?.websiteKeywords || '' },
    { name: 'description', content: sysConfig?.websiteDescription || '' },
  ],
})

// SSR 时 config 可能拿不到（开发环境 API 未启动），客户端挂载后补充加载
onMounted(async () => {
  if (global.value.sysConfig?.websiteName) return
  try {
    const res = await $fetch<{ data: SysConfigDTO, version: string }>('/api/config')
    if (res?.data) {
      global.value = { sysConfig: res.data, version: res.version || '' }
    }
  }
  catch {}
})
</script>

<template>
  <NuxtLoadingIndicator />
  <NuxtLayout>
    <NuxtPage :transition="{ name: 'page', mode: 'out-in' }" />
  </NuxtLayout>
  <UModals />
</template>

<style>
html,
body,
#__nuxt {
  width: 100%;
  height: 100%;
  @apply bg-slate-50 dark:bg-slate-800;
}

.page-enter-active {
  transition: opacity 0.12s ease;
}
.page-leave-active {
  transition: opacity 0.07s ease;
}
.page-enter-from,
.page-leave-to {
  opacity: 0;
}
</style>
