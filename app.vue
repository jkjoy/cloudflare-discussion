<script setup lang="ts">
import type { SysConfigDTO } from './types'

const global = useGlobalConfig()
const { data: configData } = await useFetch('/api/config', {
  method: 'GET',
})

const sysConfig = configData.value?.data as unknown as SysConfigDTO

const version = configData.value?.version

global.value = { sysConfig, version: version! }

useHead({
  // as a string,
  // where `%s` is replaced with the title
  titleTemplate: `%s - ${global.value.sysConfig.websiteName}`,
  meta: [
    { name: 'keywords', content: global.value.sysConfig.websiteKeywords },
    { name: 'description', content: global.value.sysConfig.websiteDescription },
  ],
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
