<script setup lang="ts">
defineProps<{
  pending: boolean
  error: string
}>()

defineEmits<{
  retry: []
}>()
</script>

<template>
  <div v-if="pending" class="flex min-h-60 items-center justify-center gap-2 text-sm text-gray-500">
    <UIcon name="i-carbon-circle-dash" class="size-5 animate-spin" />
    <span>正在加载...</span>
  </div>
  <div v-else-if="error" class="flex min-h-60 flex-col items-center justify-center gap-3 px-4 text-center">
    <UIcon name="i-carbon-warning-alt" class="size-6 text-red-500" />
    <span class="text-sm text-red-500">{{ error }}</span>
    <UButton
      icon="i-carbon-renew" color="gray" variant="solid" size="sm" title="重新加载"
      @click="$emit('retry')"
    />
  </div>
  <slot v-else />
</template>
