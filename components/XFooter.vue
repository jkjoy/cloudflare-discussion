<script lang="ts" setup>
withDefaults(defineProps<{
  version: string
}>(), {
  version: '1.0',
})

const currentYear = new Date().getFullYear()
</script>

<template>
  <div class="footer max-w-[1080px] mx-auto flex gap-2 text-xs my-4 px-2 md:px-0 pb-4 text-gray-400 justify-between md:justify-normal relative">
    <span class="">
      @{{ currentYear }}
    </span>
    <span>版本v{{ version }}</span>
  </div>
</template>

<style></style>
