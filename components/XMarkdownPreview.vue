<script lang="ts" setup>
import { MdPreview } from 'md-editor-v3'
import 'md-editor-v3/lib/style.css'

withDefaults(defineProps<{
  modelValue: string
  editorId: string
  noHighlight?: boolean
}>(), {
  noHighlight: false,
})
</script>

<template>
  <MdPreview :model-value="modelValue" :editor-id="editorId" no-mermaid no-katex :no-highlight="noHighlight" />
</template>
