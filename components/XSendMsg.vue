<script lang="ts" setup>
import { toast } from 'vue-sonner'
import type { SysConfigDTO } from '~/types'

const props = defineProps<{
  toUsername: string
}>()
const emit = defineEmits(['sendMsgSuccess'])

const state = reactive({
  content: '',
  toUser: props.toUsername,
})
const pending = ref(false)
const global = useGlobalConfig()
const sysconfig = global.value?.sysConfig as SysConfigDTO
const turnstileRef = ref<{ execute: () => Promise<string> } | null>(null)

async function sendMsg() {
  pending.value = true
  try {
    const token = sysconfig.turnstile?.enable ? await turnstileRef.value?.execute() || '' : ''
    await doSendMsg(token)
  }
  catch (error) {
    toast.error(error instanceof Error ? error.message : '人机验证失败')
  }
  finally {
    pending.value = false
  }
}

async function doSendMsg(token?: string) {
  const { success, message } = await $fetch('/api/member/sendMsg', {
    method: 'POST',
    body: JSON.stringify({
      content: state.content,
      toUser: state.toUser,
      token,
    }),
  })
  if (success) {
    toast.success(message)
    state.content = ''
    emit('sendMsgSuccess')
    // await navigateTo(`/member/${props.toUsername}`)
  }
  else {
    toast.error(message)
  }
}
</script>

<template>
  <div class="flex flex-col gap-2">
    <UTextarea v-model="state.content" color="white" variant="outline" :rows="5" autoresize padded :placeholder="`发送私信给${props.toUsername}`" />
    <XTurnstile
      v-if="sysconfig.turnstile?.enable"
      ref="turnstileRef"
      :site-key="sysconfig.turnstile.siteKey"
      action="sendMsg"
    />
    <UButtonGroup size="sm">
      <UButton color="primary" class="w-fit" @click="sendMsg">
        发送私信
      </UButton>
    </UButtonGroup>
  </div>
</template>

<style scoped>

</style>
