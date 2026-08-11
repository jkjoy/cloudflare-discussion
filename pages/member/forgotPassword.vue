<script lang="ts" setup>
import { toast } from 'vue-sonner'

const state = reactive({
  identify: '',
  emailCode: '',
  emailCodeKey: '',
  password: '',
  repeatPassword: '',
})
const pending = ref(false)
const resetting = ref(false)

function getErrorMessage(error: unknown, fallback: string) {
  if (error && typeof error === 'object' && 'data' in error) {
    const data = error.data
    if (data && typeof data === 'object' && 'message' in data && typeof data.message === 'string') {
      return data.message
    }
  }
  return error instanceof Error && error.message ? error.message : fallback
}

async function sendResetPwdEmail() {
  if (!state.identify) {
    toast.error('请输入用户名或者邮箱')
    return
  }
  pending.value = true
  try {
    const result = await $fetch('/api/member/sendForgotPasswordEmail', {
      method: 'POST',
      body: { identify: state.identify },
    })
    if (result?.success) {
      toast.success(result.message || '发送邮件成功')
      state.emailCodeKey = result.emailCodeKey || ''
    }
    else {
      toast.error(result?.message || '发送重置邮件失败')
    }
  }
  catch (error) {
    toast.error(getErrorMessage(error, '发送重置邮件失败'))
  }
  finally {
    pending.value = false
  }
}

async function doResetPwd() {
  if (!state.emailCode) {
    toast.error('请输入验证码')
    return
  }
  if (!state.password) {
    toast.error('请输入新密码')
    return
  }

  if (state.password !== state.repeatPassword) {
    toast.error('两次密码不一致')
    return
  }

  resetting.value = true
  try {
    const { success, message } = await $fetch('/api/member/resetPwd', {
      method: 'POST',
      body: state,
    })
    if (success) {
      toast.success('修改成功,快去登录吧')
      await navigateTo('/member/login')
    }
    else {
      toast.error(message || '重置密码失败')
    }
  }
  catch (error) {
    toast.error(getErrorMessage(error, '重置密码失败'))
  }
  finally {
    resetting.value = false
  }
}
</script>

<template>
  <UCard class="w-full mt-2">
    <template #header>
      <div class="text-center text-sm">
        找回密码
      </div>
    </template>
    <div class="flex flex-col my-2 lg:w-[300px] mx-auto">
      <UForm :state="state" class="space-y-4" autocomplete="off">
        <UFormGroup label="用户名或者邮箱" name="identify">
          <UInput v-model="state.identify" autocomplete="off" :disabled="state.emailCodeKey !== ''" />
        </UFormGroup>
        <UButton v-if="!state.emailCodeKey" :loading="pending" @click="sendResetPwdEmail">
          申请重置密码
        </UButton>
        <template v-if="state.emailCodeKey">
          <UFormGroup label="验证码" name="identify">
            <UInput v-model="state.emailCode" autocomplete="off" />
          </UFormGroup>
          <UFormGroup label="新密码" name="password">
            <UInput v-model="state.password" autocomplete="off" type="password" />
          </UFormGroup>
          <UFormGroup label="重复新密码" name="repeatPassword">
            <UInput v-model="state.repeatPassword" autocomplete="off" type="password" />
          </UFormGroup>
          <UButton :loading="resetting" :disabled="resetting" @click="doResetPwd">
            修改密码
          </UButton>
        </template>
      </UForm>
    </div>
  </UCard>
</template>

<style scoped></style>
