<script lang="ts" setup>
import type { ToolbarNames } from 'md-editor-v3'
import { MdEditor } from 'md-editor-v3'
import { toast } from 'vue-sonner'
import { useColorMode } from '@vueuse/core'

useHead({
  title: '系统设置',
})
definePageMeta({
  layout: 'backend',
})
const mode = useColorMode()

const toolbars: ToolbarNames[] = [
  'bold',
  'underline',
  '-',
  'strikeThrough',
  'quote',
  'unorderedList',
  'orderedList',
  '-',
  'codeRow',
  'code',
  'link',
  'image',
  'table',
  '-',
  'revoke',
  'next',
  '=',
  'preview',
]
const { data: configData } = await useFetch('/api/manage/config/get', { method: 'POST' })

const state = reactive({
  websiteName: '极简论坛',
  websiteUrl: '',
  webBgimage: '',
  websiteKeywords: '极简,论坛,极简论坛',
  websiteDescription: '极简论坛',
  favicon: '',
  pointPerPost: 5,
  pointPerPostByDay: 20,
  pointPerComment: 1,
  pointPerCommentByDay: 20,
  pointPerLikeOrDislike: 1,
  pointPerDaySignInMin: 1,
  pointPerDaySignInMax: 10,
  websiteAnnouncement: ``,
  css: '',
  js: '',
  postUrlFormat: {
    type: 'UUID',
    minNumber: 10000,
    dateFormat: 'YYYYMMDDHHmmssSSS',
  },
  invite: false,
  createInviteCodePoint: 100,
  regWithEmailCodeVerify: false,
  email: {
    apiKey: '',
    from: '',
    to: '',
    senderName: '',
  },
  turnstile: {
    siteKey: '',
    secretKey: '',
    enable: false,
  },
  notify: {
    tgBotEnabled: false,
    tgBotToken: '',
    tgBotName: '',
    tgSecret: '',
  },
  upload: {
    imgStrategy: 'r2',
    attachmentStrategy: 'r2',
  },
})

Object.assign(state, configData.value?.config)

function randomString(e: number) {
  e = e || 32
  const t = 'ABCDEFGHJKMNPQRSTWXYZabcdefhijkmnprstwxyz2345678'
  const a = t.length
  let n = ''
  for (let i = 0; i < e; i++) n += t.charAt(Math.floor(Math.random() * a))
  return n
}

function normalizeWebsiteUrl(url: string) {
  return url.trim().replace(/\/+$/, '')
}

function ensureTelegramSecret() {
  if (state.notify.tgBotEnabled && !state.notify.tgSecret) {
    state.notify.tgSecret = randomString(32)
  }
}

async function persistSettings(options: { reload?: boolean, successMessage?: string } = {}) {
  const { reload = true, successMessage = '保存成功' } = options
  if (state.turnstile.enable && (!state.turnstile.siteKey || !state.turnstile.secretKey)) {
    toast.error('启用了 Turnstile，请填写 Site Key 和 Secret Key')
    return false
  }

  if (state.regWithEmailCodeVerify && (!state.email.apiKey || !state.email.from)) {
    toast.error('启用了邮件验证码，请填写 Resend API Key 和发件邮箱')
    return false
  }

  ensureTelegramSecret()
  state.websiteUrl = normalizeWebsiteUrl(state.websiteUrl)
  state.upload = {
    imgStrategy: 'r2',
    attachmentStrategy: 'r2',
  }

  await $fetch('/api/manage/config/save', {
    method: 'POST',
    body: JSON.stringify(state),
  })

  if (successMessage) {
    toast.success(successMessage)
  }
  if (reload) {
    window.location.reload()
  }
  return true
}

async function saveSettings() {
  await persistSettings()
}

const postUrlFormatOptions = [{ value: 'UUID', label: 'UUID' }, { value: 'Number', label: '数字' }, { value: 'Date', label: '日期' }]

const items = [{
  label: '邮件设置',
  icon: 'i-carbon-email',
  defaultOpen: false,
  slot: 'email-settings',
}, {
  label: '外观设置',
  icon: 'i-carbon-machine-learning',
  defaultOpen: false,
  slot: 'appearance-settings',
}, {
  label: 'Turnstile',
  icon: 'i-carbon-security',
  defaultOpen: false,
  slot: 'turnstile-settings',
}, {
  label: '通知设置',
  icon: 'i-carbon-chat',
  defaultOpen: false,
  slot: 'notify-settings',
}]

const emailSending = ref(false)

async function testEmail() {
  emailSending.value = true
  const { success, message } = await $fetch('/api/manage/testEmail', {
    method: 'POST',
    body: JSON.stringify({ email: state.email }),
  })
  if (success) {
    toast.success('发送成功')
  }
  else {
    toast.error(message)
  }
  emailSending.value = false
}

const { copy } = useCopyToClipboard()

async function copyWebhook() {
  if (!state.notify.tgBotEnabled) {
    toast.error('请先启用 Telegram 机器人')
    return
  }
  if (!state.notify.tgBotToken) {
    toast.error('请先填写 Bot Token')
    return
  }

  state.websiteUrl = normalizeWebsiteUrl(state.websiteUrl)
  if (!state.websiteUrl) {
    toast.error('请先填写论坛地址')
    return
  }

  let websiteUrl: URL
  try {
    websiteUrl = new URL(state.websiteUrl)
  }
  catch {
    toast.error('论坛地址必须是完整的 URL')
    return
  }

  if (websiteUrl.protocol !== 'https:') {
    toast.error('Telegram Webhook 需要 https 地址')
    return
  }

  const saved = await persistSettings({ reload: false, successMessage: '' })
  if (!saved) {
    return
  }

  const webhookUrl = new URL('/api/tg', `${websiteUrl.toString().replace(/\/+$/, '')}/`)
  const apiUrl = new URL(`https://api.telegram.org/bot${state.notify.tgBotToken}/setwebhook`)
  apiUrl.searchParams.set('secret_token', state.notify.tgSecret)
  apiUrl.searchParams.set('url', webhookUrl.toString())
  copy(apiUrl.toString())
  toast.success('已保存配置并复制 WebHook 地址')
}
</script>

<template>
  <UCard class="flex-1">
    <div class="flex flex-col space-y-2">
      <div class="flex flex-row space-x-2">
        <UFormGroup label="论坛名称" name="websiteName">
          <UInput v-model="state.websiteName" autocomplete="off" />
        </UFormGroup>
        <UFormGroup label="论坛地址" name="websiteUrl">
          <UInput v-model="state.websiteUrl" autocomplete="off" />
        </UFormGroup>
      </div>
      <div class="flex flex-row space-x-2">
        <UFormGroup label="论坛背景图" name="webBgimage">
          <UInput v-model="state.webBgimage" autocomplete="off" />
        </UFormGroup>
        <UFormGroup label="论坛关键词" name="websiteKeywords">
          <UInput v-model="state.websiteKeywords" autocomplete="off" />
        </UFormGroup>
      </div>
      <div class="flex flex-row space-x-2">
        <UFormGroup label="论坛描述" name="websiteDescription">
          <UInput v-model="state.websiteDescription" autocomplete="off" />
        </UFormGroup>
        <UFormGroup label="favicon" name="favicon">
          <UInput v-model="state.favicon" autocomplete="off" />
        </UFormGroup>
      </div>
      <div class="flex flex-row space-x-2">
        <UFormGroup label="站点公告" name="websiteAnnouncement">
          <ClientOnly>
            <MdEditor
              v-model="state.websiteAnnouncement" style="height:200px;" :theme="mode as any" :preview="false"
              :toolbars="toolbars" editor-id="sysSettings" @on-upload-img="onUploadImg"
            />
          </ClientOnly>
        </UFormGroup>
      </div>

      <div class="flex flex-row space-x-2">
        <UFormGroup label="帖子链接格式定义" name="type" class="w-[225px]">
          <USelectMenu
            v-model="state.postUrlFormat.type" :options="postUrlFormatOptions" value-attribute="value"
            option-attribute="label"
          />
        </UFormGroup>
        <UFormGroup v-if="state.postUrlFormat.type === 'Number'" label="起始数字" name="minNumber">
          <UInput v-model="state.postUrlFormat.minNumber" />
        </UFormGroup>
        <UFormGroup v-if="state.postUrlFormat.type === 'Date'" label="日期格式" name="dateFormat">
          <template #hint>
            <ULink class="text-green-600 underline" to="https://day.js.org/docs/zh-CN/display/format" target="_blank">
              支持的格式
            </ULink>
          </template>
          <UInput v-model="state.postUrlFormat.dateFormat" />
        </UFormGroup>
      </div>

      <div class="flex flex-row space-x-2">
        <UFormGroup label="每次发帖获得积分" name="pointPerPost">
          <UInput v-model.number="state.pointPerPost" autocomplete="off" />
        </UFormGroup>
        <UFormGroup label="每天发帖获得积分上限" name="pointPerPostByDay">
          <UInput v-model.number="state.pointPerPostByDay" autocomplete="off" />
        </UFormGroup>
      </div>

      <div class="flex flex-row space-x-2">
        <UFormGroup label="每次回复获得积分" name="pointPerComment">
          <UInput v-model.number="state.pointPerComment" autocomplete="off" />
        </UFormGroup>
        <UFormGroup label="每天回复获得积分上限" name="pointPerCommentByDay">
          <UInput v-model.number="state.pointPerCommentByDay" autocomplete="off" />
        </UFormGroup>
      </div>

      <div class="flex flex-row space-x-2">
        <UFormGroup label="每次点赞/点踩扣减积分" name="pointPerLikeOrDislike">
          <UInput v-model.number="state.pointPerLikeOrDislike" autocomplete="off" />
        </UFormGroup>
      </div>
      <div class="flex flex-row space-x-2">
        <UFormGroup label="每天签到送积分(最小)" name="pointPerDaySignInMin">
          <UInput v-model.number="state.pointPerDaySignInMin" autocomplete="off" />
        </UFormGroup>
        <UFormGroup label="每天签到送积分(最大)" name="pointPerDaySignInMax">
          <UInput v-model.number="state.pointPerDaySignInMax" autocomplete="off" />
        </UFormGroup>
      </div>
      <div class="flex flex-row space-x-2">
        <UFormGroup label="是否启用邀请注册" name="pointPerDaySignInMin">
          <UToggle v-model="state.invite" />
        </UFormGroup>
      </div>

      <div class="flex flex-row space-x-2">
        <UFormGroup label="每次生成邀请码需要积分" name="createInviteCodePoint">
          <UInput v-model.number="state.createInviteCodePoint" autocomplete="off" />
        </UFormGroup>
      </div>
      <UAccordion :items="items" :ui="{ container: 'max-w-[500px]' }">
        <template #email-settings>
          <div class="flex flex-col space-y-2 ">
            <UFormGroup label="开启邮件验证注册用户" name="regWithEmailCodeVerify">
              <USelectMenu
                v-model="state.regWithEmailCodeVerify" value-attribute="value" option-attribute="label"
                :options="[{ value: true, label: '是' }, { value: false, label: '否' }]"
              />
            </UFormGroup>
            <UFormGroup label="Resend API Key" name="apiKey">
              <template #hint>
                需要在 Resend 后台创建 API Key
              </template>
              <UInput v-model="state.email.apiKey" type="password" autocomplete="off" />
            </UFormGroup>
            <UFormGroup label="发件邮箱" name="from">
              <template #hint>
                这里填写 Resend 已验证域名下的邮箱地址
              </template>
              <UInput v-model="state.email.from" autocomplete="off" />
            </UFormGroup>
            <UFormGroup label="发件人名称" name="senderName">
              <UInput v-model="state.email.senderName" autocomplete="off" />
            </UFormGroup>
            <UButtonGroup size="sm" orientation="horizontal" class="my-2">
              <UInput v-model="state.email.to" placeholder="测试邮件接收地址" />
              <UButton class="w-fit " size="xs" :loading="emailSending" @click="testEmail">
                测试发送邮件
              </UButton>
            </UButtonGroup>
          </div>
        </template>

        <template #appearance-settings>
          <div class="flex flex-col space-y-2 ">
            <div class="flex flex-row space-x-2">
              <UFormGroup label="自定义css" name="css" class="w-[500px]">
                <UTextarea v-model="state.css" :rows="10" />
              </UFormGroup>
            </div>

            <div class="flex flex-row space-x-2">
              <UFormGroup label="自定义JS" name="css" class="w-[500px]">
                <UTextarea v-model="state.js" :rows="10" />
              </UFormGroup>
            </div>
          </div>
        </template>

        <template #turnstile-settings>
          <div class="flex flex-col space-y-2 ">
            <div class="flex flex-row space-x-2">
              <UFormGroup label="是否启用" name="turnstileEnabled" class="w-[500px]">
                <USelectMenu
                  v-model="state.turnstile.enable" value-attribute="value" option-attribute="label"
                  :options="[{ value: true, label: '是' }, { value: false, label: '否' }]"
                />
              </UFormGroup>
            </div>

            <div class="flex flex-row space-x-2">
              <UFormGroup label="Site Key" name="css" class="w-[500px]">
                <UInput v-model="state.turnstile.siteKey" autocomplete="off" />
              </UFormGroup>
              <UFormGroup label="Secret Key" name="css" class="w-[500px]">
                <UInput v-model="state.turnstile.secretKey" autocomplete="off" />
              </UFormGroup>
            </div>
          </div>
        </template>

        <template #notify-settings>
          <div class="flex flex-col space-y-2 ">
            <div class="flex flex-row space-x-2">
              <UFormGroup label="是否启用Telegram机器人" name="tgBotEnabled" class="w-[500px]">
                <UButtonGroup>
                  <USelectMenu
                    v-model="state.notify.tgBotEnabled" class="w-[400px]" value-attribute="value"
                    option-attribute="label" :options="[{ value: true, label: '是' }, { value: false, label: '否' }]"
                  />
                  <UButton @click="copyWebhook">
                    复制WebHook地址
                  </UButton>
                </UButtonGroup>
              </UFormGroup>
            </div>

            <div class="flex flex-row space-x-2">
              <UFormGroup label="Bot Token" name="tgBotToken" class="w-[500px]">
                <UInput v-model="state.notify.tgBotToken" autocomplete="off" />
              </UFormGroup>
              <UFormGroup label="Bot Name" name="tgBotName" class="w-[500px]" hint="显示在消息页面">
                <UInput v-model="state.notify.tgBotName" autocomplete="off" />
              </UFormGroup>
            </div>
          </div>
        </template>
      </UAccordion>

      <UButton class="w-fit" @click="saveSettings">
        保存
      </UButton>
    </div>
  </UCard>
</template>

<style scoped></style>
