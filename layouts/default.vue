<script lang="ts" setup>
import { useTitle } from '@vueuse/core'
import { Toaster } from 'vue-sonner'
import type { SysConfigDTO, TagDTO, UserDTO } from '~/types'

const userinfo = useState<UserDTO>('userinfo', () => ({} as UserDTO))
const keyWords = ref('')
const config = useRuntimeConfig()
const token = useCookie(config.public.tokenKey)
const route = useRoute()
const router = useRouter()
const sliderOpen = useState('sliderOpen', () => {
  return false
})

const global = useGlobalConfig()

let profileLoadPending = false
let profileLoadTimer: ReturnType<typeof setTimeout> | null = null

async function loadProfile(immediate = false) {
  if (!process.client) return
  if (!token.value) {
    userinfo.value = {} as UserDTO
    return
  }
  if (profileLoadPending && !immediate) return

  if (!immediate) {
    if (profileLoadTimer) clearTimeout(profileLoadTimer)
    await new Promise<void>(resolve => {
      profileLoadTimer = setTimeout(resolve, 300)
    })
  }

  profileLoadPending = true
  try {
    const userinfoRes = await $fetch<UserDTO>('/api/member/profile', {
      method: 'POST',
    })
    if (userinfoRes) {
      userinfo.value = userinfoRes
      if (userinfoRes.unRead > 0) {
        const title = useTitle()
        title.value = `${title.value}(${userinfoRes.unRead})`
      }
    }
  }
  catch (error) {
    const status = error && typeof error === 'object' && 'statusCode' in error
      ? Number(error.statusCode)
      : 0
    if (status === 401) {
      token.value = null
      userinfo.value = {} as UserDTO
    }
  }
  finally {
    profileLoadPending = false
  }
}

const sysconfig = computed(() => (global.value?.sysConfig ?? {}) as SysConfigDTO)
// 版本号来自构建时注入的 NUXT_PUBLIC_APP_VERSION(deploy 时等于 git tag),去掉前导 v 以配合页脚已有的 "版本v" 前缀
const version = String(config.public.appVersion || '1.0').replace(/^v/, '')

userCardChanged.on(() => loadProfile())

watch(token, () => loadProfile(true))

if (process.client) {
  void loadProfile()
}

// sysconfig 变为响应式后，useHead 统一用 computed 形式，config 后加载时自动生效
useHead(computed(() => {
  const cfg = sysconfig.value
  return {
    style: cfg.css ? [{ innerHTML: cfg.css }] : [],
    script: [
      ...(cfg.js ? [{ type: 'text/javascript', innerHTML: cfg.js }] : []),
      ...(cfg.turnstile?.enable ? [{
        type: 'text/javascript',
        src: 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit',
        defer: true,
      }] : []),
    ],
    link: cfg.favicon ? [{ rel: 'shortcut icon', href: cfg.favicon }] : [],
  }
}))

useHead(() => {
  const userCss = userinfo.value?.css
  const userJs = userinfo.value?.js

  return {
    style: userCss
      ? [
          {
            key: 'user-custom-css',
            innerHTML: userCss,
          },
        ]
      : [],
    script: userJs
      ? [
          {
            key: 'user-custom-js',
            type: 'text/javascript',
            innerHTML: userJs,
          },
        ]
      : [],
  }
})

const open = ref(false)
function search() {
  if (!keyWords.value)
    return
  router.push({ path: '/', query: { key: keyWords.value.trim() } })
  keyWords.value = ''
  open.value = false
}

useHead({
  title: '首页',
})

const tag = ref<TagDTO>()
watch(() => route.fullPath, async (n) => {
  if (n.startsWith('/post/') || n === '/') {
    userCardChanged.emit()
  }
})
watch(() => route.fullPath, async () => {
  if (!process.client) return
  if (route.fullPath.startsWith('/go/')) {
    const name = String(route.params.tag || '')
    if (!name) {
      tag.value = undefined
      return
    }
    const res = await $fetch<{ tags: Array<TagDTO> }>(`/api/go/list?name=${encodeURIComponent(name)}`, {
      method: 'GET',
    })
    tag.value = res.tags[0] as TagDTO
  }
}, { immediate: true })

const color = useColorMode()
const theme = ref<'light' | 'dark'>(color.value === 'dark' ? 'dark' : 'light')
themeChanged.on((val) => {
  if (!process.client) return
  theme.value = val === 'dark' ? 'dark' : 'light'
})

function inputKey(value: any) {
  if (value.target?.value.length > 1) {
    open.value = true
  }
  else {
    open.value = false
  }
}
function GoogleSearch() {
  const webUrl = sysconfig.value.websiteUrl || window.location.hostname
  const url = `https://www.google.com/search?q=site:${webUrl}%20${encodeURIComponent(keyWords.value)}`
  window.open(url, '_blank')
}
</script>

<template>
  <div class="dark:bg-slate-800 min-h-screen">
    <USlideover v-model="sliderOpen" class="md:hidden overflow-y-auto" side="left">
      <div class="p-4 flex-1 space-y-4 bg-slate-700">
        <UIcon name="i-carbon-close-large" class="size-5 text-white" @click="sliderOpen = false" />
        <UCard class="w-full mt-2" :ui="{ header: { padding: 'px-0 py-0 sm:px-0' } }">
          <UInput
            v-model="keyWords" icon="i-heroicons-magnifying-glass-20-solid" size="sm" color="white"
            :trailing="false" placeholder="Search..." @keydown.enter="search"
          />
        </UCard>
        <XUserCard v-if="userinfo && userinfo.username" />

        <UCard
          v-if="sysconfig && sysconfig.websiteAnnouncement" class="w-full mt-2"
          :ui="{ header: { padding: 'px-0 py-0 sm:px-0' } }"
        >
          <template #header>
            <div class="px-4 py-1 rounded-t sm:px-6 text-primary bg-gray-100 dark:bg-slate-500">
              关于本站
            </div>
          </template>
          <div class="text-sm">
            <LazyXMarkdownPreview :model-value="sysconfig.websiteAnnouncement" editor-id="websiteAnnouncement" no-highlight />
          </div>
        </UCard>
        <UCard
          v-if="route.fullPath.startsWith('/go/') && tag" class="w-full mt-2"
          :ui="{ header: { padding: 'px-0 py-0 sm:px-0' } }"
        >
          <template #header>
            <div class="px-4 py-1 rounded-t sm:px-6 text-primary bg-gray-100 dark:bg-slate-500">
              {{ tag.name }}
            </div>
          </template>
          <div class="text-sm">
            {{ tag.desc }}
          </div>
        </UCard>
        <LazyXHotUser />
      </div>
    </USlideover>

    <x-header :site-name="sysconfig?.websiteName" />
    <div
      v-if="sysconfig.webBgimage" :style="{ backgroundImage: `url(${sysconfig.webBgimage})` }"
      class="hidden md:block fixed w-screen h-screen bg-cover bg-no-repeat bg-[100%] z-0"
    />
    <div class="main flex max-w-[1080px] mx-auto h-full gap-4 relative">
      <div class="flex-1 w-full">
        <slot />
      </div>
      <div class="right-bar space-y-4 w-[300px] hidden md:block">
        <UCard class="w-full mt-2 card" :ui="{ header: { padding: 'px-0 py-0 sm:px-0' } }">
          <UInput
            v-model="keyWords" icon="i-heroicons-magnifying-glass-20-solid" size="sm" color="white" :trailing="false"
            placeholder="Search..." @input="inputKey" @keydown.enter="search"
          />
          <UPopover v-model:open="open" :popper="{ offsetDistance: -10 }">
            <div />
            <template #panel>
              <div class="w-[250px] cursor-pointer">
                <div class="px-4 py-2" @click="search">
                  搜索帖子：{{ keyWords }}
                </div>
                <UDivider />
                <div class="px-4 py-2" @click="GoogleSearch">
                  谷歌搜索：{{ keyWords }}
                </div>
              </div>
            </template>
          </UPopover>
        </UCard>
        <XUserCard v-if="userinfo && userinfo.username" />
        <UCard
          v-if="route.fullPath.startsWith('/go/') && tag" class="profile w-full mt-2"
          :ui="{ header: { padding: 'px-0 py-0 sm:px-0' } }"
        >
          <template #header>
            <div class="px-4 py-1 rounded-t sm:px-6 text-primary bg-gray-100 dark:bg-slate-500">
              {{ tag.name }}
            </div>
          </template>
          <div class="text-sm">
            {{ tag.desc }}
          </div>
        </UCard>
        <UCard
          v-if="sysconfig && sysconfig.websiteAnnouncement" class="ann w-full mt-2"
          :ui="{ header: { padding: 'px-0 py-0 sm:px-0' } }"
        >
          <template #header>
            <div class="px-4 py-1 rounded-t sm:px-6 text-primary bg-gray-100 dark:bg-slate-500">
              关于本站
            </div>
          </template>
          <div class="text-sm">
            <LazyXMarkdownPreview :model-value="sysconfig.websiteAnnouncement" editor-id="websiteAnnouncement" no-highlight />
          </div>
        </UCard>
        <LazyXHotUser />
      </div>
    </div>
    <XFooter :version="version" />
  </div>
  <Toaster position="top-center" rich-colors :duration="1000" />
</template>

<style scoped>
   .card div.relative:nth-of-type(2){
    height: 0px;
   }
</style>
