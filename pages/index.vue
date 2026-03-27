<script lang="ts" setup>
import type { PostDTO } from '~/types'

interface PostListResponse {
  posts: PostDTO[]
  total: number
}

const route = useRoute()
function buildPostListUrl(page: number, key?: string) {
  const params = new URLSearchParams({
    page: String(page),
    size: String(state.size),
  })
  const keyword = String(key || '').trim()
  if (keyword) {
    params.set('key', keyword)
  }
  return `/api/post/list?${params.toString()}`
}

const state = reactive({
  page: 1,
  size: 20,
  key: route.query.key,
})

state.page = Number.parseInt(route.query.page as any as string) || 1
const { data, pending } = useLazyFetch<PostListResponse>(buildPostListUrl(state.page, state.key as string), {
  method: 'GET',
  default: () => ({
    posts: [],
    total: 0,
  }),
})

watch(() => route.fullPath, async () => {
  state.page = Number.parseInt(route.query.page as any as string) || 1
  state.key = route.query.key as any as string
  const res = await $fetch(buildPostListUrl(state.page, state.key as string), {
    method: 'GET',
  })
  data.value = res
})

const postList = computed(() => {
  return data.value?.posts || []
})

const totalPosts = computed(() => {
  return data.value?.total || 0
})

const { getAbsoluteUrl } = useAbsoluteUrl()
function getQuery(page: number) {
  return {
    query: { ...route.query, page },
  }
}
useHead({
  title: `首页`,
  link: [
    {
      rel: 'canonical',
      href: getAbsoluteUrl(route.path),
    },
  ],
})
</script>

<template>
  <UCard class="h-full overflow-y-auto mt-0 md:mt-2 w-full min-h-60" :ui="{ rounded: 'rounded-none md:rounded-lg', body: { padding: 'px-0 sm:p-0' }, header: { padding: ' py-2 sm:px-4 px-2' } }">
    <template #header>
      <XTagList />
    </template>
    <div class="flex flex-col divide-y divide-gray-300 dark:divide-slate-700">
      <XPost v-for="post in postList" :key="post.pid" :show-avatar="true" v-bind="post" />
      <div v-if="pending && postList.length === 0" class="space-y-3 p-4">
        <div v-for="index in 6" :key="index" class="animate-pulse space-y-2">
          <div class="h-4 w-3/5 rounded bg-gray-200 dark:bg-slate-700" />
          <div class="h-3 w-2/5 rounded bg-gray-100 dark:bg-slate-800" />
        </div>
      </div>
      <div v-else-if="postList.length === 0" class="p-4 text-sm">
        暂无帖子,注册登录发言吧
      </div>
    </div>
    <UPagination
      v-if="totalPosts > state.size"
      v-model="state.page" size="sm" class="m-2 p-2" :to="getQuery" :page-count="state.size"
      :total="totalPosts"
    />
  </UCard>

  <XScrollToolbar />
</template>

<style></style>
