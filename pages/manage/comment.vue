<script lang="ts" setup>
import { toast } from 'vue-sonner'
import type { CommentDTO } from '~/types'

useHead({
  title: '评论管理',
})

const route = useRoute()
definePageMeta({
  layout: 'backend',
})

const state = reactive({
  page: Number.parseInt(route.query.page as any as string) || 1,
  size: 20,
  begin: undefined,
  end: undefined,
  pid: '',
  username: '',
})

const columns = [{
  key: 'author.avatarUrl',
  label: '头像',
}, {
  key: 'author.username',
  label: '用户名称',
}, {
  key: 'post.title',
  label: '帖子标题',
}, {
  key: 'content',
  label: '评论内容',
}, {
  key: 'createdAt',
  label: '创建时间',
}, {
  key: 'actions',
}]

async function doRemove(row: CommentDTO) {
  try {
    assertApiSuccess(await $fetch(`/api/manage/comment/delete?cid=${row.cid}`, {
      method: 'POST',
    }), '删除评论失败')
    toast.success('操作成功')
    await reload()
  }
  catch (error) {
    toast.error(getApiErrorMessage(error, '删除评论失败'))
  }
}

interface CommentListResponse {
  success: boolean
  message?: string
  comments?: CommentDTO[]
  total?: number
}

const {
  data: commentListRes,
  pending,
  errorMessage,
  execute: reload,
} = useApiRequest(() => $fetch<CommentListResponse>('/api/manage/commentList', {
  method: 'POST',
  body: state,
}), '评论列表加载失败')

const commentList = computed(() => commentListRes.value?.comments ?? [])
const total = computed(() => commentListRes.value?.total ?? 0)

onMounted(reload)
watch(() => route.query.page, () => {
  state.page = Number.parseInt(String(route.query.page || '1')) || 1
  void reload()
})
</script>

<template>
  <UCard class="flex-1">
    <template #header>
      <div class="max-w-[300px]">
        <div class="space-y-4">
          <div class="flex flex-row gap-4">
            <UFormGroup label="用户名" name="username">
              <UInput v-model="state.username" />
            </UFormGroup>
            <UFormGroup label="帖子PID" name="pid">
              <UInput v-model="state.pid" />
            </UFormGroup>
          </div>
          <UButton type="button" @click="reload">
            查询
          </UButton>
        </div>
      </div>
    </template>
    <XManageDataState :pending="pending" :error="errorMessage" @retry="reload">
      <UTable :rows="commentList" :columns="columns">
      <template #author.avatarUrl-data="{ row }">
        <NuxtLink :to="`/member/${row.author.username}`">
          <UAvatar :src="getAvatarUrl(row.author.avatarUrl!, row.author.headImg)" size="lg" alt="Avatar" />
        </NuxtLink>
      </template>
      <template #author.username-data="{ row }">
        <UButton :to="`/member/${row.author.username}`" color="white">
          {{ row.author.username }}
        </UButton>
      </template>
      <template #post.title-data="{ row }">
        <ULink target="_blank" class="text-blue-500 max-w-[300px] line-clamp-3 text-wrap" :to="`/post/${row.pid}`">
          {{ row.post.title }}
        </ULink>
      </template>
      <template #content-data="{ row }">
        <div class="max-w-[300px] line-clamp-3 text-wrap" :title="row.content">
          {{ row.content }}
        </div>
      </template>
      <template #createdAt-data="{ row }">
        {{ dateFormat(row.createdAt) }}
      </template>
      <template #actions-data="{ row }">
        <div class="space-x-2">
          <UButton color="white" @click="doRemove(row)">
            删除
          </UButton>
        </div>
      </template>
      </UTable>
    </XManageDataState>
    <template #footer>
      <UPagination
        v-if="total > state.size" v-model="state.page" size="sm" :to="(page: number) => ({
          query: { page },
        })" class="my-2" :page-count="state.size" :total="total"
      />
    </template>
  </UCard>
</template>

<style scoped></style>
