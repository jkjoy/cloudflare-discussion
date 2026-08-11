<script lang="ts" setup>
import { toast } from 'vue-sonner'
import type { TagDTO } from '~/types'

useHead({
  title: '标签管理',
})
const route = useRoute()
definePageMeta({
  layout: 'backend',
})

const page = ref(Number.parseInt(route.query.page as any as string) || 1)
const size = ref(20)

const saveState = reactive({
  name: '',
  desc: '',
  enName: '',
  id: 0,
})

const isOpen = ref(false)

function doEdit(row: TagDTO) {
  saveState.name = row.name
  saveState.desc = row.desc
  saveState.enName = row.enName
  saveState.id = row.id
  isOpen.value = true
}

function doAdd() {
  saveState.name = ''
  saveState.desc = ''
  saveState.enName = ''
  saveState.id = 0
  isOpen.value = true
}

const columns = [{
  key: 'name',
  label: '名称',
}, {
  key: 'enName',
  label: '编码',
}, {
  key: 'desc',
  label: '描述',
}, {
  key: 'hot',
  label: '是否热门',
}, {
  key: 'count',
  label: '帖子数量',
}, {
  key: 'actions',
}]

interface TagListResponse {
  success: boolean
  message?: string
  tags?: TagDTO[]
  total?: number
}

const {
  data: tagListRes,
  pending,
  errorMessage,
  execute: reload,
} = useApiRequest(() => $fetch<TagListResponse>('/api/manage/tagList', {
  method: 'POST',
  body: {
    page: page.value,
    size: size.value,
  },
}), '标签列表加载失败')
const tagList = computed(() => tagListRes.value?.tags ?? [])
const total = computed(() => tagListRes.value?.total ?? 0)

onMounted(reload)

async function saveTag() {
  if (!saveState.enName.trim() || !saveState.name.trim() || !saveState.desc.trim()) {
    toast.error('请填写完整,都是必填字段')
    return
  }
  try {
    const res = assertApiSuccess(await $fetch('/api/manage/saveTag', {
      method: 'POST',
      body: saveState,
    }), '保存失败')
    isOpen.value = false
    await reload()
    await refreshNuxtData(['hotTagLists', 'allTagLists'])
    toast.success('保存成功')
  }
  catch (error) {
    toast.error(getApiErrorMessage(error, '保存失败'))
  }
}

async function toggleHot(tag: TagDTO) {
  try {
    assertApiSuccess(await $fetch('/api/manage/toggleHot', {
      method: 'POST',
      body: { id: tag.id },
    }), '更新热门状态失败')
    await reload()
    await refreshNuxtData(['hotTagLists', 'allTagLists'])
  }
  catch (error) {
    toast.error(getApiErrorMessage(error, '更新热门状态失败'))
  }
}

watch(() => route.query.page, () => {
  page.value = Number.parseInt(String(route.query.page || '1')) || 1
  void reload()
})
</script>

<template>
  <UCard class="flex-1">
    <template #header>
      <UButton @click="doAdd">
        新增标签
      </UButton>
    </template>
    <XManageDataState :pending="pending" :error="errorMessage" @retry="reload">
      <UTable :rows="tagList" :columns="columns">
      <template #avatarUrl-data="{ row }">
        <NuxtLink :to="`/member/${row.username}`">
          <UAvatar :src="getAvatarUrl(row.avatarUrl!, row.headImg)" size="lg" alt="Avatar" />
        </NuxtLink>
      </template>
      <template #actions-data="{ row }">
        <div class="space-x-2">
          <UButton color="white" @click="doEdit(row)">
            编辑
          </UButton>
          <UButton color="gray" @click="toggleHot(row)">
            {{ row.hot ? '取消' : '设为' }}热门
          </UButton>
        </div>
      </template>
      <template #hot-data="{ row }">
        {{ row.hot ? '是' : '否' }}
      </template>
      </UTable>
    </XManageDataState>
    <template #footer>
      <UPagination
        v-if="total > size" v-model="page" size="sm" :to="(page: number) => ({
          query: { page },
        })" class="my-2" :page-count="size" :total="total || 0"
      />
    </template>
  </UCard>

  <UModal v-model="isOpen">
    <div class="p-4 space-y-4">
      <UFormGroup label="名称" name="name">
        <UInput v-model="saveState.name" />
      </UFormGroup>
      <UFormGroup label="编码" name="enName">
        <UInput v-model="saveState.enName" />
      </UFormGroup>
      <UFormGroup label="描述" name="desc">
        <UTextarea v-model="saveState.desc" />
      </UFormGroup>
      <UButton @click="saveTag">
        提交
      </UButton>
    </div>
  </UModal>
</template>

<style scoped></style>
