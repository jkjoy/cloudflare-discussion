<script lang="ts" setup>
import { toast } from 'vue-sonner'
import type { TitleDTO, UserDTO } from '~/types'

const route = useRoute()
definePageMeta({
  layout: 'backend',
})
const selectedUid = ref('')

async function banUser(day: number) {
  try {
    assertApiSuccess(await $fetch('/api/manage/member/banUser', {
      method: 'POST',
      body: { day, uid: selectedUid.value },
    }), '禁言操作失败')
    await reload()
  }
  catch (error) {
    toast.error(getApiErrorMessage(error, '禁言操作失败'))
  }
}

const items = [
  [{
    label: '一天',
    click: () => { banUser(1) },
  }],
  [{
    label: '三天',
    click: () => { banUser(3) },
  }],
  [{
    label: '一周',
    click: () => { banUser(7) },
  }],
  [{
    label: '一月',
    click: () => { banUser(31) },
  }],
  [{
    label: '永久',
    click: () => { banUser(99999) },
  }],
]

const state = reactive({
  page: Number.parseInt(route.query.page as any as string) || 1,
  size: 20,
  begin: undefined,
  end: undefined,
  username: '',
})

const columns = [{
  key: 'avatarUrl',
  label: '头像',
}, {
  key: 'username',
  label: '用户名称',
}, {
  key: 'role',
  label: '角色',
}, {
  key: 'titles',
  label: '头衔',
}, {
  key: 'email',
  label: '邮箱',
}, {
  key: 'createdAt',
  label: '加入时间',
}, {
  key: 'lastLogin',
  label: '最后登录',
}, {
  key: 'bannedEnd',
  label: '禁言结束',
}, {
  key: 'point',
  label: '积分',
}, {
  key: '_count.posts',
  label: '帖子',
}, {
  key: '_count.comments',
  label: '评论',
}, {
  key: 'actions',
}]

async function revokeBanned(row: UserDTO) {
  try {
    assertApiSuccess(await $fetch('/api/manage/member/revokeBanUser', {
      method: 'POST',
      body: { uid: row.uid },
    }), '撤销禁言失败')
    await reload()
  }
  catch (error) {
    toast.error(getApiErrorMessage(error, '撤销禁言失败'))
  }
}

interface UserListResponse {
  success: boolean
  message?: string
  users?: UserDTO[]
  total?: number
}

interface TitleListResponse {
  success: boolean
  message?: string
  titles?: TitleDTO[]
}

const {
  data: userListRes,
  pending: userListPending,
  errorMessage: userListError,
  execute: reload,
} = useApiRequest(() => $fetch<UserListResponse>('/api/manage/userList', {
  method: 'POST',
  body: state,
}), '用户列表加载失败')
const userList = computed(() => userListRes.value?.users ?? [])
const total = computed(() => userListRes.value?.total ?? 0)

const {
  data: titleListRes,
  pending: titleListPending,
  errorMessage: titleListError,
  execute: reloadTitles,
} = useApiRequest(() => $fetch<TitleListResponse>('/api/manage/title/titleList', {
    method: 'POST',
    body: { onlyEnabled: true },
  }), '头衔列表加载失败')

const pagePending = computed(() => userListPending.value || titleListPending.value)
const pageError = computed(() => userListError.value || titleListError.value)

async function loadPage() {
  await Promise.all([reload(), reloadTitles()])
}

onMounted(loadPage)
watch(() => route.query.page, () => {
  state.page = Number.parseInt(String(route.query.page || '1')) || 1
  void reload()
})

const titles = computed(() => {
  return (titleListRes.value?.titles ?? []).map((x) => {
    return [{
      label: x.title,
      click: () => {
        assignTitle(x.title)
      },
    }]
  })
})

async function assignTitle(title: string) {
  try {
    assertApiSuccess(await $fetch('/api/manage/title/assign', {
      method: 'POST',
      body: { title, uid: selectedUid.value },
    }), '分配头衔失败')
    await reload()
  }
  catch (error) {
    toast.error(getApiErrorMessage(error, '分配头衔失败'))
  }
}

async function removeTitle(userId: number, titleId: number) {
  try {
    assertApiSuccess(await $fetch('/api/manage/title/remove', {
      method: 'POST',
      body: { userId, titleId },
    }), '移除头衔失败')
    await reload()
  }
  catch (error) {
    toast.error(getApiErrorMessage(error, '移除头衔失败'))
  }
}

const sendPointOpen = ref(false)
const pointOptions = [{ key: 'PUNISH', label: '处罚' }, { key: 'SEND', label: '赠送' }]

const pointActionState = reactive({
  reason: 'SEND',
  amount: '',
  uid: '',
  remark: '',
})
function openPointAction(uid: string) {
  pointActionState.reason = 'SEND'
  pointActionState.amount = ''
  pointActionState.remark = ''
  pointActionState.uid = uid
  sendPointOpen.value = true
}
async function makePointAction() {
  if (!pointActionState.amount) {
    toast.error('请填写数量')
    return
  }
  if (!pointActionState.reason) {
    toast.error('请填写动作')
    return
  }
  if (!pointActionState.remark) {
    toast.error('请填写事由')
    return
  }
  try {
    assertApiSuccess(await $fetch('/api/manage/member/point', {
      method: 'POST',
      body: pointActionState,
    }), '积分操作失败')
    sendPointOpen.value = false
    await reload()
  }
  catch (error) {
    toast.error(getApiErrorMessage(error, '积分操作失败'))
  }
}
</script>

<template>
  <UModal v-model="sendPointOpen">
    <div class="p-4 space-y-4">
      <UFormGroup label="动作" name="reason">
        <USelectMenu
          v-model="pointActionState.reason" value-attribute="key" option-attribute="label"
          :options="pointOptions"
        />
      </UFormGroup>
      <UFormGroup label="数量" name="status">
        <UInput v-model="pointActionState.amount" />
      </UFormGroup>
      <UFormGroup label="事由" name="style">
        <UInput v-model="pointActionState.remark" />
      </UFormGroup>
      <UButton @click="makePointAction">
        提交
      </UButton>
    </div>
  </UModal>

  <UCard class="flex-1 overflow-hidden">
    <template #header>
      <div class="max-w-[300px]">
        <div class="space-y-4">
          <UFormGroup label="用户名" name="username">
            <UInput v-model="state.username" />
          </UFormGroup>
          <UButton type="button" @click="reload">
            查询
          </UButton>
        </div>
      </div>
    </template>
    <XManageDataState :pending="pagePending" :error="pageError" @retry="loadPage">
      <UTable
        :rows="userList" :columns="columns" class="overflow-auto w-full"
        :ui="{ wrapper: 'w-[300px]', th: { base: 'text-nowrap' } }"
      >
      <template #avatarUrl-data="{ row }">
        <NuxtLink :to="`/member/${row.username}`">
          <UAvatar :src="getAvatarUrl(row.avatarUrl!, row.headImg)" size="lg" alt="Avatar" />
        </NuxtLink>
      </template>
      <template #username-data="{ row }">
        <UButton :to="`/member/${row.username}`" color="white">
          {{ row.username }}
        </UButton>
      </template>
      <template #titles-data="{ row }">
        <div class="flex flex-col gap-1 w-fit">
          <UChip
            v-for="(t, index) in row.titles" :key="index" class="cursor-pointer" text="X"
            @click="removeTitle(row.id, t.id)"
          >
            <UBadge :color="t.style ?? 'primary'" size="xs">
              {{ t.title }}
            </UBadge>
          </UChip>
        </div>
      </template>
      <template #createdAt-data="{ row }">
        {{ dateFormat(row.createdAt) }}
      </template>
      <template #lastLogin-data="{ row }">
        {{ row.lastLogin && dateFormat(row.lastLogin) }}
      </template>
      <template #bannedEnd-data="{ row }">
        {{ row.bannedEnd && dateFormat(row.bannedEnd) }}
      </template>
      <template #actions-data="{ row }">
        <div class="space-x-2">
          <UDropdown
            v-if="row.status === 'NORMAL'" :items="items" :popper="{ placement: 'bottom-start' }"
            :ui="{ width: 'w-20' }"
          >
            <UButton
              color="white" label="禁言" trailing-icon="i-heroicons-chevron-down-20-solid"
              @click="selectedUid = row.uid"
            />
          </UDropdown>
          <UButton v-else color="white" label="撤销禁言" @click="revokeBanned(row)" />
          <UDropdown :items="titles" :popper="{ placement: 'bottom-start' }" :ui="{ width: 'w-20' }">
            <UButton
              color="white" label="分配头衔" trailing-icon="i-heroicons-chevron-down-20-solid"
              @click="selectedUid = row.uid"
            />
          </UDropdown>
          <UButton color="white" label="积分调整" @click="openPointAction(row.uid)" />
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
