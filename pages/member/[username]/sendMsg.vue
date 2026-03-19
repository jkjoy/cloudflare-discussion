<script lang="ts" setup>
import type { UserDTO } from '~/types'

const route = useRoute()
const currentUser = useState<UserDTO>('userinfo')
const username = route.params.username as string
async function sendMsgSuccess() {
  const owner = currentUser.value?.username || username
  await navigateTo(`/member/${owner}/from/${username}`)
}
</script>

<template>
  <XSendMsg :to-username="username" @send-msg-success="sendMsgSuccess" />
</template>

<style scoped>

</style>
