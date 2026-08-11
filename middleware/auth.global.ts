import type { UserDTO } from '~/types'

export default defineNuxtRouteMiddleware(async (to) => {
  const config = useRuntimeConfig()
  const token = useCookie(config.public.tokenKey)
  const needLoginUrls = ['/post/new', '/member/settings', '/manage']
  const sliderOpen = useState('sliderOpen')
  if (sliderOpen.value) {
    sliderOpen.value = false
  }

  if (
    !token.value
    && needLoginUrls.some(path => to.path === path || to.path.startsWith(`${path}/`))
  ) {
    return navigateTo({ path: '/member/login', query: { redirect: to.fullPath } })
  }

  if (!to.path.startsWith('/manage')) {
    return
  }

  const userinfo = useState<UserDTO>('userinfo', () => ({} as UserDTO))
  const validatedAdminToken = useState<string>('validatedAdminToken', () => '')
  if (validatedAdminToken.value === token.value && userinfo.value?.role === 'ADMIN') {
    return
  }

  try {
    const profile = await $fetch<UserDTO>('/api/member/profile', { method: 'POST' })
    if (!profile?.username) {
      token.value = null
      validatedAdminToken.value = ''
      return navigateTo({ path: '/member/login', query: { redirect: to.fullPath } })
    }
    if (profile.role !== 'ADMIN') {
      return navigateTo('/')
    }
    userinfo.value = profile
    validatedAdminToken.value = token.value || ''
  }
  catch {
    validatedAdminToken.value = ''
    return navigateTo({ path: '/member/login', query: { redirect: to.fullPath } })
  }
})
