import type { SysConfigDTO } from '~/types'

export function useAbsoluteUrl() {
  const getAbsoluteUrl = (path: string) => {
    if (import.meta.client) {
      return new URL(path, window.location.origin).toString()
    }

    const url = useRequestURL()
    const baseUrl = `${url.protocol}//${url.host}`
    return baseUrl + path
  }
  return { getAbsoluteUrl }
}

export function useGlobalConfig() {
  return useState('globalConfig', () => {
    return { sysConfig: {} as SysConfigDTO, version: '' }
  })
}
