import type { Env } from './types'
import { clone, deepMerge, normalizeEmailConfig } from './utils'
import { first, run } from './db'

export const defaultSysConfig = {
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
  websiteAnnouncement: '',
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
    senderName: '',
    to: '',
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
}

export function normalizeSysConfig(config: any) {
  const normalized = config && typeof config === 'object' ? clone(config) : {}

  delete normalized.enableUploadLocalImage
  delete normalized.s3
  delete normalized.r2
  delete normalized.ForwardUrl
  delete normalized.proxyUrl
  if (normalized.googleRecaptcha && (!normalized.turnstile || typeof normalized.turnstile !== 'object')) {
    normalized.turnstile = normalized.googleRecaptcha
  }
  delete normalized.googleRecaptcha

  if (normalized.notify && typeof normalized.notify === 'object') {
    delete normalized.notify.tgProxyUrl
  }

  normalized.email = normalizeEmailConfig(normalized.email)

  normalized.upload = {
    ...(normalized.upload && typeof normalized.upload === 'object' ? normalized.upload : {}),
    imgStrategy: 'r2',
    attachmentStrategy: 'r2',
  }

  return normalized
}

export async function getSysConfig(env: Env) {
  const row = await first(env, 'SELECT content FROM sys_config WHERE id = 1', [])
  if (!row?.content) {
    await saveSysConfig(env, defaultSysConfig)
    return clone(defaultSysConfig)
  }
  return deepMerge(clone(defaultSysConfig), normalizeSysConfig(JSON.parse(row.content)))
}

export function getPublicSysConfig(config: any) {
  const publicConfig = clone(config)

  if (publicConfig.turnstile && typeof publicConfig.turnstile === 'object') {
    publicConfig.turnstile = {
      ...publicConfig.turnstile,
      secretKey: '',
    }
  }

  publicConfig.email = null

  if (publicConfig.notify && typeof publicConfig.notify === 'object') {
    publicConfig.notify = {
      ...publicConfig.notify,
      tgBotToken: '',
      tgSecret: '',
    }
  }

  return publicConfig
}

export async function saveSysConfig(env: Env, config: any) {
  const merged = deepMerge(clone(defaultSysConfig), normalizeSysConfig(config))
  const exists = await first(env, 'SELECT id FROM sys_config WHERE id = 1', [])
  if (exists) {
    await run(env, 'UPDATE sys_config SET content = ? WHERE id = 1', [JSON.stringify(merged)])
  }
  else {
    await run(env, 'INSERT INTO sys_config (id, content) VALUES (1, ?)', [JSON.stringify(merged)])
  }
}
