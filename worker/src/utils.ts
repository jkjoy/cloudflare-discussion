import type { EmailConfig } from './types'

export const DAY_MS = 24 * 60 * 60 * 1000

export function json(data: any, headers?: Headers, status = 200) {
  const responseHeaders = headers || new Headers()
  responseHeaders.set('content-type', 'application/json; charset=utf-8')
  return new Response(JSON.stringify(data), {
    status,
    headers: responseHeaders,
  })
}

export function jsonError(message: string, status: number) {
  return json({ success: false, message }, undefined, status)
}

export async function readBody(request: Request) {
  const text = await request.text()
  if (!text) {
    return {}
  }
  try {
    return JSON.parse(text)
  }
  catch {
    return {}
  }
}

export function randomId(prefix: string) {
  return `${prefix}${crypto.randomUUID().replace(/-/g, '').slice(0, 22)}`
}

export function randomNumericCode(length: number) {
  let result = ''
  for (let i = 0; i < length; i++) {
    result += `${Math.floor(Math.random() * 10)}`
  }
  return result
}

export function nowIso() {
  return new Date().toISOString()
}

export function startOfDayIso() {
  const date = new Date()
  date.setHours(0, 0, 0, 0)
  return date.toISOString()
}

export function endOfDayIso() {
  const date = new Date()
  date.setHours(23, 59, 59, 999)
  return date.toISOString()
}

export function getPage(value: any) {
  const page = Number(value || 1)
  return Number.isFinite(page) && page > 0 ? Math.floor(page) : 1
}

export function getSize(value: any, fallback: number) {
  const size = Number(value || fallback)
  if (!Number.isFinite(size) || size <= 0) {
    return fallback
  }
  return Math.min(100, Math.floor(size))
}

export function parseJsonObject(value: string) {
  try {
    return JSON.parse(value)
  }
  catch {
    return {}
  }
}

export function parseJsonArray(value: string) {
  try {
    const result = JSON.parse(value || '[]')
    return Array.isArray(result) ? result : []
  }
  catch {
    return []
  }
}

export function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value))
}

export function deepMerge(target: any, source: any): any {
  if (!source || typeof source !== 'object') {
    return target
  }

  for (const [key, value] of Object.entries(source)) {
    if (value && typeof value === 'object' && !Array.isArray(value) && target[key] && typeof target[key] === 'object' && !Array.isArray(target[key])) {
      target[key] = deepMerge({ ...target[key] }, value)
    }
    else {
      target[key] = value
    }
  }

  return target
}

export function escapeHtml(value: string) {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll('\'', '&#39;')
}

export function normalizeNullableString(value: any) {
  const text = String(value ?? '').trim()
  return text ? text : null
}

export function base64urlEncode(value: string) {
  return base64urlFromBytes(new TextEncoder().encode(value))
}

export function base64urlDecode(value: string) {
  const bytes = base64urlToBytes(value)
  return new TextDecoder().decode(bytes)
}

export function base64urlFromBytes(bytes: Uint8Array) {
  let binary = ''
  for (const byte of bytes) {
    binary += String.fromCharCode(byte)
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '')
}

export function base64urlToBytes(value: string) {
  const padded = value.replace(/-/g, '+').replace(/_/g, '/').padEnd(Math.ceil(value.length / 4) * 4, '=')
  const binary = atob(padded)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i)
  }
  return bytes
}

export function timingSafeEqual(left: Uint8Array, right: Uint8Array) {
  if (left.length !== right.length) {
    return false
  }
  let result = 0
  for (let i = 0; i < left.length; i++) {
    result |= left[i] ^ right[i]
  }
  return result === 0
}

export async function sha256Hex(value: string) {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value.trim().toLowerCase()))
  return Array.from(new Uint8Array(digest)).map(byte => byte.toString(16).padStart(2, '0')).join('')
}

export function normalizeEmail(value: string) {
  return value.trim().toLowerCase()
}

export function isSameEmail(left: string, right: string) {
  return normalizeEmail(left) === normalizeEmail(right)
}

export function isValidEmail(value: string) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)
}

export function normalizeEmailConfig(value: any): EmailConfig {
  const email = value && typeof value === 'object' ? value : {}
  const password = String(email.password || '').trim()

  return {
    apiKey: String(email.apiKey || (password.startsWith('re_') ? password : '')).trim(),
    from: normalizeEmail(String(email.from || email.username || '')),
    senderName: String(email.senderName || '').trim(),
    to: normalizeEmail(String(email.to || '')),
  }
}

export function validateEmailConfig(config: EmailConfig) {
  if (!config.apiKey) {
    return '请先配置 Resend API Key'
  }
  if (!isValidEmail(config.from)) {
    return '请先配置发件邮箱'
  }
  return ''
}

export function getUserLevelByPoint(point: number) {
  if (point < 200)
    return 1
  if (point < 400)
    return 2
  if (point < 900)
    return 3
  if (point < 1600)
    return 4
  if (point < 2500)
    return 5
  return 6
}

export function extractMentions(text: string) {
  const regex = /\[@([^\]]+)\]/g
  return (text.match(regex) || []).map(match => match.slice(1, -1))
}

export function truncateText(value: string, maxLength: number) {
  if (value.length <= maxLength) {
    return value
  }
  return `${value.slice(0, maxLength)}...`
}

export function formatDatePid(pattern: string) {
  const now = new Date()
  const map: Record<string, string> = {
    YYYY: `${now.getFullYear()}`,
    MM: `${now.getMonth() + 1}`.padStart(2, '0'),
    DD: `${now.getDate()}`.padStart(2, '0'),
    HH: `${now.getHours()}`.padStart(2, '0'),
    mm: `${now.getMinutes()}`.padStart(2, '0'),
    ss: `${now.getSeconds()}`.padStart(2, '0'),
    SSS: `${now.getMilliseconds()}`.padStart(3, '0'),
  }

  return pattern.replace(/YYYY|MM|DD|HH|mm|ss|SSS/g, token => map[token] || token)
}

export function getRandomIntWeighted(min: number, max: number) {
  min = Math.ceil(min)
  max = Math.floor(max)
  if (!Number.isFinite(min) || !Number.isFinite(max)) {
    return 0
  }
  if (max <= min) {
    return min
  }
  const mid = Math.floor((min + max) / 2)
  const random = Math.random()
  if (random < 0.8) {
    return Math.floor(Math.random() * (mid - min + 1)) + min
  }
  return Math.floor(Math.random() * (max - mid)) + (mid + 1)
}

export function formatEmailAddress(email: string, senderName: string) {
  const safeName = senderName.replace(/"/g, '').trim()
  return safeName ? `${safeName} <${email}>` : email
}

export function normalizeSiteUrl(config: any) {
  return String(config?.websiteUrl || '').trim().replace(/\/+$/, '')
}

export function buildSiteLink(config: any, path: string) {
  const siteUrl = normalizeSiteUrl(config)
  if (!siteUrl) {
    return ''
  }
  return `${siteUrl}${path.startsWith('/') ? path : `/${path}`}`
}
