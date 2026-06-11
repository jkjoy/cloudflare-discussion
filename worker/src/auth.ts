import type { CurrentUser, Env, TokenPayload } from './types'
import { first, run } from './db'
import { base64urlDecode, base64urlEncode, base64urlFromBytes, base64urlToBytes, nowIso, timingSafeEqual } from './utils'

export function getTokenKey(env: Env) {
  return env.TOKEN_KEY || 'discussion_token'
}

export function shouldUseSecureCookie(env: Env) {
  return env.COOKIE_SECURE === 'true'
}

export function buildCookie(name: string, value: string, maxAgeMs: number, env: Env) {
  const parts = [
    `${name}=${encodeURIComponent(value)}`,
    'Path=/',
    'SameSite=Lax',
    `Max-Age=${Math.floor(maxAgeMs / 1000)}`,
  ]
  if (shouldUseSecureCookie(env)) {
    parts.push('Secure')
  }
  return parts.join('; ')
}

export function expireCookie(name: string, env: Env) {
  const parts = [
    `${name}=`,
    'Path=/',
    'SameSite=Lax',
    'Expires=Thu, 01 Jan 1970 00:00:00 GMT',
  ]
  if (shouldUseSecureCookie(env)) {
    parts.push('Secure')
  }
  return parts.join('; ')
}

export function getCookie(request: Request, name: string) {
  const cookie = request.headers.get('cookie') || ''
  const items = cookie.split(';').map(item => item.trim())
  for (const item of items) {
    const index = item.indexOf('=')
    if (index === -1)
      continue
    const key = item.slice(0, index)
    if (key === name) {
      return decodeURIComponent(item.slice(index + 1))
    }
  }
  return ''
}

export async function signHmac(value: string, secret: string) {
  const enc = new TextEncoder()
  const key = await crypto.subtle.importKey(
    'raw',
    enc.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  )
  const signature = await crypto.subtle.sign('HMAC', key, enc.encode(value))
  return base64urlFromBytes(new Uint8Array(signature))
}

export async function createToken(payload: TokenPayload, env: Env) {
  const secret = env.JWT_SECRET_KEY || 'replace-this-secret'
  const header = { alg: 'HS256', typ: 'JWT' }
  const encodedHeader = base64urlEncode(JSON.stringify(header))
  const encodedPayload = base64urlEncode(JSON.stringify(payload))
  const signingInput = `${encodedHeader}.${encodedPayload}`
  const signature = await signHmac(signingInput, secret)
  return `${signingInput}.${signature}`
}

export async function verifyToken(token: string, env: Env) {
  const secret = env.JWT_SECRET_KEY || 'replace-this-secret'
  const [encodedHeader, encodedPayload, signature] = token.split('.')
  if (!encodedHeader || !encodedPayload || !signature) {
    return null
  }
  const signingInput = `${encodedHeader}.${encodedPayload}`
  const expectedSignature = await signHmac(signingInput, secret)
  if (expectedSignature !== signature) {
    return null
  }
  const payload = JSON.parse(base64urlDecode(encodedPayload)) as TokenPayload
  if (!payload.exp || payload.exp < Math.floor(Date.now() / 1000)) {
    return null
  }
  return payload
}

async function derivePasswordBits(password: string, salt: Uint8Array, iterations: number) {
  const enc = new TextEncoder()
  const key = await crypto.subtle.importKey('raw', enc.encode(password), 'PBKDF2', false, ['deriveBits'])
  const bits = await crypto.subtle.deriveBits({
    name: 'PBKDF2',
    hash: 'SHA-256',
    salt,
    iterations,
  }, key, 256)
  return new Uint8Array(bits)
}

export async function hashPassword(password: string) {
  const salt = crypto.getRandomValues(new Uint8Array(16))
  const derived = await derivePasswordBits(password, salt, 100_000)
  return `pbkdf2$100000$${base64urlFromBytes(salt)}$${base64urlFromBytes(derived)}`
}

export async function verifyPassword(password: string, stored: string) {
  const [algorithm, iterationsText, saltEncoded, hashEncoded] = stored.split('$')
  if (algorithm !== 'pbkdf2') {
    return false
  }
  const iterations = Number(iterationsText || 100000)
  const salt = base64urlToBytes(saltEncoded)
  const expected = base64urlToBytes(hashEncoded)
  const derived = await derivePasswordBits(password, salt, iterations)
  return timingSafeEqual(derived, expected)
}

export function isAdmin(user: CurrentUser | null) {
  return user?.role === 'ADMIN'
}

export function mapCurrentUser(row: any): CurrentUser {
  return {
    id: Number(row.id),
    uid: row.uid,
    createdAt: row.created_at ?? null,
    updatedAt: row.updated_at ?? null,
    username: row.username,
    role: row.role,
    status: row.status,
    point: Number(row.point ?? 0),
    level: Number(row.level ?? 1),
    email: row.email,
    avatarUrl: row.avatar_url,
    headImg: row.head_img,
    postCount: Number(row.post_count ?? 0),
    commentCount: Number(row.comment_count ?? 0),
    lastActive: row.last_active,
    lastLogin: row.last_login,
    bannedEnd: row.banned_end,
    css: row.css,
    js: row.js,
    signature: row.signature,
    tgChatID: row.tg_chat_id,
    secretKey: row.secret_key,
  }
}

export function sanitizeUser(user: CurrentUser, includePrivateFields = false) {
  const payload: Record<string, any> = {
    id: user.id,
    uid: user.uid,
    createdAt: user.createdAt,
    updatedAt: user.updatedAt,
    username: user.username,
    email: user.email,
    avatarUrl: user.avatarUrl,
    headImg: user.headImg,
    point: user.point,
    postCount: user.postCount,
    commentCount: user.commentCount,
    role: user.role,
    status: user.status,
    lastLogin: user.lastLogin,
    level: user.level,
    bannedEnd: user.bannedEnd,
    css: user.css,
    js: user.js,
    signature: user.signature,
    lastActive: user.lastActive,
  }

  if (includePrivateFields) {
    payload.secretKey = user.secretKey
    payload.tgChatID = user.tgChatID
  }

  return payload
}

export async function ensureUserSecretKey(env: Env, row: any) {
  const secretKey = String(row?.secret_key || '').trim()
  if (secretKey) {
    return secretKey
  }

  const nextSecretKey = randomId('')
  const updatedAt = nowIso()
  await run(env, 'UPDATE users SET secret_key = ?, updated_at = ? WHERE uid = ?', [nextSecretKey, updatedAt, row.uid])
  row.secret_key = nextSecretKey
  row.updated_at = updatedAt
  return nextSecretKey
}

export async function getCurrentUser(request: Request, env: Env) {
  const token = getCookie(request, getTokenKey(env))
  if (!token) {
    return null
  }

  try {
    const payload = await verifyToken(token, env)
    if (!payload) {
      return null
    }
    const row = await first(env, 'SELECT * FROM users WHERE uid = ?', [payload.uid])
    if (!row) {
      return null
    }
    await ensureUserSecretKey(env, row)
    if (row.status === 'BANNED' && row.banned_end && String(row.banned_end) < nowIso()) {
      await run(env, 'UPDATE users SET status = ?, banned_end = NULL, updated_at = ? WHERE uid = ?', ['NORMAL', nowIso(), row.uid])
      row.status = 'NORMAL'
      row.banned_end = null
    }
    return mapCurrentUser(row)
  }
  catch {
    return null
  }
}

function randomId(prefix: string) {
  return `${prefix}${crypto.randomUUID().replace(/-/g, '').slice(0, 22)}`
}
