import type { CurrentUser, Env } from './types'
import { json, nowIso, randomId } from './utils'

export function buildImageObjectKey(filename: string, mimeType: string) {
  const now = new Date()
  const year = `${now.getFullYear()}`
  const month = `${now.getMonth() + 1}`.padStart(2, '0')
  const ext = resolveImageExtension(filename, mimeType)
  return `uploads/${year}/${month}/${randomId('img_')}.${ext}`
}

function resolveImageExtension(filename: string, mimeType: string) {
  const fallback = getImageExtensionByMimeType(mimeType)
  const match = filename.toLowerCase().match(/\.([a-z0-9]{1,10})$/)
  if (!match) {
    return fallback
  }
  const ext = match[1]
  return ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'svg', 'avif'].includes(ext) ? ext : fallback
}

function getImageExtensionByMimeType(mimeType: string) {
  const map: Record<string, string> = {
    'image/jpeg': 'jpg',
    'image/png': 'png',
    'image/gif': 'gif',
    'image/webp': 'webp',
    'image/bmp': 'bmp',
    'image/svg+xml': 'svg',
    'image/avif': 'avif',
  }
  return map[mimeType] || 'bin'
}

function getImageContentType(key: string) {
  const ext = key.split('.').pop()?.toLowerCase()
  const map: Record<string, string> = {
    jpg: 'image/jpeg',
    jpeg: 'image/jpeg',
    png: 'image/png',
    gif: 'image/gif',
    webp: 'image/webp',
    bmp: 'image/bmp',
    svg: 'image/svg+xml',
    avif: 'image/avif',
  }
  return map[ext || ''] || 'application/octet-stream'
}

export async function handleImageAsset(request: Request, env: Env, url: URL) {
  if (!env.IMAGES_BUCKET) {
    return new Response('R2 bucket is not configured', { status: 503 })
  }

  const method = request.method.toUpperCase()
  if (method !== 'GET' && method !== 'HEAD') {
    return new Response('Method Not Allowed', { status: 405 })
  }

  const key = decodeURIComponent(url.pathname.slice('/imgs/'.length))
  if (!key) {
    return new Response('Not Found', { status: 404 })
  }

  const object = await env.IMAGES_BUCKET.get(key)
  if (!object) {
    return new Response('Not Found', { status: 404 })
  }

  const headers = new Headers()
  if (object.httpMetadata?.contentType) {
    headers.set('Content-Type', object.httpMetadata.contentType)
  }
  if (object.httpMetadata?.contentDisposition) {
    headers.set('Content-Disposition', object.httpMetadata.contentDisposition)
  }
  if (object.httpMetadata?.contentEncoding) {
    headers.set('Content-Encoding', object.httpMetadata.contentEncoding)
  }
  if (object.httpMetadata?.contentLanguage) {
    headers.set('Content-Language', object.httpMetadata.contentLanguage)
  }
  headers.set('Cache-Control', object.httpMetadata?.cacheControl || 'public, max-age=31536000, immutable')
  if (object.etag) {
    headers.set('ETag', object.etag)
  }

  return new Response(method === 'HEAD' ? null : (object.body ?? null), {
    status: 200,
    headers,
  })
}

export async function handleImageUpload(request: Request, env: Env, currentUser: CurrentUser | null) {
  if (!currentUser) {
    return json({ success: false, filename: '', message: '请先去登录' })
  }
  if (!env.IMAGES_BUCKET) {
    return json({ success: false, filename: '', message: '未绑定 IMAGES_BUCKET R2 bucket' })
  }

  const formData = await request.formData()
  const file = formData.get('file')
  if (!(file instanceof File)) {
    return json({ success: false, filename: '', message: '请选择图片文件' })
  }
  if (!file.type.startsWith('image/')) {
    return json({ success: false, filename: '', message: '只支持上传图片文件' })
  }
  if (file.size <= 0) {
    return json({ success: false, filename: '', message: '图片内容不能为空' })
  }
  if (file.size > 10 * 1024 * 1024) {
    return json({ success: false, filename: '', message: '图片大小不能超过10MB' })
  }

  const key = buildImageObjectKey(file.name, file.type)
  const contentType = file.type || getImageContentType(key)

  await env.IMAGES_BUCKET.put(key, await file.arrayBuffer(), {
    httpMetadata: {
      contentType,
      cacheControl: 'public, max-age=31536000, immutable',
    },
    customMetadata: {
      uploader: currentUser.uid,
    },
  })

  return json({
    success: true,
    filename: new URL(`/imgs/${key}`, request.url).toString(),
    message: '上传成功',
  })
}
