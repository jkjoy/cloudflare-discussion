export interface D1PreparedLike {
  bind(...args: any[]): D1PreparedLike
  all<T = Record<string, any>>(): Promise<{ results?: T[] }>
  first<T = Record<string, any>>(): Promise<T | null>
  run(): Promise<any>
}

export interface D1DatabaseLike {
  prepare(query: string): D1PreparedLike
}

export interface FetcherLike {
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>
}

export interface R2HttpMetadataLike {
  contentType?: string
  cacheControl?: string
  contentDisposition?: string
  contentEncoding?: string
  contentLanguage?: string
}

export interface R2ObjectBodyLike {
  body?: ReadableStream | null
  etag?: string
  httpMetadata?: R2HttpMetadataLike
}

export interface R2BucketLike {
  get(key: string): Promise<R2ObjectBodyLike | null>
  put(
    key: string,
    value: ArrayBuffer | ArrayBufferView | Blob | ReadableStream | string,
    options?: {
      httpMetadata?: R2HttpMetadataLike
      customMetadata?: Record<string, string>
    },
  ): Promise<unknown>
}

export interface Env {
  DB: D1DatabaseLike
  ASSETS?: FetcherLike
  IMAGES_BUCKET?: R2BucketLike
  JWT_SECRET_KEY?: string
  TOKEN_KEY?: string
  AVATAR_CDN?: string
  COOKIE_SECURE?: string
}

export interface CurrentUser {
  id: number
  uid: string
  createdAt: string | null
  updatedAt: string | null
  username: string
  role: 'ADMIN' | 'USER'
  status: 'NORMAL' | 'BANNED'
  point: number
  level: number
  email: string
  avatarUrl: string
  headImg: string | null
  postCount: number
  commentCount: number
  lastActive: string | null
  lastLogin: string | null
  bannedEnd: string | null
  css: string | null
  js: string | null
  signature: string | null
  tgChatID: string | null
  secretKey: string | null
}

export interface TokenPayload {
  uid: string
  userId: number
  username: string
  exp: number
}

export interface EmailConfig {
  apiKey: string
  from: string
  senderName: string
  to: string
}

export interface UserTitleSummary {
  id: number
  title: string
  style: string
  status: boolean
}

export interface ExecutionContextLike {
  waitUntil(promise: Promise<unknown>): void
}
