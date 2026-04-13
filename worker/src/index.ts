import { defaultSysConfig } from './config.ts'

interface D1PreparedLike {
  bind(...args: any[]): D1PreparedLike
  all<T = Record<string, any>>(): Promise<{ results?: T[] }>
  first<T = Record<string, any>>(): Promise<T | null>
  run(): Promise<any>
}

interface D1DatabaseLike {
  prepare(query: string): D1PreparedLike
}

interface FetcherLike {
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>
}

interface R2HttpMetadataLike {
  contentType?: string
  cacheControl?: string
  contentDisposition?: string
  contentEncoding?: string
  contentLanguage?: string
}

interface R2ObjectBodyLike {
  body?: ReadableStream | null
  etag?: string
  httpMetadata?: R2HttpMetadataLike
}

interface R2BucketLike {
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

interface Env {
  DB: D1DatabaseLike
  ASSETS?: FetcherLike
  IMAGES_BUCKET?: R2BucketLike
  JWT_SECRET_KEY?: string
  TOKEN_KEY?: string
  AVATAR_CDN?: string
  COOKIE_SECURE?: string
  APP_VERSION?: string
}

interface CurrentUser {
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

interface TokenPayload {
  uid: string
  userId: number
  username: string
  exp: number
}

interface EmailConfig {
  apiKey: string
  from: string
  senderName: string
  to: string
}

interface UserTitleSummary {
  id: number
  title: string
  style: string
  status: boolean
}

interface ExecutionContextLike {
  waitUntil(promise: Promise<unknown>): void
}

const DAY_MS = 24 * 60 * 60 * 1000
const PUBLIC_API_PATHS = new Set([
  '/api/config',
  '/api/version',
  '/api/go/list',
  '/api/member/hot',
  '/api/member/login',
  '/api/member/reg',
  '/api/member/sendEmail',
  '/api/member/sendForgotPasswordEmail',
  '/api/member/resetPwd',
  '/api/tg',
])
const PUBLIC_GET_API_PATHS = new Set([
  '/api/config',
  '/api/version',
  '/api/go/list',
  '/api/member/hot',
  '/api/post/list',
])

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContextLike): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname.startsWith('/imgs/')) {
      return handleImageAsset(request, env, url)
    }
    if (url.pathname.startsWith('/api/')) {
      return handleApi(request, env, url, ctx)
    }

    if (env.ASSETS) {
      return env.ASSETS.fetch(request)
    }

    return new Response('Not Found', { status: 404 })
  },
}

async function handleApi(request: Request, env: Env, url: URL, ctx: ExecutionContextLike) {
  const pathname = url.pathname
  const method = request.method.toUpperCase()
  const currentUser = shouldResolveCurrentUser(pathname, method) ? await getCurrentUser(request, env) : null

  if (pathname === '/api/config') {
    if (method === 'GET') {
      return buildConfigResponse(env)
    }
    if (method === 'POST') {
      return buildConfigResponse(env)
    }
  }

  if (pathname === '/api/version') {
    if (method === 'GET') {
      return respondWithEdgeCache(request, ctx, 300, async () => buildVersionResponse(env))
    }
    if (method === 'POST') {
      return buildVersionResponse(env)
    }
  }

  if (pathname === '/api/go/list') {
    if (method === 'GET') {
      return respondWithEdgeCache(request, ctx, 300, async () => buildTagListResponse(env, url))
    }
    if (method === 'POST') {
      return buildTagListResponse(env, url)
    }
  }

  if (pathname === '/api/member/hot') {
    if (method === 'GET') {
      return respondWithEdgeCache(request, ctx, 120, async () => buildMemberHotResponse(env))
    }
    if (method === 'POST') {
      return buildMemberHotResponse(env)
    }
  }

  if (pathname === '/api/member/login' && method === 'POST') {
    const body = await readBody(request)
    const username = String(body.username || '').trim()
    const password = String(body.password || '')
    if (username.length < 3 || password.length < 6) {
      return json({ success: false, message: '用户名/密码不正确' })
    }

    const config = await getSysConfig(env)
    if (config.turnstile?.enable) {
      const turnstile = await verifyTurnstile(config.turnstile.secretKey, body.token, 'login', request)
      if (!turnstile.success) {
        return json(turnstile)
      }
    }

    const user = await first(env, 'SELECT * FROM users WHERE username = ?', [username])
    if (!user || !(await verifyPassword(password, user.password_hash))) {
      return json({ success: false, message: '用户名/密码不正确' })
    }

    const now = nowIso()
    await run(env, 'UPDATE users SET last_login = ?, updated_at = ? WHERE id = ?', [now, now, user.id])

    const token = await createToken({
      uid: user.uid,
      userId: user.id,
      username: user.username,
      exp: Math.floor(Date.now() / 1000) + 10 * 24 * 60 * 60,
    }, env)

    const headers = new Headers()
    headers.append('Set-Cookie', buildCookie(getTokenKey(env), token, 10 * DAY_MS, env))

    return json({
      success: true,
      token,
      tokenKey: getTokenKey(env),
    }, headers)
  }

  if (pathname === '/api/member/reg' && method === 'POST') {
    const body = await readBody(request)
    const username = String(body.username || '').trim()
    const password = String(body.password || '')
    const repeatPassword = String(body.repeatPassword || '')
    const email = normalizeEmail(String(body.email || ''))

    if (username.length < 3) {
      return json({ success: false, message: '用户名最少3个字符,中文一个算2个字符' })
    }
    if (password.length < 6) {
      return json({ success: false, message: '密码最少6个字符' })
    }
    if (password !== repeatPassword) {
      return json({ success: false, message: '两次密码不一致' })
    }
    if (!email.includes('@')) {
      return json({ success: false, message: '请填写正确的邮箱地址' })
    }

    const config = await getSysConfig(env)
    if (config.turnstile?.enable) {
      const turnstile = await verifyTurnstile(config.turnstile.secretKey, body.token, 'reg', request)
      if (!turnstile.success) {
        return json(turnstile)
      }
    }

    const existing = await queryCount(env, 'SELECT COUNT(*) AS count FROM users WHERE username = ? OR email = ?', [username, email])
    if (existing > 0) {
      return json({ success: false, message: '用户名/邮箱已经存在了' })
    }

    let inviteRow: any = null
    let inviteUserId: number | null = null
    if (config.invite) {
      const inviteCode = String(body.inviteCode || '').trim()
      if (!inviteCode) {
        return json({ success: false, message: '当前已开启邀请码注册' })
      }

      inviteRow = await first(env, 'SELECT * FROM invite_codes WHERE content = ? AND to_uid IS NULL AND end_at >= ?', [inviteCode, nowIso()])
      if (!inviteRow) {
        return json({ success: false, message: '邀请码已失效' })
      }

      const inviteUser = await first(env, 'SELECT id FROM users WHERE uid = ?', [inviteRow.from_uid])
      if (!inviteUser) {
        return json({ success: false, message: '邀请人不存在' })
      }
      inviteUserId = inviteUser.id
    }

    if (config.regWithEmailCodeVerify) {
      const emailCodeKey = String(body.emailCodeKey || '')
      const emailCode = String(body.emailCode || '')
      if (!emailCodeKey || !emailCode) {
        return json({ success: false, message: '请输入邮箱验证码' })
      }

      const record = await first(env, 'SELECT * FROM email_codes WHERE key = ?', [emailCodeKey])
      if (!record
        || String(record.reason) !== 'REGISTER'
        || !isSameEmail(String(record.target_email || ''), email)
        || String(record.code).toUpperCase() !== emailCode.toUpperCase()) {
        return json({ success: false, message: '邮箱验证码错误' })
      }
      if (Number(record.used) === 1) {
        return json({ success: false, message: '邮箱验证码已使用了' })
      }
      if (String(record.valid_at) < nowIso()) {
        return json({ success: false, message: '邮箱验证码已过期' })
      }
      await run(env, 'UPDATE email_codes SET used = 1, updated_at = ? WHERE id = ?', [nowIso(), record.id])
    }

    const userCount = await queryCount(env, 'SELECT COUNT(*) AS count FROM users', [])
    const point = 100
    const level = getUserLevelByPoint(point)
    const uid = randomId('u')
    const passwordHash = await hashPassword(password)
    const avatarUrl = await sha256Hex(email)
    const secretKey = randomId('')
    const role = userCount === 0 ? 'ADMIN' : 'USER'
    const now = nowIso()

    await run(env, `
      INSERT INTO users (
        uid, created_at, updated_at, username, password_hash, email, avatar_url,
        point, post_count, comment_count, role, level, status, invited_by_id, secret_key
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, 'NORMAL', ?, ?)
    `, [uid, now, now, username, passwordHash, email, avatarUrl, point, role, level, inviteUserId, secretKey])

    if (inviteRow) {
      await run(env, 'UPDATE invite_codes SET to_uid = ? WHERE id = ?', [uid, inviteRow.id])
    }

    return json({ success: true })
  }

  if (pathname === '/api/member/profile' && method === 'POST') {
    if (!currentUser) {
      return json({})
    }

    return json(await buildProfile(env, currentUser))
  }

  if (pathname === '/api/member/signIn' && method === 'POST') {
    if (!currentUser) {
      return json({ success: false, message: '请先去登录' })
    }

    const todayStart = startOfDayIso()
    const count = await queryCount(env, `
      SELECT COUNT(*) AS count
      FROM point_history
      WHERE uid = ?
        AND reason = 'SIGNIN'
        AND created_at >= ?
    `, [currentUser.uid, todayStart])

    if (count > 0) {
      return json({ success: false, message: '今天已经签到过了,请不要反复签到' })
    }

    const config = await getSysConfig(env)
    const point = getRandomIntWeighted(config.pointPerDaySignInMin, config.pointPerDaySignInMax)
    const now = nowIso()
    await run(env, 'INSERT INTO point_history (created_at, updated_at, reason, uid, point) VALUES (?, ?, ?, ?, ?)', [now, now, 'SIGNIN', currentUser.uid, point])
    const newPoint = currentUser.point + point
    await run(env, 'UPDATE users SET point = ?, level = ?, updated_at = ?, last_active = ? WHERE uid = ?', [newPoint, getUserLevelByPoint(newPoint), now, now, currentUser.uid])

    return json({ success: true, message: `签到成功,获得${point}积分` })
  }

  if (pathname === '/api/member/saveSettings' && method === 'POST') {
    if (!currentUser) {
      return json({ success: false, message: '请先去登录' })
    }

    const body = await readBody(request)
    const email = normalizeEmail(String(body.email || ''))
    if (!email.includes('@')) {
      return json({ success: false, message: '请填写正确邮箱地址' })
    }

    const now = nowIso()
    const avatarUrl = await sha256Hex(email)
    const headImg = normalizeNullableString(body.headImg)
    const css = normalizeNullableString(body.css)
    const js = normalizeNullableString(body.js)
    const signature = normalizeNullableString(body.signature)

    if (body.password) {
      const passwordHash = await hashPassword(String(body.password))
      await run(env, `
        UPDATE users
        SET email = ?, avatar_url = ?, head_img = ?, css = ?, js = ?, signature = ?, password_hash = ?, updated_at = ?
        WHERE uid = ?
      `, [email, avatarUrl, headImg, css, js, signature, passwordHash, now, currentUser.uid])

      const headers = new Headers()
      headers.append('Set-Cookie', expireCookie(getTokenKey(env), env))
      return json({ success: true }, headers)
    }

    await run(env, `
      UPDATE users
      SET email = ?, avatar_url = ?, head_img = ?, css = ?, js = ?, signature = ?, updated_at = ?
      WHERE uid = ?
    `, [email, avatarUrl, headImg, css, js, signature, now, currentUser.uid])

    return json({ success: true })
  }

  if (pathname === '/api/member/createInviteCode' && method === 'POST') {
    if (!currentUser) {
      return json({ success: false, message: '请先去登录' })
    }

    const config = await getSysConfig(env)
    const cost = config.createInviteCodePoint
    const deduct = currentUser.role === 'ADMIN' ? 1 : cost
    if (currentUser.point < deduct) {
      return json({ success: false, message: '您的积分不足，无法生成邀请码' })
    }

    const now = nowIso()
    const inviteCode = randomId('i')
    const nextPoint = currentUser.point - deduct

    await run(env, 'INSERT INTO point_history (created_at, updated_at, reason, uid, point) VALUES (?, ?, ?, ?, ?)', [now, now, 'INVITE', currentUser.uid, -deduct])
    await run(env, 'UPDATE users SET point = ?, level = ?, updated_at = ? WHERE uid = ?', [nextPoint, getUserLevelByPoint(nextPoint), now, currentUser.uid])
    await run(env, 'INSERT INTO invite_codes (created_at, end_at, from_uid, to_uid, content) VALUES (?, ?, ?, NULL, ?)', [now, new Date(Date.now() + DAY_MS).toISOString(), currentUser.uid, inviteCode])

    return json({ success: true, data: inviteCode, message: '邀请码生成成功！' })
  }

  if (pathname === '/api/member/inviteCodeList' && method === 'POST') {
    if (!currentUser) {
      return json({ success: false, message: '请先去登录' })
    }

    const rows = await all(env, `
      SELECT ic.*, u.username AS to_username, u.uid AS to_uid_value
      FROM invite_codes ic
      LEFT JOIN users u ON u.uid = ic.to_uid
      WHERE ic.from_uid = ?
      ORDER BY ic.created_at DESC
    `, [currentUser.uid])

    return json({
      success: true,
      list: rows.map(row => ({
        id: row.id,
        createdAt: row.created_at,
        endAt: row.end_at,
        fromUid: row.from_uid,
        toUid: row.to_uid,
        content: row.content,
        toUser: row.to_uid ? { uid: row.to_uid_value, username: row.to_username } : null,
      })),
      total: rows.length,
    })
  }

  if (pathname === '/api/post/list') {
    if (method === 'GET') {
      return respondWithEdgeCache(request, ctx, 60, async () => buildPostListResponse(env, null, getPostListInputFromUrl(url)))
    }
    if (method === 'POST') {
      return buildPostListResponse(env, currentUser, await readBody(request))
    }
  }

  if (pathname === '/api/post/new' && method === 'POST') {
    if (!currentUser) {
      return json({ success: false, message: '请先去登录' })
    }
    if (currentUser.status === 'BANNED') {
      return json({ success: false, message: '用户不存在或已被封禁' })
    }
    if (currentUser.point <= 0) {
      return json({ success: false, message: '用户积分小于或等于0分,不能发帖' })
    }

    const body = await readBody(request)
    const title = String(body.title || '').trim()
    const content = String(body.content || '')
    const tagId = Number(body.tagId || 0)
    const readRole = Number(body.readRole || 0)
    const editingPid = normalizeNullableString(body.pid)
    const hide = Boolean(body.hide)
    const hideContent = normalizeNullableString(body.hideContent)
    const payPoint = Number(body.payPoint || 0)

    if (title.length < 4 || content.trim().length < 6 || !tagId) {
      return json({ success: false, message: '标题、内容或标签不合法' })
    }

    const config = await getSysConfig(env)
    if (config.turnstile?.enable) {
      const turnstile = await verifyTurnstile(config.turnstile.secretKey, body.token, 'newPost', request)
      if (!turnstile.success) {
        return json(turnstile)
      }
    }

    if (editingPid) {
      const existing = await first(env, 'SELECT uid FROM posts WHERE pid = ?', [editingPid])
      if (!existing) {
        return json({ success: false, message: '帖子不存在' })
      }
      if (existing.uid !== currentUser.uid) {
        return json({ success: false, message: '无权修改该帖子' })
      }

      await run(env, `
        UPDATE posts
        SET title = ?, content = ?, tag_id = ?, read_role = ?, hide = ?, hide_content = ?, pay_point = ?, updated_at = ?
        WHERE pid = ?
      `, [title, content, tagId, readRole, hide ? 1 : 0, hideContent, payPoint, nowIso(), editingPid])

      return json({ success: true, pid: editingPid })
    }

    let pid = randomId('p')
    if (config.postUrlFormat?.type === 'Number') {
      const maxRow = await first(env, 'SELECT MAX(id) AS id FROM posts', [])
      pid = String(Number(maxRow?.id ?? 0) + Number(config.postUrlFormat.minNumber ?? 10000) + 1)
    }
    else if (config.postUrlFormat?.type === 'Date') {
      pid = formatDatePid(config.postUrlFormat.dateFormat)
    }

    const now = nowIso()
    const initialPoint = calculateHotPoint(currentUser.point, 0, 0, now)
    await run(env, `
      INSERT INTO posts (
        pid, created_at, updated_at, title, content, uid, tag_id, read_role, point, hide, hide_content, pay_point, last_comment_time
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [pid, now, now, title, content, currentUser.uid, tagId, readRole, initialPoint, hide ? 1 : 0, hideContent, payPoint, now])

    await run(env, 'UPDATE tags SET count = count + 1 WHERE id = ?', [tagId])

    const todayPostPoints = await queryPointSum(env, currentUser.uid, 'POST')
    const limit = todayPostPoints >= Number(config.pointPerPostByDay || 0)
    const nextPoint = currentUser.point + (limit ? 0 : Number(config.pointPerPost || 0))

    await run(env, `
      UPDATE users
      SET post_count = post_count + 1, point = ?, level = ?, last_active = ?, updated_at = ?
      WHERE uid = ?
    `, [nextPoint, getUserLevelByPoint(nextPoint), now, now, currentUser.uid])

    if (!limit) {
      await run(env, 'INSERT INTO point_history (created_at, updated_at, reason, uid, pid, point) VALUES (?, ?, ?, ?, ?, ?)', [now, now, 'POST', currentUser.uid, pid, Number(config.pointPerPost || 0)])
    }

    return json({ success: true, pid })
  }

  if (pathname === '/api/post/support' && method === 'POST') {
    if (!currentUser) {
      return json({ success: false, message: '请先去登录' })
    }

    const pid = String(url.searchParams.get('pid') || '')
    if (!pid) {
      return json({ success: false, message: '帖子不存在' })
    }

    const exists = await queryCount(env, 'SELECT COUNT(*) AS count FROM post_support WHERE uid = ? AND pid = ?', [currentUser.uid, pid])
    if (exists > 0) {
      await run(env, 'DELETE FROM post_support WHERE uid = ? AND pid = ?', [currentUser.uid, pid])
    }
    else {
      await run(env, 'INSERT INTO post_support (uid, pid, created_at, updated_at) VALUES (?, ?, ?, ?)', [currentUser.uid, pid, nowIso(), nowIso()])
    }

    await run(env, 'UPDATE users SET last_active = ?, updated_at = ? WHERE uid = ?', [nowIso(), nowIso(), currentUser.uid])
    await syncPostPoint(env, pid)
    return json({ success: true })
  }

  if (pathname === '/api/post/fav' && method === 'POST') {
    if (!currentUser) {
      return json({ success: false, message: '请先去登录' })
    }

    const pid = String(url.searchParams.get('pid') || '')
    if (!pid) {
      return json({ success: false, message: '帖子不存在' })
    }

    const exists = await queryCount(env, 'SELECT COUNT(*) AS count FROM favorites WHERE user_id = ? AND pid = ?', [currentUser.id, pid])
    if (exists > 0) {
      await run(env, 'DELETE FROM favorites WHERE user_id = ? AND pid = ?', [currentUser.id, pid])
    }
    else {
      await run(env, 'INSERT INTO favorites (user_id, pid, created_at, updated_at) VALUES (?, ?, ?, ?)', [currentUser.id, pid, nowIso(), nowIso()])
    }
    return json({ success: true })
  }

  if (pathname === '/api/post/pay' && method === 'POST') {
    if (!currentUser) {
      return json({ success: false, content: '请先去登录' })
    }

    const body = await readBody(request)
    const pid = String(body.pid || '')
    const post = await first(env, 'SELECT pid, uid, hide_content, pay_point FROM posts WHERE pid = ?', [pid])
    if (!post) {
      return json({ success: false, content: '帖子不存在' })
    }

    const alreadyPaid = await queryCount(env, 'SELECT COUNT(*) AS count FROM payments WHERE pid = ? AND uid = ?', [pid, currentUser.uid])
    if (currentUser.uid === post.uid || alreadyPaid > 0) {
      return json({ success: true, content: post.hide_content || '' })
    }

    if (currentUser.point < Number(post.pay_point || 0)) {
      return json({ success: false, content: '积分不够' })
    }

    const author = await first(env, 'SELECT uid, point FROM users WHERE uid = ?', [post.uid])
    if (!author) {
      return json({ success: false, content: '帖子作者不存在' })
    }

    const amount = Number(post.pay_point || 0)
    const now = nowIso()
    const buyerPoint = currentUser.point - amount
    const authorPoint = Number(author.point || 0) + amount

    await run(env, 'UPDATE users SET point = ?, level = ?, last_active = ?, updated_at = ? WHERE uid = ?', [buyerPoint, getUserLevelByPoint(buyerPoint), now, now, currentUser.uid])
    await run(env, 'UPDATE users SET point = ?, level = ?, last_active = ?, updated_at = ? WHERE uid = ?', [authorPoint, getUserLevelByPoint(authorPoint), now, now, author.uid])
    await run(env, 'INSERT INTO point_history (created_at, updated_at, reason, uid, pid, point) VALUES (?, ?, ?, ?, ?, ?)', [now, now, 'PUTIN', currentUser.uid, pid, -amount])
    await run(env, 'INSERT INTO point_history (created_at, updated_at, reason, uid, pid, point) VALUES (?, ?, ?, ?, ?, ?)', [now, now, 'INCOME', author.uid, pid, amount])
    await run(env, 'INSERT INTO payments (created_at, pid, uid, point) VALUES (?, ?, ?, ?)', [now, pid, currentUser.uid, amount])

    return json({ success: true, content: post.hide_content || '' })
  }

  const postMatch = pathname.match(/^\/api\/post\/([^/]+)$/)
  if (postMatch && method === 'POST') {
    return handlePostDetail(env, currentUser, decodeURIComponent(postMatch[1]), request)
  }

  if (pathname === '/api/comment/new' && method === 'POST') {
    if (!currentUser) {
      return json({ success: false, message: '请先去登录' })
    }
    if (currentUser.status === 'BANNED') {
      return json({ success: false, message: '用户不存在或已被封禁' })
    }
    if (currentUser.point <= 0) {
      return json({ success: false, message: '用户积分小于或等于0分,不能回帖' })
    }

    const body = await readBody(request)
    const content = String(body.content || '').trim()
    const pid = String(body.pid || '')
    const editingCid = normalizeNullableString(body.cid)
    if (!content) {
      return json({ success: false, message: '评论内容不能为空' })
    }

    const config = await getSysConfig(env)
    if (config.turnstile?.enable) {
      const turnstile = await verifyTurnstile(config.turnstile.secretKey, body.token, 'reply', request)
      if (!turnstile.success) {
        return json(turnstile)
      }
    }

    const post = await first(env, `
      SELECT p.pid, p.uid, p.title, u.tg_chat_id AS author_tg_chat_id
      FROM posts p
      JOIN users u ON u.uid = p.uid
      WHERE p.pid = ?
    `, [pid])
    if (!post) {
      return json({ success: false, message: '帖子不存在' })
    }

    const now = nowIso()
    if (editingCid) {
      const currentComment = await first(env, 'SELECT uid FROM comments WHERE cid = ?', [editingCid])
      if (!currentComment || currentComment.uid !== currentUser.uid) {
        return json({ success: false, message: '无权编辑该评论' })
      }

      await run(env, 'UPDATE comments SET content = ?, updated_at = ? WHERE cid = ?', [content, now, editingCid])
      await run(env, 'UPDATE posts SET last_comment_time = ?, last_comment_uid = ?, updated_at = ? WHERE pid = ?', [now, currentUser.uid, now, pid])
      await syncPostPoint(env, pid)
      return json({ success: true })
    }

    const maxFloor = await first(env, 'SELECT MAX(floor) AS floor FROM comments WHERE pid = ?', [pid])
    const cid = randomId('c')
    const postAuthorUsername = await getUsernameByUid(env, post.uid)
    const mentioned = extractMentions(content).filter(name => name !== `@${postAuthorUsername}`)

    await run(env, `
      INSERT INTO comments (cid, created_at, updated_at, uid, pid, mentioned, content, floor)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `, [cid, now, now, currentUser.uid, pid, JSON.stringify(mentioned), content, Number(maxFloor?.floor ?? 0) + 1])

    const todayCommentPoints = await queryPointSum(env, currentUser.uid, 'COMMENT')
    const limit = todayCommentPoints >= Number(config.pointPerCommentByDay || 0)
    const nextPoint = currentUser.point + (limit ? 0 : Number(config.pointPerComment || 0))
    await run(env, `
      UPDATE users
      SET point = ?, level = ?, comment_count = comment_count + 1, last_active = ?, updated_at = ?
      WHERE uid = ?
    `, [nextPoint, getUserLevelByPoint(nextPoint), now, now, currentUser.uid])

    await run(env, 'UPDATE posts SET reply_count = reply_count + 1, last_comment_time = ?, last_comment_uid = ?, updated_at = ? WHERE pid = ?', [now, currentUser.uid, now, pid])

    if (!limit) {
      await run(env, 'INSERT INTO point_history (created_at, updated_at, reason, uid, pid, cid, point) VALUES (?, ?, ?, ?, ?, ?, ?)', [now, now, 'COMMENT', currentUser.uid, pid, cid, Number(config.pointPerComment || 0)])
    }

    if (currentUser.uid !== post.uid) {
      await run(env, `
        INSERT INTO messages (created_at, updated_at, read, from_uid, to_uid, content, type, relation_id)
        VALUES (?, ?, 0, ?, ?, ?, ?, ?)
      `, [now, now, currentUser.uid, post.uid, `你的帖子<a class="mx-1 text-blue-500" href='/post/${pid}#${cid}'>${post.title}</a>有了新回复`, 'COMMENT', pid])
      await sendTgMessage(
        config,
        post.author_tg_chat_id,
        `你的帖子《${post.title}》有了新回复${buildSiteLink(config, `/post/${pid}#${cid}`) ? `\n${buildSiteLink(config, `/post/${pid}#${cid}`)}` : ''}`,
      )
    }

    for (const mention of mentioned) {
      const target = await first(env, 'SELECT uid, tg_chat_id FROM users WHERE username = ?', [mention.slice(1)])
      if (target) {
        await run(env, `
          INSERT INTO messages (created_at, updated_at, read, from_uid, to_uid, content, type, relation_id)
          VALUES (?, ?, 0, ?, ?, ?, ?, ?)
        `, [now, now, currentUser.uid, target.uid, `你在帖子<a class="text-blue-500 mx-1" href='/post/${pid}#${cid}'>${post.title}</a>中被提到了`, 'MENTIONED', pid])
        await sendTgMessage(
          config,
          target.tg_chat_id,
          `你在帖子《${post.title}》中被提到了${buildSiteLink(config, `/post/${pid}#${cid}`) ? `\n${buildSiteLink(config, `/post/${pid}#${cid}`)}` : ''}`,
        )
      }
    }

    await syncPostPoint(env, pid)
    return json({ success: true })
  }

  if (pathname === '/api/comment/like' && method === 'POST') {
    return handleCommentReaction(env, currentUser, url.searchParams.get('cid'), 'LIKE')
  }

  if (pathname === '/api/comment/dislike' && method === 'POST') {
    return handleCommentReaction(env, currentUser, url.searchParams.get('cid'), 'DISLIKE')
  }

  const commentDetailMatch = pathname.match(/^\/api\/comment\/detail\/([^/]+)$/)
  if (commentDetailMatch && method === 'POST') {
    return handleCommentDetail(env, currentUser, decodeURIComponent(commentDetailMatch[1]))
  }

  if (pathname === '/api/member/post' && method === 'POST') {
    const body = await readBody(request)
    const username = String(body.username || '')
    const page = getPage(body.page)
    const size = getSize(body.size, 20)
    const user = await first(env, 'SELECT * FROM users WHERE username = ?', [username])
    if (!user) {
      return json({ success: false, message: '用户不存在' })
    }

    const includeFav = currentUser?.id != null
    const rows = await all(env, postListSql('WHERE p.uid = ?', 'p.created_at DESC', 'LIMIT ? OFFSET ?', includeFav), includeFav ? [currentUser.id, user.uid, size, (page - 1) * size] : [user.uid, size, (page - 1) * size])
    const total = await queryCount(env, 'SELECT COUNT(*) AS count FROM posts WHERE uid = ?', [user.uid])

    return json({
      success: true,
      posts: await buildPostSummaries(env, rows, currentUser?.id),
      total,
    })
  }

  if (pathname === '/api/member/comment' && method === 'POST') {
    const body = await readBody(request)
    const username = String(body.username || '')
    const page = getPage(body.page)
    const size = getSize(body.size, 20)
    const user = await first(env, 'SELECT * FROM users WHERE username = ?', [username])
    if (!user) {
      return json({ success: false, message: '用户不存在' })
    }

    const rows = await all(env, `
      SELECT
        c.*,
        u.id AS author_id,
        u.uid AS author_uid,
        u.username AS author_username,
        u.avatar_url AS author_avatar_url,
        u.head_img AS author_head_img,
        u.role AS author_role,
        u.signature AS author_signature,
        p.pid AS post_pid,
        p.title AS post_title,
        p.created_at AS post_created_at
      FROM comments c
      JOIN users u ON u.uid = c.uid
      JOIN posts p ON p.pid = c.pid
      WHERE c.uid = ?
      ORDER BY c.created_at DESC
      LIMIT ? OFFSET ?
    `, [user.uid, size, (page - 1) * size])

    const total = await queryCount(env, 'SELECT COUNT(*) AS count FROM comments WHERE uid = ?', [user.uid])

    return json({
      success: true,
      comments: await buildCommentsWithPosts(env, rows),
      total,
    })
  }

  if (pathname === '/api/member/fav' && method === 'POST') {
    const body = await readBody(request)
    const username = String(body.username || '')
    const page = getPage(body.page)
    const size = getSize(body.size, 20)
    const user = await first(env, 'SELECT * FROM users WHERE username = ?', [username])
    if (!user) {
      return json({ success: false, message: '用户不存在' })
    }

    const rows = await all(env, `
      SELECT
        p.*,
        au.id AS author_id,
        au.uid AS author_uid,
        au.username AS author_username,
        au.avatar_url AS author_avatar_url,
        au.head_img AS author_head_img,
        au.role AS author_role,
        au.signature AS author_signature,
        t.name AS tag_name,
        t.en_name AS tag_en_name,
        t."desc" AS tag_desc,
        t.count AS tag_count,
        t.hot AS tag_hot,
        lu.uid AS last_comment_user_uid,
        lu.username AS last_comment_user_username,
        (SELECT COUNT(*) FROM comments c WHERE c.pid = p.pid) AS comments_count,
        (SELECT COUNT(*) FROM post_support ps WHERE ps.pid = p.pid) AS support_count,
        1 AS fav_count
      FROM favorites f
      JOIN posts p ON p.pid = f.pid
      JOIN users au ON au.uid = p.uid
      JOIN tags t ON t.id = p.tag_id
      LEFT JOIN users lu ON lu.uid = p.last_comment_uid
      WHERE f.user_id = ?
      ORDER BY f.created_at DESC
      LIMIT ? OFFSET ?
    `, [user.id, size, (page - 1) * size])

    const total = await queryCount(env, 'SELECT COUNT(*) AS count FROM favorites WHERE user_id = ?', [user.id])

    return json({
      success: true,
      posts: await buildPostSummaries(env, rows, user.id),
      total,
    })
  }

  if (pathname === '/api/member/point' && method === 'POST') {
    if (!currentUser) {
      return json({ success: false, message: '请先去登录' })
    }

    const body = await readBody(request)
    const username = String(body.username || '')
    const page = getPage(body.page)
    const size = getSize(body.size, 20)
    const user = await first(env, 'SELECT * FROM users WHERE username = ?', [username])
    if (!user) {
      return json({ success: false, message: '用户不存在' })
    }

    const rows = await all(env, `
      SELECT ph.*, p.pid AS post_pid, p.title AS post_title
      FROM point_history ph
      LEFT JOIN posts p ON p.pid = ph.pid
      WHERE ph.uid = ?
      ORDER BY ph.created_at DESC
      LIMIT ? OFFSET ?
    `, [user.uid, size, (page - 1) * size])

    const total = await queryCount(env, 'SELECT COUNT(*) AS count FROM point_history WHERE uid = ?', [user.uid])

    return json({
      success: true,
      points: rows.map(row => ({
        createdAt: row.created_at,
        pid: row.pid,
        cid: row.cid,
        reason: row.reason,
        point: Number(row.point),
        remark: row.remark,
        post: row.post_pid ? { pid: row.post_pid, title: row.post_title } : null,
        comment: row.cid ? { cid: row.cid, pid: row.pid } : null,
      })),
      total,
    })
  }

  const memberMatch = pathname.match(/^\/api\/member\/([^/]+)$/)
  if (memberMatch && method === 'POST') {
    const username = decodeURIComponent(memberMatch[1])
    if (!['privateMsg', 'privateMsgList', 'sendMsg', 'message', 'readMessage'].includes(username)) {
      return handleMemberDetail(env, currentUser, username)
    }
  }

  if (pathname === '/api/manage/config/get' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    return json({ success: true, config: await getSysConfig(env) })
  }

  if (pathname === '/api/manage/config/save' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    await saveSysConfig(env, body)
    return json({ success: true })
  }

  if (pathname === '/api/manage/tagList' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    const page = getPage(body.page)
    const size = getSize(body.size, 20)
    const rows = await all(env, 'SELECT id, name, en_name, "desc", count, hot FROM tags ORDER BY hot DESC, id DESC LIMIT ? OFFSET ?', [size, (page - 1) * size])
    const total = await queryCount(env, 'SELECT COUNT(*) AS count FROM tags', [])
    return json({ success: true, tags: rows.map(mapTag), total })
  }

  if (pathname === '/api/manage/saveTag' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    const id = Number(body.id || 0)
    const name = String(body.name || '').trim()
    const enName = String(body.enName || '').trim()
    const desc = String(body.desc || '').trim()
    if (!name || !enName || !desc) {
      return json({ success: false, message: '请填写完整,都是必填字段' })
    }
    const duplicate = await first(env, `
      SELECT id
      FROM tags
      WHERE (name = ? OR en_name = ?)
        AND id != ?
      LIMIT 1
    `, [name, enName, id])
    if (duplicate) {
      return json({ success: false, message: '标签名称或编码已存在' })
    }
    if (id > 0) {
      await run(env, 'UPDATE tags SET name = ?, en_name = ?, "desc" = ? WHERE id = ?', [name, enName, desc, id])
    }
    else {
      await run(env, 'INSERT INTO tags (name, en_name, "desc", count, hot) VALUES (?, ?, ?, 0, 0)', [name, enName, desc])
    }
    return json({ success: true })
  }

  if (pathname === '/api/manage/toggleHot' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    const tag = await first(env, 'SELECT hot FROM tags WHERE id = ?', [Number(body.id || 0)])
    if (!tag) {
      return json({ success: false, message: '标签不存在' })
    }
    await run(env, 'UPDATE tags SET hot = ? WHERE id = ?', [Number(tag.hot) === 1 ? 0 : 1, Number(body.id || 0)])
    return json({ success: true })
  }

  if (pathname === '/api/manage/title/titleList' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    const page = getPage(body.page)
    const size = getSize(body.size, 20)
    const where = body.onlyEnabled ? 'WHERE status = 1' : ''
    const rows = await all(env, `SELECT id, title, count, style, status FROM titles ${where} ORDER BY id DESC LIMIT ? OFFSET ?`, [size, (page - 1) * size])
    return json({
      success: true,
      titles: rows.map(row => ({
        id: row.id,
        title: row.title,
        count: Number(row.count ?? 0),
        style: row.style,
        status: Number(row.status) === 1,
      })),
    })
  }

  if (pathname === '/api/manage/title/saveTitle' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    const id = Number(body.id || 0)
    const title = String(body.title || '').trim()
    const style = String(body.style || 'primary')
    const status = body.status ? 1 : 0
    if (!title) {
      return json({ success: false, message: '请填写完整,头衔必填字段' })
    }
    if (id > 0) {
      await run(env, 'UPDATE titles SET title = ?, style = ?, status = ? WHERE id = ?', [title, style, status, id])
    }
    else {
      await run(env, 'INSERT INTO titles (title, count, style, status) VALUES (?, 0, ?, ?)', [title, style, status])
    }
    return json({ success: true })
  }

  if (pathname === '/api/manage/title/assign' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    const user = await first(env, 'SELECT id FROM users WHERE uid = ?', [String(body.uid || '')])
    const title = await first(env, 'SELECT id FROM titles WHERE title = ?', [String(body.title || '')])
    if (!user || !title) {
      return json({ success: false, message: '用户或头衔不存在' })
    }
    const exists = await queryCount(env, 'SELECT COUNT(*) AS count FROM user_titles WHERE user_id = ? AND title_id = ?', [user.id, title.id])
    if (exists === 0) {
      await run(env, 'INSERT INTO user_titles (user_id, title_id) VALUES (?, ?)', [user.id, title.id])
      await run(env, 'UPDATE titles SET count = count + 1 WHERE id = ?', [title.id])
    }
    return json({ success: true })
  }

  if (pathname === '/api/manage/title/remove' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    const userId = Number(body.userId || 0)
    const titleId = Number(body.titleId || 0)
    const exists = await queryCount(env, 'SELECT COUNT(*) AS count FROM user_titles WHERE user_id = ? AND title_id = ?', [userId, titleId])
    if (exists > 0) {
      await run(env, 'DELETE FROM user_titles WHERE user_id = ? AND title_id = ?', [userId, titleId])
      await run(env, 'UPDATE titles SET count = CASE WHEN count > 0 THEN count - 1 ELSE 0 END WHERE id = ?', [titleId])
    }
    return json({ success: true })
  }

  if (pathname === '/api/manage/userList' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    const page = getPage(body.page)
    const size = getSize(body.size, 20)
    const username = String(body.username || '').trim()
    const where = username ? 'WHERE username LIKE ?' : ''
    const args = username ? [`%${username}%`, size, (page - 1) * size] : [size, (page - 1) * size]
    const rows = await all(env, `SELECT * FROM users ${where} ORDER BY created_at DESC LIMIT ? OFFSET ?`, args)
    const total = await queryCount(env, `SELECT COUNT(*) AS count FROM users ${where}`, username ? [`%${username}%`] : [])
    const users = await Promise.all(rows.map(row => buildUserSummary(env, row)))
    return json({ success: true, users, total })
  }

  if (pathname === '/api/manage/member/banUser' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    const uid = String(body.uid || '')
    const day = Number(body.day || 0)
    await run(env, 'UPDATE users SET status = ?, banned_end = ?, updated_at = ? WHERE uid = ?', ['BANNED', new Date(Date.now() + day * DAY_MS).toISOString(), nowIso(), uid])
    return json({ success: true })
  }

  if (pathname === '/api/manage/member/revokeBanUser' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    await run(env, 'UPDATE users SET status = ?, banned_end = NULL, updated_at = ? WHERE uid = ?', ['NORMAL', nowIso(), String(body.uid || '')])
    return json({ success: true })
  }

  if (pathname === '/api/manage/member/point' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    const reason = String(body.reason || '')
    const amount = Number(body.amount || 0)
    const uid = String(body.uid || '')
    const remark = String(body.remark || '')
    if (!reason || !amount || !uid || !remark) {
      return json({ success: false, message: '参数错误' })
    }
    const signed = reason === 'SEND' ? amount : -amount
    const user = await first(env, 'SELECT point FROM users WHERE uid = ?', [uid])
    if (!user) {
      return json({ success: false, message: '用户不存在' })
    }
    const nextPoint = Number(user.point || 0) + signed
    const now = nowIso()
    await run(env, 'INSERT INTO point_history (created_at, updated_at, reason, uid, point, remark) VALUES (?, ?, ?, ?, ?, ?)', [now, now, reason, uid, signed, remark])
    await run(env, 'UPDATE users SET point = ?, level = ?, updated_at = ? WHERE uid = ?', [nextPoint, getUserLevelByPoint(nextPoint), now, uid])
    return json({ success: true, message: '积分操作成功' })
  }

  if (pathname === '/api/manage/post/postList' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    const page = getPage(body.page)
    const size = getSize(body.size, 20)
    const username = String(body.username || '').trim()
    const where = username ? 'WHERE au.username LIKE ?' : ''
    const includeFav = currentUser?.id != null
    const args = username
      ? includeFav ? [currentUser.id, `%${username}%`, size, (page - 1) * size] : [`%${username}%`, size, (page - 1) * size]
      : includeFav ? [currentUser.id, size, (page - 1) * size] : [size, (page - 1) * size]
    const rows = await all(env, `
      SELECT
        p.*,
        au.id AS author_id,
        au.uid AS author_uid,
        au.username AS author_username,
        au.avatar_url AS author_avatar_url,
        au.head_img AS author_head_img,
        au.role AS author_role,
        au.signature AS author_signature,
        t.name AS tag_name,
        t.en_name AS tag_en_name,
        t."desc" AS tag_desc,
        t.count AS tag_count,
        t.hot AS tag_hot,
        lu.uid AS last_comment_user_uid,
        lu.username AS last_comment_user_username,
        (SELECT COUNT(*) FROM comments c WHERE c.pid = p.pid) AS comments_count,
        (SELECT COUNT(*) FROM post_support ps WHERE ps.pid = p.pid) AS support_count,
        ${includeFav ? '(SELECT COUNT(*) FROM favorites f WHERE f.pid = p.pid AND f.user_id = ?) AS fav_count' : '0 AS fav_count'}
      FROM posts p
      JOIN users au ON au.uid = p.uid
      JOIN tags t ON t.id = p.tag_id
      LEFT JOIN users lu ON lu.uid = p.last_comment_uid
      ${where}
      ORDER BY p.created_at DESC
      LIMIT ? OFFSET ?
    `, args)
    const total = await queryCount(env, `SELECT COUNT(*) AS count FROM posts p JOIN users au ON au.uid = p.uid ${where}`, username ? [`%${username}%`] : [])
    return json({ success: true, posts: await buildPostSummaries(env, rows, currentUser?.id), total })
  }

  if (pathname === '/api/manage/post/togglePin' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const pid = String(url.searchParams.get('pid') || '')
    const post = await first(env, 'SELECT pinned FROM posts WHERE pid = ?', [pid])
    if (!post) {
      return json({ success: false, message: '帖子不存在' })
    }
    await run(env, 'UPDATE posts SET pinned = ?, updated_at = ? WHERE pid = ?', [Number(post.pinned) === 1 ? 0 : 1, nowIso(), pid])
    return json({ success: true })
  }

  if (pathname === '/api/manage/post/delete' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const pid = String(url.searchParams.get('pid') || '')
    const post = await first(env, 'SELECT uid, tag_id FROM posts WHERE pid = ?', [pid])
    if (!post) {
      return json({ success: false, message: '帖子不存在' })
    }
    await run(env, 'DELETE FROM comment_likes WHERE pid = ?', [pid])
    await run(env, 'DELETE FROM comment_dislikes WHERE pid = ?', [pid])
    await run(env, 'DELETE FROM comments WHERE pid = ?', [pid])
    await run(env, 'DELETE FROM favorites WHERE pid = ?', [pid])
    await run(env, 'DELETE FROM point_history WHERE pid = ?', [pid])
    await run(env, 'DELETE FROM post_support WHERE pid = ?', [pid])
    await run(env, 'DELETE FROM payments WHERE pid = ?', [pid])
    await run(env, 'DELETE FROM posts WHERE pid = ?', [pid])
    await run(env, 'UPDATE users SET post_count = CASE WHEN post_count > 0 THEN post_count - 1 ELSE 0 END WHERE uid = ?', [post.uid])
    await run(env, 'UPDATE tags SET count = CASE WHEN count > 0 THEN count - 1 ELSE 0 END WHERE id = ?', [post.tag_id])
    return json({ success: true })
  }

  if (pathname === '/api/manage/commentList' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const body = await readBody(request)
    const page = getPage(body.page)
    const size = getSize(body.size, 20)
    const whereParts: string[] = []
    const args: any[] = []
    if (body.username) {
      whereParts.push('u.username LIKE ?')
      args.push(`%${String(body.username).trim()}%`)
    }
    if (body.pid) {
      whereParts.push('c.pid = ?')
      args.push(String(body.pid).trim())
    }
    const where = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : ''
    const rows = await all(env, `
      SELECT
        c.*,
        u.uid AS author_uid,
        u.username AS author_username,
        u.avatar_url AS author_avatar_url,
        u.head_img AS author_head_img,
        p.title AS post_title
      FROM comments c
      JOIN users u ON u.uid = c.uid
      JOIN posts p ON p.pid = c.pid
      ${where}
      ORDER BY c.created_at DESC
      LIMIT ? OFFSET ?
    `, [...args, size, (page - 1) * size])
    const total = await queryCount(env, `SELECT COUNT(*) AS count FROM comments c JOIN users u ON u.uid = c.uid ${where}`, args)
    return json({ success: true, comments: await Promise.all(rows.map(row => buildManageComment(env, row))), total })
  }

  if (pathname === '/api/manage/comment/delete' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }
    const cid = String(url.searchParams.get('cid') || '')
    const comment = await first(env, 'SELECT uid, pid FROM comments WHERE cid = ?', [cid])
    if (!comment) {
      return json({ success: false, message: '评论不存在' })
    }
    await run(env, 'DELETE FROM comment_likes WHERE cid = ?', [cid])
    await run(env, 'DELETE FROM comment_dislikes WHERE cid = ?', [cid])
    await run(env, 'DELETE FROM point_history WHERE cid = ?', [cid])
    await run(env, 'DELETE FROM comments WHERE cid = ?', [cid])
    await run(env, 'UPDATE users SET comment_count = CASE WHEN comment_count > 0 THEN comment_count - 1 ELSE 0 END WHERE uid = ?', [comment.uid])
    await run(env, 'UPDATE posts SET reply_count = CASE WHEN reply_count > 0 THEN reply_count - 1 ELSE 0 END WHERE pid = ?', [comment.pid])
    await syncPostPoint(env, comment.pid)
    return json({ success: true })
  }

  if (pathname === '/api/member/sendEmail' && method === 'POST') {
    const body = await readBody(request)
    const scene = String(body.scene || body.sence || '').trim().toUpperCase()
    const email = normalizeEmail(String(body.email || ''))
    if (!isValidEmail(email)) {
      return json({ success: false, emailCodeKey: '', message: '请填写正确的邮箱地址' })
    }
    if (scene !== 'REGISTER') {
      return json({ success: false, emailCodeKey: '', message: '发送邮件失败' })
    }

    const config = await getSysConfig(env)
    if (!config.regWithEmailCodeVerify) {
      return json({ success: false, emailCodeKey: '', message: '未开启邮件验证码验证注册' })
    }

    const emailError = validateEmailConfig(config.email)
    if (emailError) {
      return json({ success: false, emailCodeKey: '', message: emailError })
    }

    const exists = await queryCount(env, 'SELECT COUNT(*) AS count FROM users WHERE email = ?', [email])
    if (exists > 0) {
      return json({ success: false, emailCodeKey: '', message: '邮箱已经存在了' })
    }

    const rateLimited = await isEmailSendRateLimited(env, email, 'REGISTER')
    if (rateLimited) {
      return json({ success: false, emailCodeKey: '', message: '不要频繁发送邮件!' })
    }

    const emailCodeKey = randomId('em_')
    const emailCode = randomNumericCode(6)
    const subject = `${config.websiteName} 注册邮件 Register Email`
    const html = buildRegisterEmailHtml(config, emailCode)
    const sent = await sendResendEmail(config.email, email, subject, html, emailCodeKey)
    if (!sent.success) {
      return json({ success: false, emailCodeKey: '', message: sent.message })
    }

    await saveEmailCodeRecord(env, {
      key: emailCodeKey,
      code: emailCode,
      reason: 'REGISTER',
      targetEmail: email,
      validMinutes: 5,
    })

    return json({ success: true, emailCodeKey, message: '发送邮件成功' })
  }

  if (pathname === '/api/manage/testEmail' && method === 'POST') {
    if (!isAdmin(currentUser)) {
      return json({ success: false, message: '只有管理员才能访问' })
    }

    const body = await readBody(request)
    const emailConfig = normalizeEmailConfig(body.email)
    const emailError = validateEmailConfig(emailConfig)
    if (emailError) {
      return json({ success: false, message: emailError })
    }
    if (!isValidEmail(emailConfig.to)) {
      return json({ success: false, message: '请填写测试邮件接收地址' })
    }

    const sent = await sendResendEmail(
      emailConfig,
      emailConfig.to,
      'Discussion 测试邮件 Test Email',
      '<p>这是一封测试邮件 This is a test email</p>',
    )
    if (!sent.success) {
      return json({ success: false, message: sent.message })
    }

    return json({ success: true, message: '发送成功' })
  }

  if (pathname === '/api/member/sendForgotPasswordEmail' && method === 'POST') {
    const body = await readBody(request)
    const identify = String(body.identify || '').trim()
    if (!identify) {
      return json({ success: false, emailCodeKey: '', message: '请输入用户名或邮箱' })
    }

    const emailIdentify = normalizeEmail(identify)
    const target = await first(env, 'SELECT id, email FROM users WHERE username = ? OR email = ? LIMIT 1', [identify, emailIdentify])
    if (!target) {
      return json({ success: false, emailCodeKey: '', message: '发送邮件失败,请检查用户名或邮箱' })
    }

    const config = await getSysConfig(env)
    const emailError = validateEmailConfig(config.email)
    if (emailError) {
      return json({ success: false, emailCodeKey: '', message: emailError })
    }

    const targetEmail = normalizeEmail(String(target.email || ''))
    const rateLimited = await isEmailSendRateLimited(env, targetEmail, 'RESET_PASSWORD')
    if (rateLimited) {
      return json({ success: false, emailCodeKey: '', message: '不要频繁发送邮件!' })
    }

    const emailCodeKey = randomId('em_')
    const emailCode = randomNumericCode(6)
    const subject = `${config.websiteName} 重置密码邮件 Reset Password Email`
    const html = buildResetPasswordEmailHtml(config, emailCode)
    const sent = await sendResendEmail(config.email, targetEmail, subject, html, emailCodeKey)
    if (!sent.success) {
      return json({ success: false, emailCodeKey: '', message: sent.message })
    }

    await saveEmailCodeRecord(env, {
      key: emailCodeKey,
      code: emailCode,
      reason: 'RESET_PASSWORD',
      targetEmail,
      validMinutes: 30,
    })

    return json({ success: true, message: '发送邮件成功', emailCodeKey })
  }

  if (pathname === '/api/member/resetPwd' && method === 'POST') {
    const body = await readBody(request)
    const identify = String(body.identify || '').trim()
    const emailCode = String(body.emailCode || '').trim()
    const emailCodeKey = String(body.emailCodeKey || '').trim()
    const password = String(body.password || '')
    const repeatPassword = String(body.repeatPassword || '')

    if (!identify || !emailCode || !emailCodeKey || !password || !repeatPassword) {
      return json({ success: false, message: '请输入完整的信息' })
    }
    if (password.length < 6) {
      return json({ success: false, message: '密码最少6个字符' })
    }
    if (password !== repeatPassword) {
      return json({ success: false, message: '两次密码不一致' })
    }

    const emailIdentify = normalizeEmail(identify)
    const target = await first(env, 'SELECT id, uid, email FROM users WHERE username = ? OR email = ? LIMIT 1', [identify, emailIdentify])
    if (!target) {
      return json({ success: false, message: '重置失败,请检查用户名或邮箱' })
    }

    const record = await first(env, 'SELECT * FROM email_codes WHERE key = ?', [emailCodeKey])
    if (!record
      || String(record.reason) !== 'RESET_PASSWORD'
      || Number(record.used) === 1
      || String(record.valid_at) < nowIso()
      || !isSameEmail(String(record.target_email || ''), String(target.email || ''))) {
      return json({ success: false, message: '验证码错误或已过期或已使用' })
    }
    if (String(record.code).toUpperCase() !== emailCode.toUpperCase()) {
      return json({ success: false, message: '验证码错误' })
    }

    const now = nowIso()
    const passwordHash = await hashPassword(password)
    await run(env, 'UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?', [passwordHash, now, target.id])
    await run(env, 'UPDATE email_codes SET used = 1, updated_at = ? WHERE id = ?', [now, record.id])

    return json({ success: true, message: '重置成功' })
  }

  if (pathname === '/api/imgs/upload' && method === 'POST') {
    return handleImageUpload(request, env, currentUser)
  }

  if (pathname === '/api/member/sendMsg' && method === 'POST') {
    return handleSendPrivateMessage(request, env, currentUser)
  }

  if (pathname === '/api/member/privateMsgList' && method === 'POST') {
    return handlePrivateMessageList(request, env, currentUser)
  }

  if (pathname === '/api/member/privateMsg' && method === 'POST') {
    return handlePrivateMessageInbox(request, env, currentUser)
  }

  if (pathname === '/api/member/message' && method === 'POST') {
    return handleMemberMessages(request, env, currentUser)
  }

  if (pathname === '/api/member/readMessage' && method === 'POST') {
    return handleReadMessages(url, env, currentUser)
  }

  if (pathname === '/api/tg' && method === 'POST') {
    return handleTelegramWebhook(request, env)
  }

  return json({ success: false, message: '接口不存在' })
}

async function handleImageAsset(request: Request, env: Env, url: URL) {
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

async function handleImageUpload(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function buildConfigResponse(env: Env) {
  const config = await getSysConfig(env)
  const headers = new Headers({
    'Cache-Control': 'no-store',
  })
  return json({
    success: true,
    data: getPublicSysConfig(config),
    version: '1.0',
  }, headers)
}

async function buildVersionResponse(env: Env) {
  return json({
    success: true,
    version: '1.0',
  })
}

async function buildTagListResponse(env: Env, url: URL) {
  const hot = url.searchParams.get('hot')
  const name = url.searchParams.get('name')
  const where: string[] = []
  const args: any[] = []

  if (hot === 'true') {
    where.push('hot = 1')
  }
  if (name) {
    where.push('en_name = ?')
    args.push(name)
  }

  const sql = `SELECT id, name, en_name, "desc", count, hot FROM tags${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY hot DESC, count DESC, id ASC`
  const rows = await all(env, sql, args)

  return json({
    success: true,
    tags: rows.map(mapTag),
  })
}

async function buildMemberHotResponse(env: Env) {
  const since = new Date(Date.now() - 3 * DAY_MS).toISOString()
  const rows = await all(env, `
    SELECT u.uid, u.username, u.avatar_url, u.head_img, SUM(ph.point) AS points
    FROM point_history ph
    JOIN users u ON u.uid = ph.uid
    WHERE ph.created_at > ?
      AND ph.reason NOT IN ('INVITE', 'PUTIN')
    GROUP BY u.uid, u.username, u.avatar_url, u.head_img
    HAVING SUM(ph.point) > 0
    ORDER BY points DESC
    LIMIT 10
  `, [since])

  return json(rows.map(row => ({
    uid: row.uid,
    username: row.username,
    avatarUrl: row.avatar_url,
    headImg: row.head_img,
    points: Number(row.points ?? 0),
  })))
}

function getPostListInputFromUrl(url: URL) {
  return {
    uid: url.searchParams.get('uid') || '',
    tag: url.searchParams.get('tag') || '',
    key: url.searchParams.get('key') || '',
    page: url.searchParams.get('page') || '',
    size: url.searchParams.get('size') || '',
  }
}

async function buildPostListResponse(env: Env, currentUser: CurrentUser | null, input: any) {
  const page = getPage(input.page)
  const size = getSize(input.size, 20)
  const filters: string[] = ['p.read_role != 999']
  const args: any[] = []

  if (input.uid) {
    filters.push('p.uid = ?')
    args.push(String(input.uid))
  }
  if (input.tag) {
    filters.push('t.en_name = ?')
    args.push(String(input.tag))
  }
  if (input.key) {
    filters.push('p.title LIKE ?')
    args.push(`%${String(input.key).trim()}%`)
  }

  const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
  const includeFav = currentUser?.id != null
  const queryArgs = includeFav ? [currentUser.id, ...args] : args
  const pinnedRows = await all(env, postListSql(`${whereClause}${whereClause ? ' AND ' : 'WHERE '}p.pinned = 1`, 'p.created_at DESC', '', includeFav), queryArgs)
  const postRows = await all(env, postListSql(`${whereClause}${whereClause ? ' AND ' : 'WHERE '}p.pinned = 0`, 'p.point DESC', 'LIMIT ? OFFSET ?', includeFav), [...queryArgs, size, (page - 1) * size])
  const totalRow = await first(env, `SELECT COUNT(*) AS count FROM posts p JOIN tags t ON t.id = p.tag_id ${whereClause}`, args)

  const posts = await buildPostSummaries(env, [...pinnedRows, ...postRows], currentUser?.id)
  return json({
    success: true,
    posts,
    total: Number(totalRow?.count ?? 0),
  })
}

async function handlePostDetail(env: Env, currentUser: CurrentUser | null, pid: string, request: Request) {
  const body = await readBody(request)
  const page = getPage(body.page)
  const size = getSize(body.size, 20)

  const postMeta = await first(env, 'SELECT uid, read_role FROM posts WHERE pid = ?', [pid])
  if (!postMeta) {
    return json({ success: false, message: '帖子不存在' })
  }

  const level = currentUser ? getUserLevelByPoint(currentUser.point) : 0
  let canContinue = false
  if (currentUser?.role === 'ADMIN') {
    canContinue = true
  }
  if (!canContinue && currentUser?.uid === postMeta.uid) {
    canContinue = true
  }
  if (!currentUser && Number(postMeta.read_role) > 0) {
    return json({ success: false, message: '本帖需要注册用户才能查看' })
  }
  if (!canContinue && level >= Number(postMeta.read_role)) {
    canContinue = true
  }
  if (!canContinue) {
    return json({ success: false, message: `查看本帖需要Lv${postMeta.read_role}，您的权限不足` })
  }

  if (body.count) {
    await run(env, 'UPDATE posts SET view_count = view_count + 1, updated_at = ? WHERE pid = ?', [nowIso(), pid])
  }

  const includeFav = currentUser?.id != null
  const row = await first(env, `
    SELECT
      p.*,
      au.id AS author_id,
      au.uid AS author_uid,
      au.username AS author_username,
      au.avatar_url AS author_avatar_url,
      au.head_img AS author_head_img,
      au.role AS author_role,
      au.signature AS author_signature,
      t.name AS tag_name,
      t.en_name AS tag_en_name,
      t."desc" AS tag_desc,
      t.count AS tag_count,
      t.hot AS tag_hot,
      lu.uid AS last_comment_user_uid,
      lu.username AS last_comment_user_username,
      (SELECT COUNT(*) FROM comments c WHERE c.pid = p.pid) AS comments_count,
      (SELECT COUNT(*) FROM post_support ps WHERE ps.pid = p.pid) AS support_count,
      ${includeFav ? '(SELECT COUNT(*) FROM favorites f WHERE f.pid = p.pid AND f.user_id = ?) AS fav_count' : '0 AS fav_count'}
    FROM posts p
    JOIN users au ON au.uid = p.uid
    JOIN tags t ON t.id = p.tag_id
    LEFT JOIN users lu ON lu.uid = p.last_comment_uid
    WHERE p.pid = ?
  `, includeFav ? [currentUser.id, pid] : [pid])

  if (!row) {
    return json({ success: false, message: '帖子不存在' })
  }

  const includeReactionState = Boolean(currentUser?.uid)
  const commentRows = await all(env, `
    SELECT
      c.*,
      u.id AS author_id,
      u.uid AS author_uid,
      u.username AS author_username,
      u.avatar_url AS author_avatar_url,
      u.head_img AS author_head_img,
      u.role AS author_role,
      u.signature AS author_signature,
      (SELECT COUNT(*) FROM comment_likes cl WHERE cl.cid = c.cid) AS like_count,
      (SELECT COUNT(*) FROM comment_dislikes cd WHERE cd.cid = c.cid) AS dislike_count,
      ${includeReactionState
        ? `(SELECT COUNT(*) FROM comment_likes cl WHERE cl.cid = c.cid AND cl.uid = ?) AS liked_count,
      (SELECT COUNT(*) FROM comment_dislikes cd WHERE cd.cid = c.cid AND cd.uid = ?) AS disliked_count`
        : '0 AS liked_count, 0 AS disliked_count'}
    FROM comments c
    JOIN users u ON u.uid = c.uid
    WHERE c.pid = ?
    ORDER BY c.created_at ASC
    LIMIT ? OFFSET ?
  `, includeReactionState ? [currentUser!.uid, currentUser!.uid, pid, size, (page - 1) * size] : [pid, size, (page - 1) * size])

  if (currentUser) {
    await run(env, 'UPDATE messages SET read = 1, updated_at = ? WHERE to_uid = ? AND read = 0 AND relation_id = ?', [nowIso(), currentUser.uid, pid])
  }

  const alreadyPaid = currentUser
    ? await queryCount(env, 'SELECT COUNT(*) AS count FROM payments WHERE pid = ? AND uid = ?', [pid, currentUser.uid]) > 0
    : false
  const support = currentUser ? await queryCount(env, 'SELECT COUNT(*) AS count FROM post_support WHERE uid = ? AND pid = ?', [currentUser.uid, pid]) > 0 : false
  const titlesByUserId = await getUserTitlesMap(env, [Number(row.author_id ?? 0), ...commentRows.map(comment => Number(comment.author_id ?? 0))])
  const post: any = await buildPostSummary(env, row, currentUser?.id, true, titlesByUserId)

  post.content = row.content
  post.canViewHidden = currentUser?.uid === row.uid || alreadyPaid
  post.comments = await Promise.all(commentRows.map(comment => buildComment(env, comment, currentUser?.uid ?? '', row.uid, titlesByUserId)))
  post.support = support

  return json({ success: true, post })
}

async function handleCommentReaction(env: Env, currentUser: CurrentUser | null, cidParam: string | null, type: 'LIKE' | 'DISLIKE') {
  if (!currentUser) {
    return json({ success: false, message: '请先去登录' })
  }
  const cid = String(cidParam || '')
  if (!cid) {
    return json({ success: false, message: '评论不存在' })
  }

  const comment = await first(env, `
    SELECT c.*, p.title AS post_title, u.username AS author_username, u.tg_chat_id AS author_tg_chat_id
    FROM comments c
    JOIN posts p ON p.pid = c.pid
    JOIN users u ON u.uid = c.uid
    WHERE c.cid = ?
  `, [cid])
  if (!comment) {
    return json({ success: false, message: '帖子不存在' })
  }
  if (comment.uid === currentUser.uid) {
    return json({ success: false, message: '不能给自己表态' })
  }

  const table = type === 'LIKE' ? 'comment_likes' : 'comment_dislikes'
  const oppositeTable = type === 'LIKE' ? 'comment_dislikes' : 'comment_likes'
  const exists = await queryCount(env, `SELECT COUNT(*) AS count FROM ${table} WHERE uid = ? AND cid = ?`, [currentUser.uid, cid])
  if (exists > 0) {
    const stats = await buildCommentReactionPayload(env, cid, currentUser.uid)
    return json({ success: true, ...stats })
  }

  const config = await getSysConfig(env)
  const amount = Number(config.pointPerLikeOrDislike || 1)
  if (currentUser.point < amount) {
    return json({ success: false, message: '积分不够' })
  }
  const nextPoint = currentUser.point - amount
  const now = nowIso()
  const toggledOff = exists > 0

  await run(env, 'INSERT INTO point_history (created_at, updated_at, reason, uid, pid, cid, point) VALUES (?, ?, ?, ?, ?, ?, ?)', [now, now, type, currentUser.uid, comment.pid, cid, -amount])
  await run(env, 'UPDATE users SET point = ?, level = ?, last_active = ?, updated_at = ? WHERE uid = ?', [nextPoint, getUserLevelByPoint(nextPoint), now, now, currentUser.uid])
  await run(env, `DELETE FROM ${oppositeTable} WHERE uid = ? AND pid = ? AND cid = ?`, [currentUser.uid, comment.pid, cid])
  if (toggledOff) {
    await run(env, `DELETE FROM ${table} WHERE uid = ? AND pid = ? AND cid = ?`, [currentUser.uid, comment.pid, cid])
  }
  else {
    await run(env, `INSERT INTO ${table} (created_at, updated_at, pid, cid, uid) VALUES (?, ?, ?, ?, ?)`, [now, now, comment.pid, cid, currentUser.uid])
  }
  await run(env, `
    INSERT INTO messages (created_at, updated_at, read, from_uid, to_uid, content, type, relation_id)
    VALUES (?, ?, 0, ?, ?, ?, ?, ?)
  `, [now, now, currentUser.uid, comment.uid, `你的<a class='text-blue-500 mx-1' href='/post/${comment.pid}#${comment.floor}'>评论</a>被<a class='text-blue-500 mx-1' href='/member/${currentUser.username}'>${currentUser.username}</a>${toggledOff ? '取消' : ''}${type === 'LIKE' ? '点赞了' : '点踩了'}`, type, comment.pid])
  await sendTgMessage(
    config,
    comment.author_tg_chat_id,
    `你在帖子《${comment.post_title}》中的评论被${type === 'LIKE' ? '点赞' : '点踩'}了${buildSiteLink(config, `/post/${comment.pid}#${cid}`) ? `\n${buildSiteLink(config, `/post/${comment.pid}#${cid}`)}` : ''}`,
  )

  const stats = await buildCommentReactionPayload(env, cid, currentUser.uid)
  return json({ success: true, ...stats })
}

async function handleCommentDetail(env: Env, currentUser: CurrentUser | null, cid: string) {
  const row = await first(env, `
    SELECT
      c.*,
      u.id AS author_id,
      u.uid AS author_uid,
      u.username AS author_username,
      u.avatar_url AS author_avatar_url,
      u.head_img AS author_head_img,
      u.role AS author_role,
      u.signature AS author_signature,
      p.uid AS post_uid,
      (SELECT COUNT(*) FROM comment_likes cl WHERE cl.cid = c.cid) AS like_count,
      (SELECT COUNT(*) FROM comment_dislikes cd WHERE cd.cid = c.cid) AS dislike_count,
      (SELECT COUNT(*) FROM comment_likes cl WHERE cl.cid = c.cid AND cl.uid = ?) AS liked_count,
      (SELECT COUNT(*) FROM comment_dislikes cd WHERE cd.cid = c.cid AND cd.uid = ?) AS disliked_count
    FROM comments c
    JOIN users u ON u.uid = c.uid
    JOIN posts p ON p.pid = c.pid
    WHERE c.cid = ?
  `, [currentUser?.uid ?? '', currentUser?.uid ?? '', cid])

  if (!row) {
    return json({ success: false, message: '评论不存在' })
  }

  return json({
    success: true,
    comment: await buildComment(env, row, currentUser?.uid ?? '', row.post_uid),
  })
}

async function handleMemberDetail(env: Env, currentUser: CurrentUser | null, username: string) {
  const row = await first(env, 'SELECT * FROM users WHERE username = ?', [username])
  if (!row) {
    return json({})
  }

  if (currentUser?.uid === row.uid) {
    await ensureUserSecretKey(env, row)
  }

  const user = await buildUserSummary(env, row, currentUser?.uid === row.uid)
  const privateMsgCount = await queryCount(env, 'SELECT COUNT(*) AS count FROM messages WHERE type = ? AND to_uid = ?', ['PRIVATE_MSG', row.uid])
  let unreadMessageCount = 0
  let unreadPrivateMessageCount = 0

  if (currentUser?.uid === row.uid) {
    unreadMessageCount = await queryCount(env, `SELECT COUNT(*) AS count FROM messages WHERE to_uid = ? AND read = 0 AND (type IS NULL OR type != 'PRIVATE_MSG')`, [row.uid])
    unreadPrivateMessageCount = await queryCount(env, `SELECT COUNT(*) AS count FROM messages WHERE to_uid = ? AND read = 0 AND type = 'PRIVATE_MSG'`, [row.uid])
  }

  return json({
    ...user,
    privateMsgCount,
    unreadMessageCount,
    unreadPrivateMessageCount,
  })
}

async function handleSendPrivateMessage(request: Request, env: Env, currentUser: CurrentUser | null) {
  if (!currentUser) {
    return json({ success: false, message: '请先去登录' })
  }

  const body = await readBody(request)
  const content = String(body.content || '').trim()
  const toUsername = String(body.toUser || '').trim()
  if (!content || !toUsername) {
    return json({ success: false, message: '内容和接收者不能为空' })
  }

  const config = await getSysConfig(env)
  if (config.turnstile?.enable) {
    const turnstile = await verifyTurnstile(config.turnstile.secretKey, body.token, 'sendMsg', request)
    if (!turnstile.success) {
      return json(turnstile)
    }
  }

  const targetUser = await first(env, 'SELECT uid, username, tg_chat_id FROM users WHERE username = ?', [toUsername])
  if (!targetUser) {
    return json({ success: false, message: '接收者不存在' })
  }
  if (currentUser.uid === targetUser.uid) {
    return json({ success: false, message: '不能给自己发送私信' })
  }

  const now = nowIso()
  await run(env, `
    INSERT INTO messages (created_at, updated_at, read, from_uid, to_uid, content, type)
    VALUES (?, ?, 0, ?, ?, ?, 'PRIVATE_MSG')
  `, [now, now, currentUser.uid, targetUser.uid, content])

  await sendTgMessage(
    config,
    targetUser.tg_chat_id,
    buildPrivateMessageTelegramText(config, currentUser.username, content),
  )

  return json({ success: true, message: '发送成功' })
}

async function handlePrivateMessageInbox(request: Request, env: Env, currentUser: CurrentUser | null) {
  if (!currentUser) {
    return json({
      success: false,
      message: '请先去登录',
      list: [],
      total: 0,
    })
  }

  const body = await readBody(request)
  const page = getPage(body.page)
  const size = getSize(body.size, 20)

  const rows = await all(env, `
    SELECT
      m.id,
      m.created_at,
      m.updated_at,
      m.read,
      m.content,
      m.type,
      fu.uid AS from_user_uid,
      fu.username AS from_user_username,
      fu.avatar_url AS from_user_avatar_url,
      fu.head_img AS from_user_head_img,
      fu.role AS from_user_role,
      tu.uid AS to_user_uid,
      tu.username AS to_user_username,
      tu.avatar_url AS to_user_avatar_url,
      tu.head_img AS to_user_head_img,
      tu.role AS to_user_role
    FROM messages m
    JOIN (
      SELECT from_uid, MAX(created_at) AS latest
      FROM messages
      WHERE type = 'PRIVATE_MSG' AND to_uid = ?
      GROUP BY from_uid
    ) latest ON latest.from_uid = m.from_uid AND latest.latest = m.created_at
    JOIN users fu ON fu.uid = m.from_uid
    JOIN users tu ON tu.uid = m.to_uid
    WHERE m.type = 'PRIVATE_MSG' AND m.to_uid = ?
    ORDER BY m.created_at DESC, m.id DESC
    LIMIT ? OFFSET ?
  `, [currentUser.uid, currentUser.uid, size, (page - 1) * size])

  const total = await queryCount(env, `
    SELECT COUNT(*) AS count
    FROM (
      SELECT from_uid
      FROM messages
      WHERE type = 'PRIVATE_MSG' AND to_uid = ?
      GROUP BY from_uid
    ) grouped_messages
  `, [currentUser.uid])

  return json({
    success: true,
    list: rows.map(row => mapMessageRow(row)),
    total,
  })
}

async function handlePrivateMessageList(request: Request, env: Env, currentUser: CurrentUser | null) {
  if (!currentUser) {
    return json({
      success: false,
      message: '请先去登录',
      list: [],
    })
  }

  const body = await readBody(request)
  const fromUsername = String(body.fromUsername || '').trim()
  if (!fromUsername) {
    return json({ success: false, message: '用户不存在', list: [] })
  }

  const fromUser = await first(env, 'SELECT uid FROM users WHERE username = ?', [fromUsername])
  if (!fromUser) {
    return json({ success: false, message: '用户不存在', list: [] })
  }

  const rows = await all(env, `
    SELECT *
    FROM (
      SELECT
        m.id,
        m.created_at,
        m.updated_at,
        m.read,
        m.content,
        m.type,
        fu.uid AS from_user_uid,
        fu.username AS from_user_username,
        fu.avatar_url AS from_user_avatar_url,
        fu.head_img AS from_user_head_img,
        fu.role AS from_user_role,
        tu.uid AS to_user_uid,
        tu.username AS to_user_username,
        tu.avatar_url AS to_user_avatar_url,
        tu.head_img AS to_user_head_img,
        tu.role AS to_user_role
      FROM messages m
      LEFT JOIN users fu ON fu.uid = m.from_uid
      JOIN users tu ON tu.uid = m.to_uid
      WHERE m.type = 'PRIVATE_MSG'
        AND (
          (m.from_uid = ? AND m.to_uid = ?)
          OR (m.from_uid = ? AND m.to_uid = ?)
        )
      ORDER BY m.created_at DESC, m.id DESC
      LIMIT 50
    ) recent_messages
    ORDER BY created_at ASC, id ASC
  `, [currentUser.uid, fromUser.uid, fromUser.uid, currentUser.uid])

  await run(env, `
    UPDATE messages
    SET read = 1, updated_at = ?
    WHERE from_uid = ?
      AND to_uid = ?
      AND read = 0
      AND type = 'PRIVATE_MSG'
  `, [nowIso(), fromUser.uid, currentUser.uid])

  return json({
    success: true,
    message: '',
    list: rows.map(row => mapMessageRow(row)),
  })
}

async function handleMemberMessages(request: Request, env: Env, currentUser: CurrentUser | null) {
  if (!currentUser) {
    return json({
      success: false,
      message: '请先去登录',
      messages: [],
      total: 0,
    })
  }

  const body = await readBody(request)
  const page = getPage(body.page)
  const size = getSize(body.size, 50)

  const rows = await all(env, `
    SELECT
      m.id,
      m.created_at,
      m.updated_at,
      m.read,
      m.content,
      m.type,
      fu.uid AS from_user_uid,
      fu.username AS from_user_username,
      fu.avatar_url AS from_user_avatar_url,
      fu.head_img AS from_user_head_img,
      fu.role AS from_user_role,
      tu.uid AS to_user_uid,
      tu.username AS to_user_username,
      tu.avatar_url AS to_user_avatar_url,
      tu.head_img AS to_user_head_img,
      tu.role AS to_user_role
    FROM messages m
    LEFT JOIN users fu ON fu.uid = m.from_uid
    JOIN users tu ON tu.uid = m.to_uid
    WHERE m.to_uid = ?
      AND (m.type IS NULL OR m.type != 'PRIVATE_MSG')
    ORDER BY m.created_at DESC, m.id DESC
    LIMIT ? OFFSET ?
  `, [currentUser.uid, size, (page - 1) * size])

  const total = await queryCount(env, `
    SELECT COUNT(*) AS count
    FROM messages
    WHERE to_uid = ?
      AND (type IS NULL OR type != 'PRIVATE_MSG')
  `, [currentUser.uid])

  return json({
    success: true,
    messages: rows.map(row => mapMessageRow(row)),
    total,
  })
}

async function handleReadMessages(url: URL, env: Env, currentUser: CurrentUser | null) {
  if (!currentUser) {
    return json({ success: false, message: '请先登录' })
  }

  const messageId = Number.parseInt(url.searchParams.get('messageId') || '') || 0
  const now = nowIso()
  if (messageId > 0) {
    await run(env, `
      UPDATE messages
      SET read = 1, updated_at = ?
      WHERE id = ?
        AND to_uid = ?
        AND (type IS NULL OR type != 'PRIVATE_MSG')
    `, [now, messageId, currentUser.uid])
  }
  else {
    await run(env, `
      UPDATE messages
      SET read = 1, updated_at = ?
      WHERE to_uid = ?
        AND (type IS NULL OR type != 'PRIVATE_MSG')
    `, [now, currentUser.uid])
  }

  return json({
    success: true,
    message: '操作成功',
  })
}

async function handleTelegramWebhook(request: Request, env: Env) {
  const config = await getSysConfig(env)
  const secretToken = String(config.notify?.tgSecret || '').trim()
  const requestToken = String(request.headers.get('X-Telegram-Bot-Api-Secret-Token') || '').trim()
  if (!secretToken || requestToken !== secretToken) {
    return new Response('Unauthorized', { status: 401 })
  }

  if (!config.notify?.tgBotEnabled || !config.notify?.tgBotToken) {
    return new Response('Telegram bot is not enabled')
  }

  const body = await readBody(request)
  const { text, chatId } = getTelegramUpdateMessage(body)

  if (!text || !chatId) {
    return new Response('OK')
  }

  const binding = parseTelegramBindingCommand(text)
  if (!binding) {
    await sendTgMessage(config, chatId, '格式不正确，请发送 /bind 用户名#密钥')
    return new Response('OK')
  }

  const user = await first(env, 'SELECT uid FROM users WHERE username = ? AND secret_key = ?', [binding.username, binding.secretKey])
  if (!user) {
    await sendTgMessage(config, chatId, `不存在 ${binding.username} 这个用户，或密钥不正确`)
    return new Response('OK')
  }

  await run(env, 'UPDATE users SET tg_chat_id = ?, updated_at = ? WHERE uid = ?', [chatId, nowIso(), user.uid])
  await sendTgMessage(config, chatId, '绑定成功，后续站内消息和私信会通过 Telegram 通知你。')

  return new Response('OK')
}

function getTelegramUpdateMessage(body: any) {
  const message = body?.message ?? body?.edited_message ?? body?.channel_post ?? body?.edited_channel_post
  return {
    text: String(message?.text || '').trim(),
    chatId: message?.chat?.id ? String(message.chat.id) : '',
  }
}

function parseTelegramBindingCommand(text: string) {
  const normalized = text.replace(/\uFF03/g, '#').trim()
  const commandStripped = normalized.replace(/^\/(?:start|bind)(?:@\w+)?(?:\s+|$)/i, '').trim()
  const candidate = commandStripped || normalized
  const match = candidate.match(/^([^#\s]+)\s*#\s*(\S+)$/)
  if (!match) {
    return null
  }
  return {
    username: match[1].trim(),
    secretKey: match[2].trim(),
  }
}

async function buildProfile(env: Env, currentUser: CurrentUser) {
  const receiveCount = await queryCount(env, `SELECT COUNT(*) AS count FROM messages WHERE to_uid = ? AND (type IS NULL OR type != 'PRIVATE_MSG')`, [currentUser.uid])
  const unreadCount = await queryCount(env, `SELECT COUNT(*) AS count FROM messages WHERE to_uid = ? AND read = 0`, [currentUser.uid])
  const favCount = await queryCount(env, 'SELECT COUNT(*) AS count FROM favorites WHERE user_id = ?', [currentUser.id])
  const titles = await getUserTitles(env, currentUser.id)

  return {
    ...sanitizeUser(currentUser, true),
    titles,
    _count: {
      fav: favCount,
      comments: currentUser.commentCount,
      posts: currentUser.postCount,
      ReceiveMessage: receiveCount,
    },
    unRead: unreadCount,
  }
}

async function buildUserSummary(env: Env, row: any, includePrivateFields = false) {
  const user = mapCurrentUser(row)
  const receiveCount = await queryCount(env, `SELECT COUNT(*) AS count FROM messages WHERE to_uid = ? AND (type IS NULL OR type != 'PRIVATE_MSG')`, [row.uid])
  const favCount = await queryCount(env, 'SELECT COUNT(*) AS count FROM favorites WHERE user_id = ?', [row.id])
  const titles = await getUserTitles(env, row.id)

  return {
    ...sanitizeUser(user, includePrivateFields),
    titles,
    _count: {
      fav: favCount,
      comments: user.commentCount,
      posts: user.postCount,
      ReceiveMessage: receiveCount,
    },
  }
}

function mapMessageUser(row: any, prefix: string) {
  const uid = row[`${prefix}_uid`]
  if (!uid) {
    return null
  }

  return {
    uid,
    username: row[`${prefix}_username`],
    avatarUrl: row[`${prefix}_avatar_url`] ?? null,
    headImg: row[`${prefix}_head_img`] ?? null,
    role: row[`${prefix}_role`] || 'USER',
  }
}

function mapMessageRow(row: any) {
  return {
    id: Number(row.id),
    from: mapMessageUser(row, 'from_user'),
    to: mapMessageUser(row, 'to_user'),
    content: row.content,
    read: Number(row.read) === 1,
    createdAt: row.created_at,
    type: row.type || '',
  }
}

async function buildPostSummaries(env: Env, rows: any[], currentUserId?: number, includeContent = false) {
  const titlesByUserId = await getUserTitlesMap(env, rows.map(row => Number(row.author_id ?? 0)))
  return Promise.all(rows.map(row => buildPostSummary(env, row, currentUserId, includeContent, titlesByUserId)))
}

async function buildCommentsWithPosts(env: Env, rows: any[]) {
  const titlesByUserId = await getUserTitlesMap(env, rows.map(row => Number(row.author_id ?? 0)))
  return Promise.all(rows.map(row => buildCommentWithPost(env, row, titlesByUserId)))
}

async function buildPostSummary(env: Env, row: any, currentUserId?: number, includeContent = false, titlesByUserId?: Map<number, UserTitleSummary[]>) {
  const titles = titlesByUserId?.get(Number(row.author_id ?? 0)) ?? await getUserTitles(env, row.author_id)
  return {
    title: row.title,
    content: includeContent ? row.content : undefined,
    pid: row.pid,
    uid: row.uid,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    viewCount: Number(row.view_count ?? 0),
    replyCount: Number(row.reply_count ?? 0),
    likeCount: Number(row.like_count ?? 0),
    disLikeCount: Number(row.dis_like_count ?? 0),
    minLevel: Number(row.min_level ?? 1),
    author: {
      uid: row.author_uid,
      avatarUrl: row.author_avatar_url,
      headImg: row.author_head_img,
      username: row.author_username,
      role: row.author_role,
      titles,
      signature: row.author_signature,
    },
    tagId: Number(row.tag_id),
    readRole: Number(row.read_role ?? 0),
    tag: {
      id: Number(row.tag_id),
      name: row.tag_name,
      enName: row.tag_en_name,
      desc: row.tag_desc,
      count: Number(row.tag_count ?? 0),
      hot: Number(row.tag_hot) === 1,
    },
    pinned: Number(row.pinned) === 1,
    lastCommentTime: row.last_comment_time,
    lastCommentUid: row.last_comment_uid,
    lastCommentUser: row.last_comment_user_uid
      ? {
          uid: row.last_comment_user_uid,
          username: row.last_comment_user_username,
        }
      : null,
    point: Number(row.point ?? 0),
    hide: Number(row.hide) === 1,
    payPoint: Number(row.pay_point ?? 0),
    fav: currentUserId ? Number(row.fav_count ?? 0) > 0 : false,
    _count: {
      comments: Number(row.comments_count ?? 0),
      commentLike: 0,
      commentDisLike: 0,
      PostSupport: Number(row.support_count ?? 0),
    },
  }
}

async function buildComment(env: Env, row: any, currentUserUid: string, postUid: string, titlesByUserId?: Map<number, UserTitleSummary[]>) {
  return {
    content: row.content,
    cid: row.cid,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    mentioned: parseJsonArray(row.mentioned),
    author: {
      uid: row.author_uid,
      username: row.author_username,
      avatarUrl: row.author_avatar_url,
      headImg: row.author_head_img,
      role: row.author_role,
      signature: row.author_signature,
      titles: titlesByUserId?.get(Number(row.author_id ?? 0)) ?? await getUserTitles(env, row.author_id),
    },
    likeCount: Number(row.like_count ?? 0),
    dislikeCount: Number(row.dislike_count ?? 0),
    like: currentUserUid ? Number(row.liked_count ?? 0) > 0 : false,
    dislike: currentUserUid ? Number(row.disliked_count ?? 0) > 0 : false,
    post: {
      pid: row.pid,
      uid: postUid,
    },
    floor: Number(row.floor ?? 1),
  }
}

async function buildCommentWithPost(env: Env, row: any, titlesByUserId?: Map<number, UserTitleSummary[]>) {
  return {
    content: row.content,
    cid: row.cid,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    mentioned: parseJsonArray(row.mentioned),
    floor: Number(row.floor ?? 1),
    author: {
      uid: row.author_uid,
      username: row.author_username,
      avatarUrl: row.author_avatar_url,
      headImg: row.author_head_img,
      role: row.author_role,
      signature: row.author_signature,
      titles: titlesByUserId?.get(Number(row.author_id ?? 0)) ?? await getUserTitles(env, row.author_id ?? 0),
    },
    post: {
      pid: row.post_pid,
      title: row.post_title,
      createdAt: row.post_created_at,
    },
  }
}

async function buildManageComment(env: Env, row: any) {
  return {
    cid: row.cid,
    pid: row.pid,
    content: row.content,
    createdAt: row.created_at,
    author: {
      uid: row.author_uid,
      username: row.author_username,
      avatarUrl: row.author_avatar_url,
      headImg: row.author_head_img,
    },
    post: {
      title: row.post_title,
    },
  }
}

async function buildCommentReactionPayload(env: Env, cid: string, currentUserUid: string) {
  const row = await first(env, `
    SELECT
      (SELECT COUNT(*) FROM comment_likes WHERE cid = ?) AS like_count,
      (SELECT COUNT(*) FROM comment_dislikes WHERE cid = ?) AS dislike_count,
      (SELECT COUNT(*) FROM comment_likes WHERE cid = ? AND uid = ?) AS liked_count,
      (SELECT COUNT(*) FROM comment_dislikes WHERE cid = ? AND uid = ?) AS disliked_count
  `, [cid, cid, cid, currentUserUid, cid, currentUserUid])

  return {
    like: Number(row?.liked_count ?? 0) > 0,
    dislike: Number(row?.disliked_count ?? 0) > 0,
    likeCount: Number(row?.like_count ?? 0),
    dislikeCount: Number(row?.dislike_count ?? 0),
  }
}

async function getCurrentUser(request: Request, env: Env) {
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

async function getSysConfig(env: Env) {
  const row = await first(env, 'SELECT content FROM sys_config WHERE id = 1', [])
  if (!row?.content) {
    await saveSysConfig(env, defaultSysConfig)
    return clone(defaultSysConfig)
  }
  return deepMerge(clone(defaultSysConfig), normalizeSysConfig(parseJsonObject(row.content)))
}

function getPublicSysConfig(config: any) {
  const publicConfig = clone(config)

  if (publicConfig.turnstile && typeof publicConfig.turnstile === 'object') {
    publicConfig.turnstile = {
      ...publicConfig.turnstile,
      secretKey: '',
    }
  }

  publicConfig.email = {
    apiKey: '',
    from: '',
    senderName: '',
    to: '',
  }

  if (publicConfig.notify && typeof publicConfig.notify === 'object') {
    publicConfig.notify = {
      ...publicConfig.notify,
      tgBotToken: '',
      tgSecret: '',
    }
  }

  return publicConfig
}

async function saveSysConfig(env: Env, config: any) {
  const merged = deepMerge(clone(defaultSysConfig), normalizeSysConfig(config))
  const exists = await first(env, 'SELECT id FROM sys_config WHERE id = 1', [])
  if (exists) {
    await run(env, 'UPDATE sys_config SET content = ? WHERE id = 1', [JSON.stringify(merged)])
  }
  else {
    await run(env, 'INSERT INTO sys_config (id, content) VALUES (1, ?)', [JSON.stringify(merged)])
  }
}

async function ensureUserSecretKey(env: Env, row: any) {
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

async function getUserTitlesMap(env: Env, userIds: number[]) {
  const ids = [...new Set(userIds.filter(userId => Number.isFinite(userId) && userId > 0).map(userId => Math.floor(userId)))]
  const titlesByUserId = new Map<number, UserTitleSummary[]>()

  for (const userId of ids) {
    titlesByUserId.set(userId, [])
  }

  if (ids.length === 0) {
    return titlesByUserId
  }

  const placeholders = ids.map(() => '?').join(', ')
  const rows = await all(env, `
    SELECT ut.user_id, t.id, t.title, t.style, t.status
    FROM user_titles ut
    JOIN titles t ON t.id = ut.title_id
    WHERE ut.user_id IN (${placeholders})
    ORDER BY ut.user_id ASC, t.id ASC
  `, ids)

  for (const row of rows) {
    const userId = Number(row.user_id ?? 0)
    const titles = titlesByUserId.get(userId)
    if (!titles) {
      continue
    }

    titles.push({
      id: Number(row.id),
      title: row.title,
      style: row.style,
      status: Number(row.status) === 1,
    })
  }

  return titlesByUserId
}

async function getUserTitles(env: Env, userId: number) {
  const titlesByUserId = await getUserTitlesMap(env, [userId])
  return titlesByUserId.get(Number(userId)) || []
}

async function getUsernameByUid(env: Env, uid: string) {
  const row = await first(env, 'SELECT username FROM users WHERE uid = ?', [uid])
  return row?.username || ''
}

async function syncPostPoint(env: Env, pid: string) {
  const row = await first(env, `
    SELECT p.pid, p.created_at, p.uid, u.point AS author_point
    FROM posts p
    JOIN users u ON u.uid = p.uid
    WHERE p.pid = ?
  `, [pid])
  if (!row) {
    return
  }

  const supportCount = await queryCount(env, 'SELECT COUNT(*) AS count FROM post_support WHERE pid = ?', [pid])
  const commentCount = await queryCount(env, 'SELECT COUNT(*) AS count FROM comments WHERE pid = ? AND uid != ?', [pid, row.uid])
  const point = calculateHotPoint(Number(row.author_point ?? 0), supportCount, commentCount, row.created_at)
  await run(env, 'UPDATE posts SET point = ?, updated_at = ? WHERE pid = ?', [point, nowIso(), pid])
}

function calculateHotPoint(authorPoint: number, supportCount: number, commentCount: number, createdAt: string) {
  const second = Math.max(1, Math.floor((Date.now() - new Date(createdAt).getTime()) / 1000))
  return ((authorPoint * 2 + supportCount * 2 + commentCount - 1) / (second + 600) ** 1.1) * 10000000
}

function getUserLevelByPoint(point: number) {
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

function mapCurrentUser(row: any): CurrentUser {
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

function sanitizeUser(user: CurrentUser, includePrivateFields = false) {
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

function mapTag(row: any) {
  return {
    id: Number(row.id),
    name: row.name,
    enName: row.en_name,
    desc: row.desc,
    count: Number(row.count ?? 0),
    hot: Number(row.hot) === 1,
  }
}

function postListSql(whereClause: string, orderBy: string, tail = '', includeFav = false) {
  return `
    SELECT
      p.*,
      au.id AS author_id,
      au.uid AS author_uid,
      au.username AS author_username,
      au.avatar_url AS author_avatar_url,
      au.head_img AS author_head_img,
      au.role AS author_role,
      au.signature AS author_signature,
      t.name AS tag_name,
      t.en_name AS tag_en_name,
      t."desc" AS tag_desc,
      t.count AS tag_count,
      t.hot AS tag_hot,
      lu.uid AS last_comment_user_uid,
      lu.username AS last_comment_user_username,
      (SELECT COUNT(*) FROM comments c WHERE c.pid = p.pid) AS comments_count,
      (SELECT COUNT(*) FROM post_support ps WHERE ps.pid = p.pid) AS support_count,
      ${includeFav ? '(SELECT COUNT(*) FROM favorites f WHERE f.pid = p.pid AND f.user_id = ?) AS fav_count' : '0 AS fav_count'}
    FROM posts p
    JOIN users au ON au.uid = p.uid
    JOIN tags t ON t.id = p.tag_id
    LEFT JOIN users lu ON lu.uid = p.last_comment_uid
    ${whereClause}
    ORDER BY ${orderBy}
    ${tail}
  `
}

async function verifyTurnstile(secretKey: string, token?: string, expectedAction?: string, request?: Request) {
  if (!token) {
    return {
      success: false,
      message: '请先通过人机验证',
    }
  }

  const formData = new URLSearchParams({
    secret: secretKey,
    response: token,
  })
  const remoteIp = request?.headers.get('CF-Connecting-IP') || request?.headers.get('X-Forwarded-For')?.split(',')[0]?.trim()
  if (remoteIp) {
    formData.set('remoteip', remoteIp)
  }

  const response = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded',
    },
    body: formData.toString(),
  })
  const result = await response.json() as any
  if (!result.success) {
    return {
      success: false,
      message: '请先通过人机验证',
    }
  }
  if (expectedAction && result.action !== expectedAction) {
    return {
      success: false,
      message: '请先通过人机验证',
    }
  }
  return { success: true, message: '验证通过' }
}

function normalizeEmail(value: string) {
  return value.trim().toLowerCase()
}

function isSameEmail(left: string, right: string) {
  return normalizeEmail(left) === normalizeEmail(right)
}

function isValidEmail(value: string) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)
}

function normalizeEmailConfig(value: any): EmailConfig {
  const email = value && typeof value === 'object' ? value : {}
  const password = String(email.password || '').trim()

  return {
    apiKey: String(email.apiKey || (password.startsWith('re_') ? password : '')).trim(),
    from: normalizeEmail(String(email.from || email.username || '')),
    senderName: String(email.senderName || '').trim(),
    to: normalizeEmail(String(email.to || '')),
  }
}

function validateEmailConfig(config: EmailConfig) {
  if (!config.apiKey) {
    return '请先配置 Resend API Key'
  }
  if (!isValidEmail(config.from)) {
    return '请先配置发件邮箱'
  }
  return ''
}

async function isEmailSendRateLimited(env: Env, targetEmail: string, reason: string) {
  const since = new Date(Date.now() - 5 * 60 * 1000).toISOString()
  const count = await queryCount(env, `
    SELECT COUNT(*) AS count
    FROM email_codes
    WHERE target_email = ?
      AND reason = ?
      AND created_at >= ?
  `, [normalizeEmail(targetEmail), reason, since])

  return count >= 3
}

async function saveEmailCodeRecord(env: Env, input: {
  key: string
  code: string
  reason: string
  targetEmail: string
  validMinutes: number
}) {
  const now = nowIso()
  const targetEmail = normalizeEmail(input.targetEmail)
  await run(env, `
    UPDATE email_codes
    SET used = 1, updated_at = ?
    WHERE target_email = ?
      AND reason = ?
      AND used = 0
  `, [now, targetEmail, input.reason])

  await run(env, `
    INSERT INTO email_codes (created_at, updated_at, key, code, valid_at, used, reason, target_email)
    VALUES (?, ?, ?, ?, ?, 0, ?, ?)
  `, [now, now, input.key, input.code, new Date(Date.now() + input.validMinutes * 60 * 1000).toISOString(), input.reason, targetEmail])
}

async function sendResendEmail(configInput: EmailConfig, to: string, subject: string, html: string, idempotencyKey?: string) {
  const config = normalizeEmailConfig(configInput)
  const configError = validateEmailConfig(config)
  if (configError) {
    return { success: false, message: configError }
  }

  try {
    const headers = new Headers({
      Authorization: `Bearer ${config.apiKey}`,
      'Content-Type': 'application/json',
    })
    if (idempotencyKey) {
      headers.set('Idempotency-Key', idempotencyKey)
    }

    const response = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers,
      body: JSON.stringify({
        from: formatEmailAddress(config.from, config.senderName),
        to: [normalizeEmail(to)],
        subject,
        html,
      }),
    })

    const result = await response.json().catch(() => null) as any
    if (!response.ok) {
      return {
        success: false,
        message: `发送邮件失败:${result?.message || result?.error?.message || response.statusText || 'Resend 请求失败'}`,
      }
    }

    return { success: true, message: '发送成功', id: String(result?.id || '') }
  }
  catch (error) {
    return {
      success: false,
      message: `发送邮件失败:${error instanceof Error ? error.message : 'Resend 请求失败'}`,
    }
  }
}

function formatEmailAddress(email: string, senderName: string) {
  const safeName = senderName.replace(/"/g, '').trim()
  return safeName ? `${safeName} <${email}>` : email
}

function normalizeSiteUrl(config: any) {
  return String(config?.websiteUrl || '').trim().replace(/\/+$/, '')
}

function buildSiteLink(config: any, path: string) {
  const siteUrl = normalizeSiteUrl(config)
  if (!siteUrl) {
    return ''
  }
  return `${siteUrl}${path.startsWith('/') ? path : `/${path}`}`
}

function buildPrivateMessageTelegramText(config: any, fromUsername: string, content: string) {
  const profileUrl = buildSiteLink(config, `/member/${encodeURIComponent(fromUsername)}`)
  const lines = [
    `你收到了来自 ${fromUsername} 的一条私信`,
    profileUrl ? `发送者主页：${profileUrl}` : '',
    '',
    truncateText(content, 1000),
  ]
  return lines.filter(Boolean).join('\n')
}

function truncateText(value: string, maxLength: number) {
  if (value.length <= maxLength) {
    return value
  }
  return `${value.slice(0, maxLength)}...`
}

async function sendTgMessage(config: any, chatId: string | null | undefined, message: string) {
  if (!chatId) {
    return
  }
  if (!config?.notify?.tgBotEnabled || !config.notify?.tgBotToken) {
    return
  }

  try {
    await fetch(`https://api.telegram.org/bot${config.notify.tgBotToken}/sendMessage`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        chat_id: chatId,
        text: message,
        disable_web_page_preview: true,
      }),
    })
  }
  catch (error) {
    console.log('send tg message failed', error)
  }
}

function buildRegisterEmailHtml(config: any, code: string) {
  const websiteName = escapeHtml(String(config.websiteName || '极简论坛'))
  const websiteUrl = String(config.websiteUrl || '').trim()
  const websiteLink = websiteUrl
    ? `<a href="${escapeHtml(websiteUrl)}">${websiteName}</a>`
    : websiteName

  return `<p>欢迎使用 ${websiteLink}</p><p>您的注册验证码是：<b>${escapeHtml(code)}</b></p><p>验证码 5 分钟内有效。</p>`
}

function buildResetPasswordEmailHtml(config: any, code: string) {
  const websiteName = escapeHtml(String(config.websiteName || '极简论坛'))
  return `<p>${websiteName} 正在为您重置密码。</p><p>验证码是：<b>${escapeHtml(code)}</b></p><p>验证码 30 分钟内有效。</p>`
}

function randomNumericCode(length: number) {
  let result = ''
  for (let i = 0; i < length; i++) {
    result += `${Math.floor(Math.random() * 10)}`
  }
  return result
}

function escapeHtml(value: string) {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll('\'', '&#39;')
}

async function queryPointSum(env: Env, uid: string, reason: string) {
  const row = await first(env, `
    SELECT COALESCE(SUM(point), 0) AS point
    FROM point_history
    WHERE uid = ?
      AND reason = ?
      AND created_at >= ?
      AND created_at <= ?
  `, [uid, reason, startOfDayIso(), endOfDayIso()])

  return Number(row?.point ?? 0)
}

function extractMentions(text: string) {
  const regex = /\[@([^\]]+)\]/g
  return (text.match(regex) || []).map(match => match.slice(1, -1))
}

function normalizeNullableString(value: any) {
  const text = String(value ?? '').trim()
  return text ? text : null
}

function shouldResolveCurrentUser(pathname: string, method: string) {
  if (method === 'GET' && PUBLIC_GET_API_PATHS.has(pathname)) {
    return false
  }
  return !PUBLIC_API_PATHS.has(pathname)
}

function isAdmin(user: CurrentUser | null) {
  return user?.role === 'ADMIN'
}

async function readBody(request: Request) {
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

function json(data: any, headers?: Headers) {
  const responseHeaders = headers || new Headers()
  responseHeaders.set('content-type', 'application/json; charset=utf-8')
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: responseHeaders,
  })
}

async function respondWithEdgeCache(
  request: Request,
  ctx: ExecutionContextLike,
  ttlSeconds: number,
  buildResponse: () => Promise<Response>,
) {
  const cacheKey = new Request(request.url, { method: 'GET' })
  const cache = caches.default
  const cached = await cache.match(cacheKey)

  if (cached) {
    const response = toMutableResponse(cached)
    response.headers.set('x-edge-cache', 'HIT')
    return response
  }

  const response = await buildResponse()
  if (response.status !== 200) {
    return response
  }

  response.headers.set('Cache-Control', `public, max-age=${ttlSeconds}`)
  response.headers.set('x-edge-cache', 'MISS')
  ctx.waitUntil(cache.put(cacheKey, response.clone()))
  return response
}

function toMutableResponse(response: Response) {
  return new Response(response.body, response)
}

async function all(env: Env, sql: string, params: any[]) {
  const result = await env.DB.prepare(sql).bind(...params).all<any>()
  return result.results || []
}

async function first(env: Env, sql: string, params: any[]) {
  return env.DB.prepare(sql).bind(...params).first<any>()
}

async function run(env: Env, sql: string, params: any[]) {
  return env.DB.prepare(sql).bind(...params).run()
}

async function queryCount(env: Env, sql: string, params: any[]) {
  const row = await first(env, sql, params)
  return Number(row?.count ?? 0)
}

function getTokenKey(env: Env) {
  return env.TOKEN_KEY || 'discussion_token'
}

function shouldUseSecureCookie(env: Env) {
  return env.COOKIE_SECURE === 'true'
}

function buildCookie(name: string, value: string, maxAgeMs: number, env: Env) {
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

function expireCookie(name: string, env: Env) {
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

function getCookie(request: Request, name: string) {
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

async function createToken(payload: TokenPayload, env: Env) {
  const secret = env.JWT_SECRET_KEY || 'replace-this-secret'
  const header = { alg: 'HS256', typ: 'JWT' }
  const encodedHeader = base64urlEncode(JSON.stringify(header))
  const encodedPayload = base64urlEncode(JSON.stringify(payload))
  const signingInput = `${encodedHeader}.${encodedPayload}`
  const signature = await signHmac(signingInput, secret)
  return `${signingInput}.${signature}`
}

async function verifyToken(token: string, env: Env) {
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

async function signHmac(value: string, secret: string) {
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

async function hashPassword(password: string) {
  const salt = crypto.getRandomValues(new Uint8Array(16))
  const derived = await derivePasswordBits(password, salt, 100_000)
  return `pbkdf2$100000$${base64urlFromBytes(salt)}$${base64urlFromBytes(derived)}`
}

async function verifyPassword(password: string, stored: string) {
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

async function sha256Hex(value: string) {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value.trim().toLowerCase()))
  return Array.from(new Uint8Array(digest)).map(byte => byte.toString(16).padStart(2, '0')).join('')
}

function base64urlEncode(value: string) {
  return base64urlFromBytes(new TextEncoder().encode(value))
}

function base64urlDecode(value: string) {
  const bytes = base64urlToBytes(value)
  return new TextDecoder().decode(bytes)
}

function base64urlFromBytes(bytes: Uint8Array) {
  let binary = ''
  for (const byte of bytes) {
    binary += String.fromCharCode(byte)
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '')
}

function base64urlToBytes(value: string) {
  const padded = value.replace(/-/g, '+').replace(/_/g, '/').padEnd(Math.ceil(value.length / 4) * 4, '=')
  const binary = atob(padded)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i)
  }
  return bytes
}

function timingSafeEqual(left: Uint8Array, right: Uint8Array) {
  if (left.length !== right.length) {
    return false
  }
  let result = 0
  for (let i = 0; i < left.length; i++) {
    result |= left[i] ^ right[i]
  }
  return result === 0
}

function randomId(prefix: string) {
  return `${prefix}${crypto.randomUUID().replace(/-/g, '').slice(0, 22)}`
}

function nowIso() {
  return new Date().toISOString()
}

function startOfDayIso() {
  const date = new Date()
  date.setHours(0, 0, 0, 0)
  return date.toISOString()
}

function endOfDayIso() {
  const date = new Date()
  date.setHours(23, 59, 59, 999)
  return date.toISOString()
}

function getPage(value: any) {
  const page = Number(value || 1)
  return Number.isFinite(page) && page > 0 ? Math.floor(page) : 1
}

function getSize(value: any, fallback: number) {
  const size = Number(value || fallback)
  if (!Number.isFinite(size) || size <= 0) {
    return fallback
  }
  return Math.min(100, Math.floor(size))
}

function parseJsonObject(value: string) {
  try {
    return JSON.parse(value)
  }
  catch {
    return {}
  }
}

function parseJsonArray(value: string) {
  try {
    const result = JSON.parse(value || '[]')
    return Array.isArray(result) ? result : []
  }
  catch {
    return []
  }
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value))
}

function normalizeSysConfig(config: any) {
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

function buildImageObjectKey(filename: string, mimeType: string) {
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

function deepMerge(target: any, source: any): any {
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

function formatDatePid(pattern: string) {
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

function getRandomIntWeighted(min: number, max: number) {
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
