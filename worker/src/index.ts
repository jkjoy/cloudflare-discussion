// @ts-expect-error - 构建产物，TS 无法提前识别
import nuxtHandler from '../../.output/server/index.mjs'

import type { CurrentUser, Env, ExecutionContextLike } from './types'
import { json, readBody, nowIso, getPage, getSize, randomId, sha256Hex, getUserLevelByPoint, normalizeEmail, DAY_MS } from './utils'
import { hashPassword } from './auth'
import { queryPointSum } from './post'
import { all, first, run, queryCount } from './db'
import { getCurrentUser, isAdmin, buildCookie, expireCookie, getTokenKey, mapCurrentUser, sanitizeUser, ensureUserSecretKey } from './auth'
import { defaultSysConfig, getSysConfig, getPublicSysConfig, saveSysConfig } from './config'
import { buildTagListResponse, buildMemberHotResponse, mapTag } from './tag'
import { buildPostListResponse, handlePostDetail, buildPostSummaries, syncPostPoint, getPostListInputFromUrl } from './post'
import { handleCommentReaction, handleCommentDetail, buildCommentWithPost, buildCommentsWithPosts } from './comment'
import { handleMemberDetail, buildProfile, buildUserSummary, getUserTitles, getUserTitlesMap, getUsernameByUid } from './member'
import { handleSendPrivateMessage, handlePrivateMessageList, handlePrivateMessageInbox, handleMemberMessages, handleReadMessages } from './message'
import { handleImageAsset, handleImageUpload } from './image'
import { handleTelegramWebhook } from './telegram'
import { verifyTurnstile } from './turnstile'
import { isEmailSendRateLimited, saveEmailCodeRecord, sendResendEmail, buildRegisterEmailHtml, buildResetPasswordEmailHtml } from './email'

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

    return nuxtHandler.fetch(request, env, ctx)
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
      return buildTagListResponse(env, url)
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
    return handleLogin(request, env)
  }

  if (pathname === '/api/member/reg' && method === 'POST') {
    return handleRegister(request, env)
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

    return handleSignIn(env, currentUser)
  }

  if (pathname === '/api/member/saveSettings' && method === 'POST') {
    if (!currentUser) {
      return json({ success: false, message: '请先去登录' })
    }

    return handleSaveSettings(request, env, currentUser)
  }

  if (pathname === '/api/member/createInviteCode' && method === 'POST') {
    if (!currentUser) {
      return json({ success: false, message: '请先去登录' })
    }

    return handleCreateInviteCode(env, currentUser)
  }

  if (pathname === '/api/member/inviteCodeList' && method === 'POST') {
    if (!currentUser) {
      return json({ success: false, message: '请先去登录' })
    }

    return handleInviteCodeList(env, currentUser)
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
    return handlePostNew(request, env, currentUser)
  }

  if (pathname === '/api/post/support' && method === 'POST') {
    return handlePostSupport(env, currentUser, url)
  }

  if (pathname === '/api/post/fav' && method === 'POST') {
    return handlePostFav(env, currentUser, url)
  }

  if (pathname === '/api/post/pay' && method === 'POST') {
    return handlePostPay(request, env, currentUser)
  }

  const postMatch = pathname.match(/^\/api\/post\/([^/]+)$/)
  if (postMatch && method === 'POST') {
    return handlePostDetail(env, currentUser, decodeURIComponent(postMatch[1]), request)
  }

  if (pathname === '/api/comment/new' && method === 'POST') {
    return handleCommentNew(request, env, currentUser)
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
    return handleMemberPost(request, env, currentUser)
  }

  if (pathname === '/api/member/comment' && method === 'POST') {
    return handleMemberComment(request, env, currentUser)
  }

  if (pathname === '/api/member/fav' && method === 'POST') {
    return handleMemberFav(request, env, currentUser)
  }

  if (pathname === '/api/member/point' && method === 'POST') {
    return handleMemberPoint(request, env, currentUser)
  }

  if (pathname === '/api/member/sendEmail' && method === 'POST') {
    return handleSendEmail(request, env)
  }

  if (pathname === '/api/member/sendForgotPasswordEmail' && method === 'POST') {
    return handleSendForgotPasswordEmail(request, env)
  }

  if (pathname === '/api/member/resetPwd' && method === 'POST') {
    return handleResetPwd(request, env)
  }

  const memberMatch = pathname.match(/^\/api\/member\/([^/]+)$/)
  if (memberMatch && method === 'POST') {
    const username = decodeURIComponent(memberMatch[1])
    if (!['privateMsg', 'privateMsgList', 'sendMsg', 'message', 'readMessage'].includes(username)) {
      return handleMemberDetail(env, currentUser, username)
    }
  }

  if (pathname === '/api/manage/config/get' && method === 'POST') {
    return handleManageConfigGet(currentUser, env)
  }

  if (pathname === '/api/manage/config/save' && method === 'POST') {
    return handleManageConfigSave(request, env, currentUser)
  }

  if (pathname === '/api/manage/tagList' && method === 'POST') {
    return handleManageTagList(request, env, currentUser)
  }

  if (pathname === '/api/manage/saveTag' && method === 'POST') {
    return handleManageSaveTag(request, env, currentUser)
  }

  if (pathname === '/api/manage/toggleHot' && method === 'POST') {
    return handleManageToggleHot(request, env, currentUser)
  }

  if (pathname === '/api/manage/title/titleList' && method === 'POST') {
    return handleManageTitleList(request, env, currentUser)
  }

  if (pathname === '/api/manage/title/saveTitle' && method === 'POST') {
    return handleManageSaveTitle(request, env, currentUser)
  }

  if (pathname === '/api/manage/title/assign' && method === 'POST') {
    return handleManageTitleAssign(request, env, currentUser)
  }

  if (pathname === '/api/manage/title/remove' && method === 'POST') {
    return handleManageTitleRemove(request, env, currentUser)
  }

  if (pathname === '/api/manage/userList' && method === 'POST') {
    return handleManageUserList(request, env, currentUser)
  }

  if (pathname === '/api/manage/member/banUser' && method === 'POST') {
    return handleManageBanUser(request, env, currentUser)
  }

  if (pathname === '/api/manage/member/revokeBanUser' && method === 'POST') {
    return handleManageRevokeBanUser(request, env, currentUser)
  }

  if (pathname === '/api/manage/member/point' && method === 'POST') {
    return handleManageMemberPoint(request, env, currentUser)
  }

  if (pathname === '/api/manage/post/postList' && method === 'POST') {
    return handleManagePostList(request, env, currentUser)
  }

  if (pathname === '/api/manage/post/togglePin' && method === 'POST') {
    return handleManageTogglePin(env, currentUser, url)
  }

  if (pathname === '/api/manage/post/delete' && method === 'POST') {
    return handleManageDeletePost(env, currentUser, url)
  }

  if (pathname === '/api/manage/commentList' && method === 'POST') {
    return handleManageCommentList(request, env, currentUser)
  }

  if (pathname === '/api/manage/comment/delete' && method === 'POST') {
    return handleManageDeleteComment(env, currentUser, url)
  }

  if (pathname === '/api/manage/testEmail' && method === 'POST') {
    return handleTestEmail(request, env, currentUser)
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

function shouldResolveCurrentUser(pathname: string, method: string) {
  if (method === 'GET' && PUBLIC_GET_API_PATHS.has(pathname)) {
    return false
  }
  return !PUBLIC_API_PATHS.has(pathname)
}

async function buildConfigResponse(env: Env) {
  const config = await getSysConfig(env)
  const headers = new Headers({
    'Cache-Control': 'no-store',
  })
  return json({
    success: true,
    data: getPublicSysConfig(config),
    version: env.APP_VERSION || '1.0',
  }, headers)
}

async function buildVersionResponse(env: Env) {
  return json({
    success: true,
    version: env.APP_VERSION || '1.0',
  })
}

async function handleLogin(request: Request, env: Env) {
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
  if (!user || !(await verifyPasswordWrapper(password, user.password_hash))) {
    return json({ success: false, message: '用户名/密码不正确' })
  }

  const now = nowIso()
  await run(env, 'UPDATE users SET last_login = ?, updated_at = ? WHERE id = ?', [now, now, user.id])

  const { createToken } = await import('./auth')
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

async function handleRegister(request: Request, env: Env) {
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
      || !isSameEmailWrapper(String(record.target_email || ''), email)
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

async function handleSignIn(env: Env, currentUser: CurrentUser) {
  const todayStart = startOfDayIsoWrapper()
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
  const point = getRandomIntWeightedWrapper(config.pointPerDaySignInMin, config.pointPerDaySignInMax)
  const now = nowIso()
  await run(env, 'INSERT INTO point_history (created_at, updated_at, reason, uid, point) VALUES (?, ?, ?, ?, ?)', [now, now, 'SIGNIN', currentUser.uid, point])
  const newPoint = currentUser.point + point
  await run(env, 'UPDATE users SET point = ?, level = ?, updated_at = ?, last_active = ? WHERE uid = ?', [newPoint, getUserLevelByPoint(newPoint), now, now, currentUser.uid])

  return json({ success: true, message: `签到成功,获得${point}积分` })
}

async function handleSaveSettings(request: Request, env: Env, currentUser: CurrentUser) {
  const body = await readBody(request)
  const email = normalizeEmail(String(body.email || ''))
  if (!email.includes('@')) {
    return json({ success: false, message: '请填写正确邮箱地址' })
  }

  const now = nowIso()
  const avatarUrl = await sha256Hex(email)
  const headImg = normalizeNullableStringWrapper(body.headImg)
  const css = normalizeNullableStringWrapper(body.css)
  const js = normalizeNullableStringWrapper(body.js)
  const signature = normalizeNullableStringWrapper(body.signature)

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

async function handleCreateInviteCode(env: Env, currentUser: CurrentUser) {
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

async function handleInviteCodeList(env: Env, currentUser: CurrentUser) {
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

async function handlePostNew(request: Request, env: Env, currentUser: CurrentUser | null) {
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
  const editingPid = normalizeNullableStringWrapper(body.pid)
  const hide = Boolean(body.hide)
  const hideContent = normalizeNullableStringWrapper(body.hideContent)
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
    pid = formatDatePidWrapper(config.postUrlFormat.dateFormat)
  }

  const now = nowIso()
  const { calculateHotPoint } = await import('./post')
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

async function handlePostSupport(env: Env, currentUser: CurrentUser | null, url: URL) {
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
    await run(env, 'UPDATE posts SET support_count = CASE WHEN support_count > 0 THEN support_count - 1 ELSE 0 END, updated_at = ? WHERE pid = ?', [nowIso(), pid])
  }
  else {
    await run(env, 'INSERT INTO post_support (uid, pid, created_at, updated_at) VALUES (?, ?, ?, ?)', [currentUser.uid, pid, nowIso(), nowIso()])
    await run(env, 'UPDATE posts SET support_count = support_count + 1, updated_at = ? WHERE pid = ?', [nowIso(), pid])
  }

  await run(env, 'UPDATE users SET last_active = ?, updated_at = ? WHERE uid = ?', [nowIso(), nowIso(), currentUser.uid])
  await syncPostPoint(env, pid)
  return json({ success: true })
}

async function handlePostFav(env: Env, currentUser: CurrentUser | null, url: URL) {
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

async function handlePostPay(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function handleCommentNew(request: Request, env: Env, currentUser: CurrentUser | null) {
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
  const editingCid = normalizeNullableStringWrapper(body.cid)
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
  const mentioned = extractMentionsWrapper(content).filter(name => name !== `@${postAuthorUsername}`)

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
      `你的帖子《${post.title}》有了新回复${buildSiteLinkWrapper(config, `/post/${pid}#${cid}`) ? `\n${buildSiteLinkWrapper(config, `/post/${pid}#${cid}`)}` : ''}`,
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
        `你在帖子《${post.title}》中被提到了${buildSiteLinkWrapper(config, `/post/${pid}#${cid}`) ? `\n${buildSiteLinkWrapper(config, `/post/${pid}#${cid}`)}` : ''}`,
      )
    }
  }

  await syncPostPoint(env, pid)
  return json({ success: true })
}

async function handleMemberPost(request: Request, env: Env, currentUser: CurrentUser | null) {
  const body = await readBody(request)
  const username = String(body.username || '')
  const page = getPage(body.page)
  const size = getSize(body.size, 20)
  const user = await first(env, 'SELECT * FROM users WHERE username = ?', [username])
  if (!user) {
    return json({ success: false, message: '用户不存在' })
  }

  const { postListSql } = await import('./post')
  const includeFav = currentUser?.id != null
  const rows = await all(env, postListSql('WHERE p.uid = ?', 'p.created_at DESC', 'LIMIT ? OFFSET ?', includeFav), includeFav ? [currentUser.id, user.uid, size, (page - 1) * size] : [user.uid, size, (page - 1) * size])
  const total = await queryCount(env, 'SELECT COUNT(*) AS count FROM posts WHERE uid = ?', [user.uid])

  return json({
    success: true,
    posts: await buildPostSummaries(env, rows, currentUser?.id),
    total,
  })
}

async function handleMemberComment(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function handleMemberFav(request: Request, env: Env, currentUser: CurrentUser | null) {
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
      p.reply_count AS comments_count,
      p.support_count,
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

async function handleMemberPoint(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function handleManageConfigGet(currentUser: CurrentUser | null, env: Env) {
  if (!isAdmin(currentUser)) {
    return json({ success: false, message: '只有管理员才能访问' })
  }
  return json({ success: true, config: await getSysConfig(env) })
}

async function handleManageConfigSave(request: Request, env: Env, currentUser: CurrentUser | null) {
  if (!isAdmin(currentUser)) {
    return json({ success: false, message: '只有管理员才能访问' })
  }
  const body = await readBody(request)
  await saveSysConfig(env, body)
  return json({ success: true })
}

async function handleManageTagList(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function handleManageSaveTag(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function handleManageToggleHot(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function handleManageTitleList(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function handleManageSaveTitle(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function handleManageTitleAssign(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function handleManageTitleRemove(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function handleManageUserList(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function handleManageBanUser(request: Request, env: Env, currentUser: CurrentUser | null) {
  if (!isAdmin(currentUser)) {
    return json({ success: false, message: '只有管理员才能访问' })
  }
  const body = await readBody(request)
  const uid = String(body.uid || '')
  const day = Number(body.day || 0)
  await run(env, 'UPDATE users SET status = ?, banned_end = ?, updated_at = ? WHERE uid = ?', ['BANNED', new Date(Date.now() + day * DAY_MS).toISOString(), nowIso(), uid])
  return json({ success: true })
}

async function handleManageRevokeBanUser(request: Request, env: Env, currentUser: CurrentUser | null) {
  if (!isAdmin(currentUser)) {
    return json({ success: false, message: '只有管理员才能访问' })
  }
  const body = await readBody(request)
  await run(env, 'UPDATE users SET status = ?, banned_end = NULL, updated_at = ? WHERE uid = ?', ['NORMAL', nowIso(), String(body.uid || '')])
  return json({ success: true })
}

async function handleManageMemberPoint(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function handleManagePostList(request: Request, env: Env, currentUser: CurrentUser | null) {
  if (!isAdmin(currentUser)) {
    return json({ success: false, message: '只有管理员才能访问' })
  }
  const body = await readBody(request)
  const page = getPage(body.page)
  const size = getSize(body.size, 20)
  const username = String(body.username || '').trim()
  const where = username ? 'WHERE au.username LIKE ?' : ''
  const { postListSql } = await import('./post')
  const includeFav = currentUser?.id != null
  const args = username
    ? includeFav ? [currentUser.id, `%${username}%`, size, (page - 1) * size] : [`%${username}%`, size, (page - 1) * size]
    : includeFav ? [currentUser.id, size, (page - 1) * size] : [size, (page - 1) * size]
  const rows = await all(env, postListSql(where, 'p.created_at DESC', 'LIMIT ? OFFSET ?', includeFav), args)
  const total = await queryCount(env, `SELECT COUNT(*) AS count FROM posts p JOIN users au ON au.uid = p.uid ${where}`, username ? [`%${username}%`] : [])
  return json({ success: true, posts: await buildPostSummaries(env, rows, currentUser?.id), total })
}

async function handleManageTogglePin(env: Env, currentUser: CurrentUser | null, url: URL) {
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

async function handleManageDeletePost(env: Env, currentUser: CurrentUser | null, url: URL) {
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

async function handleManageCommentList(request: Request, env: Env, currentUser: CurrentUser | null) {
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

async function handleManageDeleteComment(env: Env, currentUser: CurrentUser | null, url: URL) {
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

async function handleSendEmail(request: Request, env: Env) {
  const body = await readBody(request)
  const scene = String(body.scene || body.sence || '').trim().toUpperCase()
  const email = normalizeEmail(String(body.email || ''))
  if (!isValidEmailWrapper(email)) {
    return json({ success: false, emailCodeKey: '', message: '请填写正确的邮箱地址' })
  }
  if (scene !== 'REGISTER') {
    return json({ success: false, emailCodeKey: '', message: '发送邮件失败' })
  }

  const config = await getSysConfig(env)
  if (!config.regWithEmailCodeVerify) {
    return json({ success: false, emailCodeKey: '', message: '未开启邮件验证码验证注册' })
  }

  const emailError = validateEmailConfigWrapper(config.email)
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
  const emailCode = randomNumericCodeWrapper(6)
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

async function handleTestEmail(request: Request, env: Env, currentUser: CurrentUser | null) {
  if (!isAdmin(currentUser)) {
    return json({ success: false, message: '只有管理员才能访问' })
  }

  const body = await readBody(request)
  const emailConfig = normalizeEmailConfigWrapper(body.email)
  const emailError = validateEmailConfigWrapper(emailConfig)
  if (emailError) {
    return json({ success: false, message: emailError })
  }
  if (!isValidEmailWrapper(emailConfig.to)) {
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

async function handleSendForgotPasswordEmail(request: Request, env: Env) {
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
  const emailError = validateEmailConfigWrapper(config.email)
  if (emailError) {
    return json({ success: false, emailCodeKey: '', message: emailError })
  }

  const targetEmail = normalizeEmail(String(target.email || ''))
  const rateLimited = await isEmailSendRateLimited(env, targetEmail, 'RESET_PASSWORD')
  if (rateLimited) {
    return json({ success: false, emailCodeKey: '', message: '不要频繁发送邮件!' })
  }

  const emailCodeKey = randomId('em_')
  const emailCode = randomNumericCodeWrapper(6)
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

async function handleResetPwd(request: Request, env: Env) {
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
    || !isSameEmailWrapper(String(record.target_email || ''), String(target.email || ''))) {
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

// Wrapper functions for utils
function startOfDayIsoWrapper() {
  const date = new Date()
  date.setHours(0, 0, 0, 0)
  return date.toISOString()
}

function getRandomIntWeightedWrapper(min: number, max: number) {
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

function normalizeNullableStringWrapper(value: any) {
  const text = String(value ?? '').trim()
  return text ? text : null
}

function formatDatePidWrapper(pattern: string) {
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

function extractMentionsWrapper(text: string) {
  const regex = /\[@([^\]]+)\]/g
  return (text.match(regex) || []).map(match => match.slice(1, -1))
}

async function verifyPasswordWrapper(password: string, stored: string) {
  const { verifyPassword } = await import('./auth')
  return verifyPassword(password, stored)
}

function isSameEmailWrapper(left: string, right: string) {
  return normalizeEmail(left) === normalizeEmail(right)
}

function isValidEmailWrapper(value: string) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)
}

function validateEmailConfigWrapper(config: any) {
  if (!config.apiKey) {
    return '请先配置 Resend API Key'
  }
  if (!isValidEmailWrapper(String(config.from || ''))) {
    return '请先配置发件邮箱'
  }
  return ''
}

function normalizeEmailConfigWrapper(value: any) {
  const email = value && typeof value === 'object' ? value : {}
  const password = String(email.password || '').trim()
  return {
    apiKey: String(email.apiKey || (password.startsWith('re_') ? password : '')).trim(),
    from: normalizeEmail(String(email.from || email.username || '')),
    senderName: String(email.senderName || '').trim(),
    to: normalizeEmail(String(email.to || '')),
  }
}

function randomNumericCodeWrapper(length: number) {
  let result = ''
  for (let i = 0; i < length; i++) {
    result += `${Math.floor(Math.random() * 10)}`
  }
  return result
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

function buildSiteLinkWrapper(config: any, path: string) {
  const siteUrl = String(config?.websiteUrl || '').trim().replace(/\/+$/, '')
  if (!siteUrl) {
    return ''
  }
  return `${siteUrl}${path.startsWith('/') ? path : `/${path}`}`
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
    const response = new Response(cached.body, cached)
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
