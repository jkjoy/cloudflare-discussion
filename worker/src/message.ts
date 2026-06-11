import type { Env, CurrentUser } from './types'
import { all, first, queryCount, run } from './db'
import { json, nowIso, getPage, getSize, readBody, buildSiteLink } from './utils'
import { sendTgMessage, buildPrivateMessageTelegramText } from './telegram'
import { getSysConfig } from './config'
import { verifyTurnstile } from './turnstile'

export function mapMessageUser(row: any, prefix: string) {
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

export function mapMessageRow(row: any) {
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

export async function handleSendPrivateMessage(request: Request, env: Env, currentUser: CurrentUser | null) {
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

export async function handlePrivateMessageInbox(request: Request, env: Env, currentUser: CurrentUser | null) {
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

export async function handlePrivateMessageList(request: Request, env: Env, currentUser: CurrentUser | null) {
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

export async function handleMemberMessages(request: Request, env: Env, currentUser: CurrentUser | null) {
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

export async function handleReadMessages(url: URL, env: Env, currentUser: CurrentUser | null) {
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
