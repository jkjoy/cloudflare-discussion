import type { Env, CurrentUser, UserTitleSummary } from './types'
import { all, first, queryCount } from './db'
import { json, parseJsonArray } from './utils'
import { mapCurrentUser, sanitizeUser, ensureUserSecretKey } from './auth'

export async function getUserTitlesMap(env: Env, userIds: number[]) {
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

export async function getUserTitles(env: Env, userId: number) {
  const titlesByUserId = await getUserTitlesMap(env, [userId])
  return titlesByUserId.get(Number(userId)) || []
}

export async function getUsernameByUid(env: Env, uid: string) {
  const row = await first(env, 'SELECT username FROM users WHERE uid = ?', [uid])
  return row?.username || ''
}

export async function buildUserSummary(env: Env, row: any, includePrivateFields = false) {
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

export async function buildProfile(env: Env, currentUser: CurrentUser) {
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

export async function handleMemberDetail(env: Env, currentUser: CurrentUser | null, username: string) {
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
