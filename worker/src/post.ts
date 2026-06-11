import type { Env, CurrentUser, UserTitleSummary } from './types'
import { all, first, queryCount, run } from './db'
import { json, nowIso, getPage, getSize, parseJsonArray, getUserLevelByPoint, extractMentions } from './utils'
import { getUserTitlesMap, getUserTitles, getUsernameByUid } from './member'
import { buildSiteLink } from './utils'
import { sendTgMessage } from './telegram'
import { getSysConfig } from './config'

const ALLOWED_ORDER_BY = new Set([
  'p.created_at DESC',
  'p.point DESC',
])

const ALLOWED_TAIL = new Set([
  '',
  'LIMIT ? OFFSET ?',
])

export function postListSql(whereClause: string, orderBy: string, tail = '', includeFav = false) {
  if (!ALLOWED_ORDER_BY.has(orderBy)) {
    throw new Error(`Invalid orderBy: ${orderBy}`)
  }
  if (!ALLOWED_TAIL.has(tail)) {
    throw new Error(`Invalid tail: ${tail}`)
  }

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
      p.reply_count AS comments_count,
      p.support_count,
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

export function calculateHotPoint(authorPoint: number, supportCount: number, commentCount: number, createdAt: string) {
  const second = Math.max(1, Math.floor((Date.now() - new Date(createdAt).getTime()) / 1000))
  return ((authorPoint * 2 + supportCount * 2 + commentCount - 1) / (second + 600) ** 1.1) * 10000000
}

export async function syncPostPoint(env: Env, pid: string) {
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

export async function queryPointSum(env: Env, uid: string, reason: string) {
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

export async function buildPostSummaries(env: Env, rows: any[], currentUserId?: number, includeContent = false) {
  const titlesByUserId = await getUserTitlesMap(env, rows.map(row => Number(row.author_id ?? 0)))
  return Promise.all(rows.map(row => buildPostSummary(env, row, currentUserId, includeContent, titlesByUserId)))
}

export async function buildPostSummary(env: Env, row: any, currentUserId?: number, includeContent = false, titlesByUserId?: Map<number, UserTitleSummary[]>) {
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

export function getPostListInputFromUrl(url: URL) {
  return {
    uid: url.searchParams.get('uid') || '',
    tag: url.searchParams.get('tag') || '',
    key: url.searchParams.get('key') || '',
    page: url.searchParams.get('page') || '',
    size: url.searchParams.get('size') || '',
  }
}

export async function buildPostListResponse(env: Env, currentUser: CurrentUser | null, input: any) {
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

export async function handlePostDetail(env: Env, currentUser: CurrentUser | null, pid: string, request: Request) {
  const { readBody } = await import('./utils')
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
      p.reply_count AS comments_count,
      p.support_count,
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
      COALESCE(lk.like_count, 0) AS like_count,
      COALESCE(dl.dislike_count, 0) AS dislike_count,
      ${includeReactionState
        ? `COALESCE(ul.liked_count, 0) AS liked_count,
      COALESCE(ud.disliked_count, 0) AS disliked_count`
        : '0 AS liked_count, 0 AS disliked_count'}
    FROM comments c
    JOIN users u ON u.uid = c.uid
    LEFT JOIN (
      SELECT cid, COUNT(*) AS like_count FROM comment_likes GROUP BY cid
    ) lk ON lk.cid = c.cid
    LEFT JOIN (
      SELECT cid, COUNT(*) AS dislike_count FROM comment_dislikes GROUP BY cid
    ) dl ON dl.cid = c.cid
    ${includeReactionState
      ? `LEFT JOIN (
      SELECT cid, COUNT(*) AS liked_count FROM comment_likes WHERE uid = ? GROUP BY cid
    ) ul ON ul.cid = c.cid
    LEFT JOIN (
      SELECT cid, COUNT(*) AS disliked_count FROM comment_dislikes WHERE uid = ? GROUP BY cid
    ) ud ON ud.cid = c.cid`
      : ''}
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
  post.comments = await Promise.all(commentRows.map(comment => buildCommentForPostDetail(env, comment, currentUser?.uid ?? '', row.uid, titlesByUserId)))
  post.support = support

  return json({ success: true, post })
}

async function buildCommentForPostDetail(env: Env, row: any, currentUserUid: string, postUid: string, titlesByUserId?: Map<number, UserTitleSummary[]>) {
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
