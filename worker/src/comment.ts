import type { Env, CurrentUser, UserTitleSummary } from './types'
import { all, first, queryCount, run } from './db'
import { getUserLevelByPoint, json, nowIso, parseJsonArray } from './utils'
import { getUserTitles } from './member'
import { buildSiteLink } from './utils'
import { sendTgMessage } from './telegram'
import { getSysConfig } from './config'
import { syncPostPoint } from './post'

export async function buildCommentReactionPayload(env: Env, cid: string, currentUserUid: string) {
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

export async function handleCommentReaction(env: Env, currentUser: CurrentUser | null, cidParam: string | null, type: 'LIKE' | 'DISLIKE') {
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

export async function handleCommentDetail(env: Env, currentUser: CurrentUser | null, cid: string) {
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

export async function buildComment(env: Env, row: any, currentUserUid: string, postUid: string, titlesByUserId?: Map<number, UserTitleSummary[]>) {
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

export async function buildCommentWithPost(env: Env, row: any, titlesByUserId?: Map<number, UserTitleSummary[]>) {
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

export async function buildCommentsWithPosts(env: Env, rows: any[]) {
  const { getUserTitlesMap } = await import('./member')
  const titlesByUserId = await getUserTitlesMap(env, rows.map(row => Number(row.author_id ?? 0)))
  return Promise.all(rows.map(row => buildCommentWithPost(env, row, titlesByUserId)))
}
