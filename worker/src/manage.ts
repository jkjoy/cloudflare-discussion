import type { Env, CurrentUser } from './types'
import { all, first, queryCount, run } from './db'
import { json, nowIso, getPage, getSize, readBody, clone, deepMerge, normalizeEmailConfig } from './utils'
import { defaultSysConfig, normalizeSysConfig } from './config'
import { buildUserSummary } from './member'

export async function buildManageComment(env: Env, row: any) {
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
