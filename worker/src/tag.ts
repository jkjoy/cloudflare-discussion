import type { Env } from './types'
import { all } from './db'
import { DAY_MS, json } from './utils'

export function mapTag(row: any) {
  return {
    id: Number(row.id),
    name: row.name,
    enName: row.en_name,
    desc: row.desc,
    count: Number(row.count ?? 0),
    hot: Number(row.hot) === 1,
  }
}

export async function buildTagListResponse(env: Env, url: URL) {
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
  const headers = new Headers({
    'Cache-Control': 'no-store',
  })

  return json({
    success: true,
    tags: rows.map(mapTag),
  }, headers)
}

export async function buildMemberHotResponse(env: Env) {
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
