import type { Env } from './types'

export async function all(env: Env, sql: string, params: any[]) {
  const result = await env.DB.prepare(sql).bind(...params).all<any>()
  return result.results || []
}

export async function first(env: Env, sql: string, params: any[]) {
  return env.DB.prepare(sql).bind(...params).first<any>()
}

export async function run(env: Env, sql: string, params: any[]) {
  return env.DB.prepare(sql).bind(...params).run()
}

export async function queryCount(env: Env, sql: string, params: any[]) {
  const row = await first(env, sql, params)
  return Number(row?.count ?? 0)
}
