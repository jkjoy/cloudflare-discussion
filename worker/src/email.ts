import type { Env } from './types'
import { first, queryCount, run } from './db'
import { escapeHtml, formatEmailAddress, normalizeEmail, nowIso, randomId, randomNumericCode } from './utils'
import type { EmailConfig } from './types'

export function buildRegisterEmailHtml(config: any, code: string) {
  const websiteName = escapeHtml(String(config.websiteName || '极简论坛'))
  const websiteUrl = String(config.websiteUrl || '').trim()
  const websiteLink = websiteUrl
    ? `<a href="${escapeHtml(websiteUrl)}">${websiteName}</a>`
    : websiteName

  return `<p>欢迎使用 ${websiteLink}</p><p>您的注册验证码是：<b>${escapeHtml(code)}</b></p><p>验证码 5 分钟内有效。</p>`
}

export function buildResetPasswordEmailHtml(config: any, code: string) {
  const websiteName = escapeHtml(String(config.websiteName || '极简论坛'))
  return `<p>${websiteName} 正在为您重置密码。</p><p>验证码是：<b>${escapeHtml(code)}</b></p><p>验证码 30 分钟内有效。</p>`
}

export async function isEmailSendRateLimited(env: Env, targetEmail: string, reason: string) {
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

export async function saveEmailCodeRecord(env: Env, input: {
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

export async function sendResendEmail(configInput: EmailConfig, to: string, subject: string, html: string, idempotencyKey?: string) {
  const config = normalizeEmailConfigForSend(configInput)
  if (!config.apiKey) {
    return { success: false, message: '请先配置 Resend API Key' }
  }
  if (!config.from) {
    return { success: false, message: '请先配置发件邮箱' }
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

function normalizeEmailConfigForSend(value: any): EmailConfig {
  const email = value && typeof value === 'object' ? value : {}
  const password = String(email.password || '').trim()

  return {
    apiKey: String(email.apiKey || (password.startsWith('re_') ? password : '')).trim(),
    from: normalizeEmail(String(email.from || email.username || '')),
    senderName: String(email.senderName || '').trim(),
    to: normalizeEmail(String(email.to || '')),
  }
}
