import type { Env } from './types'
import { first, run } from './db'
import { buildSiteLink, json, nowIso, readBody, truncateText } from './utils'
import { getSysConfig } from './config'

export async function handleTelegramWebhook(request: Request, env: Env) {
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

export function getTelegramUpdateMessage(body: any) {
  const message = body?.message ?? body?.edited_message ?? body?.channel_post ?? body?.edited_channel_post
  return {
    text: String(message?.text || '').trim(),
    chatId: message?.chat?.id ? String(message.chat.id) : '',
  }
}

export function parseTelegramBindingCommand(text: string) {
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

export async function sendTgMessage(config: any, chatId: string | null | undefined, message: string) {
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

export function buildPrivateMessageTelegramText(config: any, fromUsername: string, content: string) {
  const profileUrl = buildSiteLink(config, `/member/${encodeURIComponent(fromUsername)}`)
  const lines = [
    `你收到了来自 ${fromUsername} 的一条私信`,
    profileUrl ? `发送者主页：${profileUrl}` : '',
    '',
    truncateText(content, 1000),
  ]
  return lines.filter(Boolean).join('\n')
}
