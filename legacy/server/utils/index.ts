import short from 'short-uuid'
import dayjs from 'dayjs'

import { PrismaPg } from '@prisma/adapter-pg'
import { PrismaClient } from '@prisma/client'
import nodemailer from 'nodemailer'

import { createCache, memoryStore } from 'cache-manager'
import pg from 'pg'
// import TelegramBot from 'node-telegram-bot-api'
// import TGBot from '../tgBot'
import type { SysConfigDTO, recaptchaResponse } from '~/types'

const { Pool } = pg

interface GlobalWithPrisma {
  __discussionPool?: pg.Pool
  __discussionPrisma?: PrismaClient
}

const globalForPrisma = globalThis as typeof globalThis & GlobalWithPrisma

function getDatabaseUrl() {
  const connectionString = process.env.DATABASE_URL
  if (!connectionString) {
    throw new Error('DATABASE_URL 未配置')
  }
  return connectionString
}

function getPrismaClient() {
  if (!globalForPrisma.__discussionPool) {
    globalForPrisma.__discussionPool = new Pool({ connectionString: getDatabaseUrl() })
  }

  if (!globalForPrisma.__discussionPrisma) {
    const adapter = new PrismaPg(globalForPrisma.__discussionPool)
    globalForPrisma.__discussionPrisma = new PrismaClient({ adapter, log: ['warn', 'error'] })
  }

  return globalForPrisma.__discussionPrisma
}

export const prisma = new Proxy({} as PrismaClient, {
  get(_target, prop) {
    const client = getPrismaClient() as any
    const value = client[prop]
    return typeof value === 'function' ? value.bind(client) : value
  },
})

export function randomId() {
  return short.generate().toString()
}

export function getAvatarUrl(hash: string) {
  const config = useRuntimeConfig()
  return `${config.public.avatarCdn}${hash}`
}

const userLevelRanges = [
  { min: 0, max: 200, level: 1 },
  { min: 200, max: 400, level: 2 },
  { min: 400, max: 900, level: 3 },
  { min: 900, max: 1600, level: 4 },
  { min: 1600, max: 2500, level: 5 },
  { min: 2500, max: Number.POSITIVE_INFINITY, level: 6 },
]

export function getUserLevelByPoint(point: number) {
  const target = userLevelRanges.find(range => point >= range.min && point < range.max)
  return target?.level ?? 1
}

export async function syncUserLevel(uid: string) {
  const user = await prisma.user.findUnique({
    where: { uid },
    select: {
      point: true,
      level: true,
    },
  })

  if (!user) {
    return
  }

  const nextLevel = getUserLevelByPoint(user.point)
  if (nextLevel === user.level) {
    return
  }

  await prisma.user.update({
    where: { uid },
    data: {
      level: nextLevel,
    },
  })
}

export async function syncPostPoint(pid: string) {
  const post = await prisma.post.findUnique({
    where: { pid },
    select: {
      pid: true,
      createdAt: true,
      uid: true,
      author: {
        select: {
          point: true,
        },
      },
      _count: {
        select: {
          PostSupport: true,
        },
      },
    },
  })

  if (!post) {
    return
  }

  const commentCount = await prisma.comment.count({
    where: {
      pid,
      uid: {
        not: post.uid,
      },
    },
  })
  const second = dayjs().diff(post.createdAt, 'second')
  const point = ((post.author.point * 2 + post._count.PostSupport * 2 + commentCount - 1) / (second + 600) ** 1.1) * 10000000

  await prisma.post.update({
    where: { pid },
    data: {
      point,
    },
  })
}

export const emailCodeCache = createCache(memoryStore({
  max: 100,
  ttl: 5 * 60 * 1000,
}))

export async function sendMail(to: string, subject: string, html: string) {
  const config = await prisma.sysConfig.findFirst({})
  const sysConfigDTO = config?.content as unknown as SysConfigDTO
  const { host, port, username, password, senderName } = sysConfigDTO.email
  if (host === '' || port === 0 || username === '' || password === '' || senderName === '') {
    return '请先配置邮箱'
  }

  return sendMailWithParams({ ...sysConfigDTO.email, to, subject, html }, sysConfigDTO.ForwardUrl)
}

export interface sendMailParams {
  host: string
  username: string
  port: number
  secure: boolean
  password: string
  to: string
  subject: string
  html: string
  senderName: string
}

export async function sendMailWithParams({ host, username, port, secure, password, to, subject, html, senderName }: sendMailParams, url: string) {
  if (host === '' || port === 0 || username === '' || password === '' || senderName === '') {
    return '请先配置邮箱'
  }
  if (url) {
    const res: any = await fetch(url, {
      method: 'post',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ host, username, port, secure, password, to, subject, html, senderName }),
    })
    const body = await res.json()
    return body.message
  }

  try {
    const transporter = nodemailer.createTransport({
      host,
      port,
      secure,
      auth: {
        user: username,
        pass: password,
      },
      dnsTimeout: 3000,
      socketTimeout: 3000,
      greetingTimeout: 3000,
      connectionTimeout: 3000,
    })

    await transporter.sendMail({
      from: `${senderName} <${username}>`,
      to,
      subject,
      html,
    })
  }
  catch (e: any) {
    console.log(e)
    return `发送邮件失败${'message' in e ? e.message : ''}`
  }
  return ''
}

export async function checkGoogleRecaptcha(sk: string, token?: string) {
  if (!token) {
    return {
      success: false,
      message: '请先通过人机验证',
    }
  }
  const url = `https://recaptcha.net/recaptcha/api/siteverify?secret=${sk}&response=${token}`
  const response = (await $fetch(url)) as any as recaptchaResponse
  if (response.success === false) {
    return {
      success: false,
      message: '傻逼,还来??',
    }
  }
  if (response.score <= 0.5) {
    return {
      success: false,
      message: '二货,你是不是人机?',
    }
  }
  return {
    success: true,
    message: '验证通过',
  }
}

export async function sendTgMessage(sysConfigDTO: SysConfigDTO, chatId: string | null, message: string) {
  if (!chatId) {
    return
  }
  if (sysConfigDTO.notify?.tgBotEnabled && sysConfigDTO.notify.tgBotToken) {
    let url = sysConfigDTO.notify.tgProxyUrl ? sysConfigDTO.notify.tgProxyUrl : 'https://api.telegram.org'
    if (url.endsWith('/')) {
      url = url.substring(0, url.length - 1)
    }
    const target = `${url}/bot${sysConfigDTO.notify.tgBotToken}/sendMessage`
    const escapeMessage = message.replace(/[_*[\]()~`>#+=|{}.!-]/g, '\\$&')
    console.log(new Date(), '开始发送tg消息通知，chatId:', chatId, 'message:', escapeMessage, target)
    try {
      const res = await fetch(target, {
        method: 'POST',
        body: JSON.stringify({
          chat_id: chatId,
          text: escapeMessage,
          parse_mode: 'MarkdownV2',
        }),
        headers: {
          'Content-Type': 'application/json',
        },
      })
      const resJson = await res.json()
      console.log('tg消息发送结果:', resJson)
    }
    catch (e) {
      console.log('tg消息发送失败:', e)
    }
  }
}
