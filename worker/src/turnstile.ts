export async function verifyTurnstile(secretKey: string, token?: string, expectedAction?: string, request?: Request) {
  if (!token) {
    return {
      success: false,
      message: '请先通过人机验证',
    }
  }

  const formData = new URLSearchParams({
    secret: secretKey,
    response: token,
  })
  const remoteIp = request?.headers.get('CF-Connecting-IP') || request?.headers.get('X-Forwarded-For')?.split(',')[0]?.trim()
  if (remoteIp) {
    formData.set('remoteip', remoteIp)
  }

  const response = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded',
    },
    body: formData.toString(),
  })
  const result = await response.json() as any
  if (!result.success) {
    return {
      success: false,
      message: '请先通过人机验证',
    }
  }
  if (expectedAction && result.action !== expectedAction) {
    return {
      success: false,
      message: '请先通过人机验证',
    }
  }
  return { success: true, message: '验证通过' }
}
