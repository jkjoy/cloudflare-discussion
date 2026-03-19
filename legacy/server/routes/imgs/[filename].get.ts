export default defineEventHandler(async (event) => {
  const filename = getRouterParam(event, 'filename')
  if (!filename) {
    throw createError({
      statusCode: 404,
      statusMessage: '图片不存在',
    })
  }

  const config = useRuntimeConfig()
  const filepath = `${config.uploadDir}/${filename}`
  const fs = await import('node:fs')

  if (!fs.existsSync(filepath)) {
    throw createError({
      statusCode: 404,
      statusMessage: '图片不存在',
    })
  }

  return sendStream(event, fs.createReadStream(filepath))
})
