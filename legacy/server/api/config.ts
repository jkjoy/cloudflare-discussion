export default defineEventHandler(async (_) => {
  const config = await prisma.sysConfig.findFirst({
    select: {
      content: true,
    },
  })
  const runtimeConfig = useRuntimeConfig()
  return {
    success: true,
    data: config?.content,
    version: runtimeConfig.public.appVersion || 'cloudflare',
  }
})
