import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import dayjs from 'dayjs'
import short from 'short-uuid'
import type { SysConfigDTO } from '~/types'

interface FileInfo {
  name: string
  filename: string
  data: Uint8Array
  type: string
}

function getObjectStorageDomain(config: SysConfigDTO) {
  const domain = config.s3.domain?.trim()
  if (!domain) {
    return ''
  }
  return domain.endsWith('/') ? domain.slice(0, -1) : domain
}

export default defineEventHandler(async (event) => {
  if (!event.context.uid) {
    throw createError('请先去登录')
  }

  const formData = await readMultipartFormData(event)
  if (!formData) {
    return {
      success: false,
      message: 'No file found',
      filename: '',
    }
  }

  const file = formData[0] as FileInfo
  if (!file.type.startsWith('image')) {
    return {
      success: false,
      message: '只支持上传图片',
      filename: '',
    }
  }

  const sysConfig = await prisma.sysConfig.findFirst()
  const sysConfigDTO = sysConfig?.content as unknown as SysConfigDTO
  const filetype = file.type.split('/')[1] || 'png'
  const filename = `${short.generate()}.${filetype}`

  if (sysConfigDTO?.upload?.imgStrategy === 's3') {
    const domain = getObjectStorageDomain(sysConfigDTO)
    if (!domain) {
      return {
        success: false,
        message: '请先配置 S3 / R2 的公开访问域名',
        filename: '',
      }
    }

    const key = `discussion/${dayjs().format('YYYY/MM/DD/')}${filename}`
    const client = new S3Client({
      region: sysConfigDTO.s3.region,
      endpoint: sysConfigDTO.s3.endpoint,
      credentials: {
        accessKeyId: sysConfigDTO.s3.ak,
        secretAccessKey: sysConfigDTO.s3.sk,
      },
    })

    await client.send(new PutObjectCommand({
      Bucket: sysConfigDTO.s3.bucket,
      Key: key,
      Body: file.data,
      ContentType: file.type,
    }))

    return {
      success: true,
      filename: `${domain}/${key}`,
      message: '上传文件成功!',
    }
  }

  const config = useRuntimeConfig()
  const filepath = `${config.uploadDir}/${filename}`

  try {
    const fs = await import('node:fs/promises')
    await fs.mkdir(config.uploadDir, { recursive: true })
    await fs.writeFile(filepath, file.data)
  }
  catch (e) {
    console.log('filepath is : ', filepath)
    console.log('writeFile error is : ', e)
  }

  return {
    success: true,
    filename: `/imgs/${filename}`,
    message: '上传文件成功!',
  }
})
