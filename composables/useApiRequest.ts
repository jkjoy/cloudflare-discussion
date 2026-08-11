interface ApiResponseLike {
  success?: boolean
  message?: string
}

export function getApiErrorMessage(error: unknown, fallback: string) {
  if (error && typeof error === 'object' && 'data' in error) {
    const data = error.data
    if (data && typeof data === 'object' && 'message' in data && typeof data.message === 'string') {
      return data.message
    }
  }
  return error instanceof Error && error.message ? error.message : fallback
}

export function assertApiSuccess<T>(response: T, fallback: string): T & ApiResponseLike {
  if (!response || typeof response !== 'object') {
    throw new Error(fallback)
  }
  const apiResponse = response as ApiResponseLike
  if (apiResponse.success !== true) {
    throw new Error(apiResponse.message || fallback)
  }
  return response as T & ApiResponseLike
}

export function useApiRequest<T extends ApiResponseLike>(request: () => Promise<T>, fallback: string) {
  const data = shallowRef<T | null>(null)
  const pending = ref(true)
  const errorMessage = ref('')
  let requestId = 0

  async function execute() {
    const currentRequestId = ++requestId
    pending.value = true
    errorMessage.value = ''

    try {
      const response = assertApiSuccess(await request(), fallback)
      if (currentRequestId !== requestId) {
        return null
      }
      data.value = response
      return response
    }
    catch (error) {
      if (currentRequestId === requestId) {
        errorMessage.value = getApiErrorMessage(error, fallback)
      }
      return null
    }
    finally {
      if (currentRequestId === requestId) {
        pending.value = false
      }
    }
  }

  onBeforeUnmount(() => {
    requestId++
  })

  return { data, pending, errorMessage, execute }
}
