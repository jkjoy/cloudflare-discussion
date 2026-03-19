interface TurnstileRenderOptions {
  sitekey: string
  action?: string
  execution?: 'render' | 'execute'
  appearance?: 'always' | 'execute' | 'interaction-only'
  callback?: (token: string) => void
  'expired-callback'?: () => void
  'error-callback'?: () => void
}

interface TurnstileApi {
  render(container: string | HTMLElement, options: TurnstileRenderOptions): string
  execute(container: string | HTMLElement): void
  reset(widgetId: string): void
  remove(widgetId: string): void
}

declare global {
  interface Window {
    turnstile?: TurnstileApi
  }
}

export {}
