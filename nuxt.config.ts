export default defineNuxtConfig({
  ssr: false,

  experimental: {
    emitRouteChunkError: 'automatic',
  },

  colorMode: {
    storageKey: 'vueuse-color-scheme',
  },

  modules: ['@nuxt/ui', '@vueuse/nuxt'],

  ui: {
    icons: ['carbon', 'heroicons'],
  },

  icon: {
    localApiEndpoint: '/_icon',
    serverBundle: {
      collections: ['carbon', 'heroicons'],
    },
    clientBundle: {
      scan: true,
    },
  },

  devtools: {
    enabled: false,
  },

  runtimeConfig: {
    public: {
      tokenKey: 'discussion_token',
      avatarCdn: 'https://gravatar.cooluc.com/avatar/',
      cookieSecure: false,
      appVersion: '1.0',
    },
  },

  nitro: {
    preset: 'cloudflare-module',
    // 开发时把 /api/ 和 /imgs/ 代理到 wrangler dev (默认 8787 端口)
    // 注意：/_icon 是 @nuxt/icon 的本地端点，由 Nuxt 自己处理，不代理
    devProxy: {
      '/api/': { target: 'http://localhost:8787/', changeOrigin: true },
      '/imgs/': { target: 'http://localhost:8787/', changeOrigin: true },
    },
  },

  compatibilityDate: '2025-04-01',
})
