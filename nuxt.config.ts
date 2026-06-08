export default defineNuxtConfig({
  ssr: true,

  colorMode: {
    storageKey: 'vueuse-color-scheme',
  },

  modules: ['@nuxt/ui', '@vueuse/nuxt'],

  ui: {
    icons: ['carbon', 'heroicons'],
  },

  icon: {
    serverBundle: {
      collections: ['carbon', 'heroicons'],
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
    devProxy: {
      '/api/': { target: 'http://localhost:8787/api/', changeOrigin: true },
      '/imgs/': { target: 'http://localhost:8787/imgs/', changeOrigin: true },
    },
    // 含浏览器专属依赖的页面保持 SPA 渲染
    routeRules: {
      '/post/**': { ssr: false },
      '/manage/**': { ssr: false },
    },
  },

  compatibilityDate: '2025-04-01',
})
