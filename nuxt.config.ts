export default defineNuxtConfig({
  ssr: false,

  colorMode: {
    storageKey: 'vueuse-color-scheme',
  },

  modules: ['@nuxt/ui', '@vueuse/nuxt'],

  ui: {
    icons: ['carbon'],
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
  compatibilityDate: '2025-04-01',
})
