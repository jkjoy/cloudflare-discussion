import { config, type CodeMirrorExtension } from 'md-editor-v3'

export default defineNuxtPlugin(() => {
  config({
    codeMirrorExtensions(extensions: CodeMirrorExtension[]) {
      return extensions.filter(extension => extension.type !== 'linkShortener')
    },
  })
})
