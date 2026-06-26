import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

// base './' so the built app is servable from any path (athar_view serves it
// from the localhost root; a hosted deployment may mount it under a prefix).
// @ifc-lite packages are excluded from dep optimization so their internal
// `new URL('ifc-lite_bg.wasm', import.meta.url)` asset references survive.
export default defineConfig({
  plugins: [svelte()],
  base: './',
  optimizeDeps: {
    exclude: ['@ifc-lite/wasm', '@ifc-lite/geometry'],
  },
  build: {
    target: 'esnext',
    chunkSizeWarningLimit: 1600,
  },
})
