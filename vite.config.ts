import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

// https://vitejs.dev/config/
export default defineConfig({
  base: "/smokedduck/",
  server: {
    port: 5000, // Specify the desired port here
    host: true,
  },
  plugins: [svelte()]
})
