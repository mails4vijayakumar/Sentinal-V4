import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/api':  { target: process.env.VITE_API_BASE || 'http://localhost:8001', changeOrigin: true },
      '/sse':  { target: process.env.VITE_API_BASE || 'http://localhost:8001', changeOrigin: true },
      '/rdb':  { target: process.env.VITE_ROUTING_DB_BASE || 'http://localhost:8000', changeOrigin: true, rewrite: p => p.replace(/^\/rdb/, '') },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
    rollupOptions: {
      output: {
        manualChunks: {
          vendor: ['react', 'react-dom', 'react-router-dom'],
          charts: ['recharts'],
        },
      },
    },
  },
})
