/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  experimental: {
    appDir: true,
  },
  env: {
    SURREAL_URL: process.env.SURREAL_URL,
  },
}

module.exports = nextConfig