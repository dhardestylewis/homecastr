/** @type {import('next').NextConfig} */
const nextConfig = {
  turbopack: {
    root: import.meta.dirname,
  },
  experimental: {
    instrumentationHook: true,
  },
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    unoptimized: true,
  },
}

export default nextConfig
