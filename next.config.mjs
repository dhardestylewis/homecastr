/** @type {import('next').NextConfig} */
const nextConfig = {
  turbopack: {
    root: import.meta.dirname,
  },
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    unoptimized: true,
  },

  async redirects() {
    return [
      {
        source: '/coverage/houston',
        destination: '/forecasts/tx/houston',
        permanent: true,
      },
      {
        source: '/coverage',
        destination: '/forecasts',
        permanent: true,
      },
    ]
  },
}

export default nextConfig
