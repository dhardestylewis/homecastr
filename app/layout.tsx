import type React from "react"
import type { Metadata, Viewport } from "next"
import { Geist, Geist_Mono } from "next/font/google"
import { Toaster } from "@/components/ui/toaster"
import { DatadogRum } from "@/components/datadog-rum"
import "./globals.css"

const _geist = Geist({ subsets: ["latin"] })
const _geistMono = Geist_Mono({ subsets: ["latin"] })

const APP_URL = "https://homecastr.com"
const OG_DESCRIPTION =
  "Know where home prices are headed. Built for buyers, agents, and investors."

export const metadata: Metadata = {
  title: "Homecastr Home Forecasts",
  description: OG_DESCRIPTION,
  metadataBase: new URL(APP_URL),
  icons: {
    icon: [
      { url: "/homecastr-icon.svg", type: "image/svg+xml" },
      { url: "/homecastr-icon.png", type: "image/png" },
    ],
    apple: "/homecastr-icon.png",
  },
  openGraph: {
    type: "website",
    url: APP_URL,
    siteName: "Homecastr",
    title: "Homecastr Home Forecasts",
    description: OG_DESCRIPTION,
    images: [
      {
        url: "/og-image.png",
        width: 1200,
        height: 630,
        alt: "Homecastr - AI price forecasts for Houston real estate",
      },
    ],
  },
  twitter: {
    card: "summary_large_image",
    title: "Homecastr Home Forecasts",
    description: OG_DESCRIPTION,
    images: ["/og-image.png"],
  },
}

export const viewport: Viewport = {
  themeColor: [
    { media: "(prefers-color-scheme: light)", color: "#f8fafc" },
    { media: "(prefers-color-scheme: dark)", color: "#0f1419" },
  ],
  width: "device-width",
  initialScale: 1,
  interactiveWidget: "overlays-content",
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <link href="https://assets.calendly.com/assets/external/widget.css" rel="stylesheet" />
      </head>
      <body className={`font-sans antialiased overflow-hidden`} suppressHydrationWarning>
        {children}
        <DatadogRum />
        <Toaster />
        <script src="https://assets.calendly.com/assets/external/widget.js" async />
      </body>
    </html>
  )
}
