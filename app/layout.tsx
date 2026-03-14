import type React from "react"
import type { Metadata, Viewport } from "next"
import { Geist, Geist_Mono } from "next/font/google"
import { Toaster } from "@/components/ui/toaster"
import { DatadogInit } from "@/components/datadog-init"
import { CSPostHogProvider } from "@/components/posthog-provider"
import "./globals.css"

const _geist = Geist({ subsets: ["latin"] })
const _geistMono = Geist_Mono({ subsets: ["latin"] })

const APP_URL = "https://homecastr.com"
const OG_DESCRIPTION =
  "Know where home prices are headed with our data-driven housing market forecasts. Built for buyers, agents, and investors."

export const metadata: Metadata = {
  title: "Homecastr Home Price Forecasts",
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
    title: "Homecastr Home Price Forecasts",
    description: OG_DESCRIPTION,
    images: [
      {
        url: "/og-image.png",
        width: 1200,
        height: 630,
        alt: "Homecastr - AI home price forecasts across the United States",
      },
    ],
  },
  twitter: {
    card: "summary_large_image",
    title: "Homecastr Home Price Forecasts",
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
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{
            __html: JSON.stringify({
              "@context": "https://schema.org",
              "@type": "WebSite",
              name: "Homecastr",
              url: APP_URL,
              potentialAction: {
                "@type": "SearchAction",
                target: `${APP_URL}/app?q={search_term_string}`,
                "query-input": "required name=search_term_string"
              }
            })
          }}
        />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{
            __html: JSON.stringify({
              "@context": "https://schema.org",
              "@type": "Organization",
              name: "Homecastr",
              url: APP_URL,
              logo: `${APP_URL}/homecastr-icon.png`,
              sameAs: [
                "https://linkedin.com/company/homecastr"
              ]
            })
          }}
        />
      </head>
      <body className={`font-sans antialiased overflow-hidden`} suppressHydrationWarning>
        <CSPostHogProvider>
          {children}
          <DatadogInit />
          <Toaster />
          <script src="https://assets.calendly.com/assets/external/widget.js" async />
        </CSPostHogProvider>
      </body>
    </html>
  )
}
