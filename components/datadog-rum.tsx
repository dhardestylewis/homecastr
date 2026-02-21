"use client"

import { useEffect } from "react"
import { datadogRum } from "@datadog/browser-rum"

export function DatadogRum() {
  useEffect(() => {
    if (typeof window === "undefined") return
    const appId = process.env.NEXT_PUBLIC_DD_APPLICATION_ID
    const clientToken = process.env.NEXT_PUBLIC_DD_CLIENT_TOKEN
    if (!appId || !clientToken) return

    datadogRum.init({
      applicationId: appId,
      clientToken,
      site: process.env.NEXT_PUBLIC_DD_SITE || "datadoghq.com",
      service: "homecastr-frontend",
      env: process.env.NEXT_PUBLIC_DD_ENV || process.env.NODE_ENV || "development",
      sessionSampleRate: 100,
      sessionReplaySampleRate: 0,
    })
    datadogRum.startSessionReplayRecording()
  }, [])
  return null
}
