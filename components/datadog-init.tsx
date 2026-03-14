"use client"

import { useEffect, useRef } from "react"
import { datadogRum } from "@datadog/browser-rum"
import { datadogLogs } from "@datadog/browser-logs"

export function DatadogInit() {
  const initialized = useRef(false)

  useEffect(() => {
    if (typeof window === "undefined" || initialized.current) return
    
    // Using existing RUM variables for client token if dedicated ones aren't set
    const appId = process.env.NEXT_PUBLIC_DD_APPLICATION_ID
    const clientToken = process.env.NEXT_PUBLIC_DD_CLIENT_TOKEN || process.env.NEXT_PUBLIC_DD_LOGS_CLIENT_TOKEN
    const site = process.env.NEXT_PUBLIC_DD_SITE || "datadoghq.com"
    const env = process.env.NEXT_PUBLIC_DD_ENV || process.env.NODE_ENV || "development"
    const service = process.env.NEXT_PUBLIC_DD_SERVICE || "homecastr-ui"
    
    if (!clientToken) {
        console.warn("Datadog: Missing client token, initialization skipped.")
        return
    }

    initialized.current = true

    // Initialize RUM if appId is available
    if (appId) {
        datadogRum.init({
        applicationId: appId,
        clientToken,
        site,
        service,
        env,
        proxy: '/api/dd',
        sessionSampleRate: 100,
        sessionReplaySampleRate: 20,
        trackUserInteractions: true,
        trackResources: true,
        trackLongTasks: true,
        defaultPrivacyLevel: 'mask-user-input',
        })
        datadogRum.startSessionReplayRecording()
    }

    // Initialize Logs
    datadogLogs.init({
      clientToken,
      site,
      forwardErrorsToLogs: true,
      sessionSampleRate: 100,
      env,
      service,
      proxy: '/api/dd',
    })
    
  }, [])
  
  return null
}
