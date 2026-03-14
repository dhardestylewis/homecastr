"use client"

import { useEffect } from "react"
import { useAssistant, type ForecastContext } from "./AssistantProvider"

interface Props {
  context: ForecastContext
}

/**
 * Syncs the current forecast page context to the assistant.
 * Place this in forecast pages to keep the assistant grounded.
 */
export function ForecastContextSync({ context }: Props) {
  const { setForecastContext } = useAssistant()

  useEffect(() => {
    setForecastContext(context)
    
    // Clear context when unmounting (navigating away from forecast)
    return () => {
      // Don't clear immediately - allow navigation between forecasts
    }
  }, [context, setForecastContext])

  return null
}
