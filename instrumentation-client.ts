import posthog from 'posthog-js'

if (process.env.NEXT_PUBLIC_POSTHOG_KEY) {
    posthog.init(process.env.NEXT_PUBLIC_POSTHOG_KEY, {
        api_host: process.env.NEXT_PUBLIC_POSTHOG_HOST || 'https://app.posthog.com',
    })
} else if (process.env.NODE_ENV !== 'development') {
    console.warn('PostHog initialization failed: NEXT_PUBLIC_POSTHOG_KEY is missing.')
}
