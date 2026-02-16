export default function PrivacyPage() {
    return (
        <div className="overflow-auto h-screen">
            <div className="min-h-screen bg-background text-foreground font-sans">
                <div className="max-w-3xl mx-auto px-6 py-16">
                    <a href="/api-docs" className="text-sm text-primary hover:underline mb-8 inline-block">← Back to API Docs</a>
                    <h1 className="text-4xl font-bold tracking-tight mb-2">Privacy Policy</h1>
                    <p className="text-muted-foreground text-sm mb-10">Last updated: February 2026</p>

                    <div className="prose prose-sm max-w-none space-y-6 text-foreground/90 leading-relaxed">
                        <section>
                            <h2 className="text-xl font-semibold mb-3">1. Information We Collect</h2>
                            <p className="text-sm">
                                Homecastr collects minimal personal data. When you use our API, we may log request metadata
                                (IP address, timestamps, and query parameters) for security, rate limiting, and service improvement.
                                We do not collect names, emails, or payment information unless you explicitly provide them when
                                requesting an API key.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">2. How We Use Your Data</h2>
                            <p className="text-sm">We use collected information exclusively to:</p>
                            <ul className="list-disc pl-5 text-sm space-y-1 mt-2">
                                <li>Provide and maintain our forecasting services</li>
                                <li>Monitor usage patterns to prevent abuse and ensure uptime</li>
                                <li>Improve model accuracy and API performance</li>
                                <li>Communicate service updates or changes</li>
                            </ul>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">3. Data Sources</h2>
                            <p className="text-sm">
                                Our forecasting models are trained on publicly available property records, tax appraisal data,
                                and permit records. We do not use private transaction data, personal financial information,
                                or credit data. All property data is aggregated at the neighborhood (H3 hex) level and does not
                                identify individual homeowners.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">4. Data Sharing</h2>
                            <p className="text-sm">
                                We do not sell, rent, or share your personal data with third parties. Aggregated, anonymized
                                usage statistics may be used internally for product improvement. We may disclose information
                                if required by law or to protect our rights and safety.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">5. Data Retention & Security</h2>
                            <p className="text-sm">
                                API request logs are retained for 90 days and then automatically purged. All data is encrypted
                                in transit (TLS 1.3) and at rest. We follow industry-standard security practices to protect
                                your information.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">6. Contact</h2>
                            <p className="text-sm">
                                For privacy-related inquiries, contact us at{" "}
                                <a href="mailto:privacy@homecastr.com" className="text-primary hover:underline">privacy@homecastr.com</a>.
                            </p>
                        </section>
                    </div>
                </div>
            </div>
        </div>
    )
}
