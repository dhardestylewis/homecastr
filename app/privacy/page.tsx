export default function PrivacyPage() {
    return (
        <div className="overflow-auto h-screen">
            <div className="min-h-screen bg-background text-foreground font-sans">
                <div className="max-w-3xl mx-auto px-6 py-16">
                    <a href="/about" className="text-sm text-primary hover:underline mb-8 inline-block">← Back to Home</a>
                    <h1 className="text-4xl font-bold tracking-tight mb-2">Privacy Policy</h1>
                    <p className="text-muted-foreground text-sm mb-10">Last updated: February 17, 2026</p>

                    <div className="prose prose-sm max-w-none space-y-6 text-foreground/90 leading-relaxed">
                        <section>
                            <h2 className="text-xl font-semibold mb-3">1. Information We Collect</h2>
                            <p className="text-sm">
                                Homecastr collects minimal personal data. When you use our services (dashboard, API, or downloadable reports),
                                we may collect the following:
                            </p>
                            <ul className="list-disc pl-5 text-sm space-y-1 mt-2">
                                <li><strong>Account information:</strong> email address and name, if you create an account or request an API key</li>
                                <li><strong>Usage data:</strong> pages visited, properties searched, forecasts viewed, API endpoints called, timestamps, and referring URLs</li>
                                <li><strong>Technical data:</strong> IP address, browser type, device type, and operating system</li>
                                <li><strong>Payment information:</strong> processed securely by our third-party payment provider; we do not store card details</li>
                            </ul>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">2. How We Use Your Data</h2>
                            <p className="text-sm">We use collected information to:</p>
                            <ul className="list-disc pl-5 text-sm space-y-1 mt-2">
                                <li>Provide, maintain, and improve our forecasting services, dashboard, and API</li>
                                <li>Generate and deliver PDF forecast reports</li>
                                <li>Monitor usage patterns to prevent abuse and ensure uptime</li>
                                <li>Improve model accuracy, coverage, and product features</li>
                                <li>Produce aggregated, de-identified market analytics and research insights</li>
                                <li>Communicate service updates, product announcements, or changes to these terms</li>
                            </ul>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">3. Aggregated and De-Identified Data</h2>
                            <p className="text-sm">
                                We may create aggregated, de-identified, or anonymized data from the information we collect,
                                including usage patterns and search trends. This data does not identify you personally and is
                                not subject to the restrictions in this policy. We may use and share aggregated data for any
                                lawful business purpose, including market research, product development, and analytics services.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">4. Data Sources</h2>
                            <p className="text-sm">
                                Our forecasting models are trained on publicly available property records, tax appraisal data,
                                and permit records. We do not use private transaction data, personal financial information,
                                or credit data. Property data used in forecasts is aggregated at the neighborhood level and
                                does not identify individual homeowners.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">5. Data Sharing</h2>
                            <p className="text-sm">
                                We do not sell or rent your personal data to third parties. We may share information with:
                            </p>
                            <ul className="list-disc pl-5 text-sm space-y-1 mt-2">
                                <li><strong>Service providers:</strong> hosting, payment processing, and analytics providers who assist in operating our services, bound by confidentiality obligations</li>
                                <li><strong>Legal compliance:</strong> when required by law, legal process, or to protect our rights and safety</li>
                                <li><strong>Business transfers:</strong> in connection with a merger, acquisition, or sale of assets, your data may be transferred to the successor entity</li>
                                <li><strong>Aggregated data:</strong> de-identified, aggregated analytics may be shared with partners or published as market research</li>
                            </ul>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">6. Cookies and Tracking</h2>
                            <p className="text-sm">
                                We use essential cookies to maintain session state and preferences. We may use analytics tools
                                (such as privacy-respecting analytics) to understand how our services are used. We do not use
                                third-party advertising trackers.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">7. Data Retention &amp; Security</h2>
                            <p className="text-sm">
                                Account data is retained as long as your account is active. API request logs are retained for
                                90 days and then automatically purged. Aggregated analytics data may be retained indefinitely.
                                All data is encrypted in transit (TLS 1.3) and at rest. We follow industry-standard security
                                practices to protect your information.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">8. Your Rights</h2>
                            <p className="text-sm">
                                You may request access to, correction of, or deletion of your personal data at any time by
                                contacting us. If you are located in the EU/EEA, California, or other jurisdictions with
                                applicable data protection laws, you may have additional rights including the right to
                                data portability and the right to object to certain processing.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">9. Children&apos;s Privacy</h2>
                            <p className="text-sm">
                                Our services are not directed to individuals under the age of 18. We do not knowingly collect
                                personal data from children.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">10. Changes to This Policy</h2>
                            <p className="text-sm">
                                We may update this policy from time to time. We will notify you of material changes by posting
                                the updated policy on this page with a revised &quot;Last updated&quot; date. Your continued use of our
                                services after changes constitutes acceptance of the updated policy.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">11. Contact</h2>
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
