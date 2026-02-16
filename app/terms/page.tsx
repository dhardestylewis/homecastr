export default function TermsPage() {
    return (
        <div className="overflow-auto h-screen">
            <div className="min-h-screen bg-background text-foreground font-sans">
                <div className="max-w-3xl mx-auto px-6 py-16">
                    <a href="/api-docs" className="text-sm text-primary hover:underline mb-8 inline-block">← Back to API Docs</a>
                    <h1 className="text-4xl font-bold tracking-tight mb-2">Terms of Service</h1>
                    <p className="text-muted-foreground text-sm mb-10">Last updated: February 2026</p>

                    <div className="prose prose-sm max-w-none space-y-6 text-foreground/90 leading-relaxed">
                        <section>
                            <h2 className="text-xl font-semibold mb-3">1. Acceptance of Terms</h2>
                            <p className="text-sm">
                                By accessing or using the Homecastr API and related services, you agree to be bound by these
                                Terms of Service. If you do not agree, you may not use the service.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">2. Service Description</h2>
                            <p className="text-sm">
                                Homecastr provides AI-powered real estate forecasting data through a REST API. Our models
                                generate probabilistic price bands and growth projections based on publicly available data.
                                Forecasts are statistical estimates and should not be treated as financial advice.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">3. No Financial Advice</h2>
                            <p className="text-sm">
                                Homecastr is a data and analytics platform. Nothing provided through our API or dashboard
                                constitutes financial, investment, legal, or tax advice. All forecasts are probabilistic
                                estimates subject to uncertainty. You should consult qualified professionals before making
                                any real estate decisions.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">4. API Usage</h2>
                            <ul className="list-disc pl-5 text-sm space-y-1">
                                <li>API keys are issued per-user and are non-transferable</li>
                                <li>Rate limits apply and vary by plan tier</li>
                                <li>You may not resell raw API data without a commercial license</li>
                                <li>Automated scraping or excessive requests may result in throttling or suspension</li>
                            </ul>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">5. Intellectual Property</h2>
                            <p className="text-sm">
                                All models, algorithms, aggregated datasets, and visual designs are the intellectual property
                                of Homecastr. You retain ownership of any applications you build using our API, but may not
                                reverse-engineer our models or replicate our datasets.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">6. Limitation of Liability</h2>
                            <p className="text-sm">
                                Homecastr provides data on an &quot;as-is&quot; basis. We make no warranties regarding
                                the accuracy, completeness, or reliability of any forecast. In no event shall Homecastr
                                be liable for any indirect, incidental, or consequential damages arising from the use
                                of our service.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">7. Modifications</h2>
                            <p className="text-sm">
                                We reserve the right to modify these terms at any time. Continued use of the service after
                                changes constitutes acceptance of the updated terms.
                            </p>
                        </section>
                    </div>
                </div>
            </div>
        </div>
    )
}
