export default function TermsPage() {
    return (
        <div className="overflow-auto h-screen">
            <div className="min-h-screen bg-background text-foreground font-sans">
                <div className="max-w-3xl mx-auto px-6 py-16">
                    <a href="/about" className="text-sm text-primary hover:underline mb-8 inline-block">← Back to Home</a>
                    <h1 className="text-4xl font-bold tracking-tight mb-2">Terms of Service</h1>
                    <p className="text-muted-foreground text-sm mb-10">Last updated: February 17, 2026</p>

                    <div className="prose prose-sm max-w-none space-y-6 text-foreground/90 leading-relaxed">
                        <section>
                            <h2 className="text-xl font-semibold mb-3">1. Acceptance of Terms</h2>
                            <p className="text-sm">
                                By accessing or using the Homecastr dashboard, API, downloadable reports, or any related services
                                (collectively, the &quot;Services&quot;), you agree to be bound by these Terms of Service. If you do not agree,
                                you may not use the Services.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">2. Service Description</h2>
                            <p className="text-sm">
                                Homecastr provides AI-powered real estate forecasting data through a web dashboard, downloadable PDF
                                reports, and a REST API. Our models generate probabilistic price bands and growth projections based
                                on publicly available data. Forecasts are statistical estimates and should not be treated as
                                financial advice.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">3. No Financial Advice</h2>
                            <p className="text-sm">
                                Homecastr is a data and analytics platform. Nothing provided through our Services constitutes
                                financial, investment, legal, or tax advice. All forecasts are probabilistic estimates subject
                                to uncertainty. You should consult qualified professionals before making any real estate
                                investment or transaction decisions.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">4. Accounts and API Keys</h2>
                            <p className="text-sm">You are responsible for maintaining the security of your account credentials and API keys. You agree to:</p>
                            <ul className="list-disc pl-5 text-sm space-y-1 mt-2">
                                <li>Keep your API key confidential and not share it with unauthorized parties</li>
                                <li>Notify us immediately if you suspect unauthorized use of your account</li>
                                <li>Accept responsibility for all activity under your account or API key</li>
                            </ul>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">5. Acceptable Use</h2>
                            <p className="text-sm">When using our Services, you agree not to:</p>
                            <ul className="list-disc pl-5 text-sm space-y-1 mt-2">
                                <li>Resell, redistribute, or sublicense raw API data without a commercial license from Homecastr</li>
                                <li>Use automated scraping or make excessive requests that degrade service for others</li>
                                <li>Attempt to reverse-engineer, decompile, or extract our models, algorithms, or datasets</li>
                                <li>Use the Services for any unlawful purpose or in violation of applicable regulations</li>
                                <li>Misrepresent forecasts as guaranteed outcomes in marketing materials or client communications</li>
                            </ul>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">6. Rate Limits and Usage Tiers</h2>
                            <p className="text-sm">
                                API access is subject to rate limits that vary by plan tier. Default limits are 100 requests per
                                minute and 10,000 requests per day. Exceeding rate limits may result in temporary throttling.
                                Persistent abuse may result in suspension of access. Contact us for higher limits or enterprise access.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">7. Intellectual Property</h2>
                            <p className="text-sm">
                                All models, algorithms, aggregated datasets, forecast outputs, visual designs, and documentation
                                are the intellectual property of Homecastr. You retain ownership of any applications you build
                                using our API. You may display and use forecast data within your applications, subject to proper
                                attribution to Homecastr.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">8. Data and Privacy</h2>
                            <p className="text-sm">
                                Your use of our Services is also governed by our{" "}
                                <a href="/privacy" className="text-primary hover:underline">Privacy Policy</a>, which describes
                                how we collect, use, and protect your information. By using the Services, you consent to the
                                practices described in the Privacy Policy.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">9. Disclaimers</h2>
                            <p className="text-sm">
                                The Services are provided on an &quot;as-is&quot; and &quot;as-available&quot; basis. We make no warranties,
                                express or implied, regarding the accuracy, completeness, reliability, or suitability of any
                                forecast or data. Real estate markets are inherently uncertain, and past model performance does
                                not guarantee future accuracy.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">10. Limitation of Liability</h2>
                            <p className="text-sm">
                                To the maximum extent permitted by law, Homecastr shall not be liable for any indirect, incidental,
                                special, consequential, or punitive damages, or any loss of profits, revenue, data, or business
                                opportunities arising from the use of or inability to use our Services, even if we have been
                                advised of the possibility of such damages.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">11. Termination</h2>
                            <p className="text-sm">
                                We may suspend or terminate your access to the Services at any time for violation of these Terms
                                or for any other reason at our discretion. You may stop using the Services at any time. Upon
                                termination, your API key will be revoked and your right to access the Services will cease.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">12. Governing Law</h2>
                            <p className="text-sm">
                                These Terms shall be governed by and construed in accordance with the laws of the State of Texas,
                                without regard to conflict of law principles. Any disputes arising from these Terms shall be
                                resolved in the courts located in Harris County, Texas.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">13. Modifications</h2>
                            <p className="text-sm">
                                We reserve the right to modify these Terms at any time. We will notify you of material changes
                                by posting the updated Terms on this page with a revised &quot;Last updated&quot; date. Your continued
                                use of the Services after changes constitutes acceptance of the updated Terms.
                            </p>
                        </section>

                        <section>
                            <h2 className="text-xl font-semibold mb-3">14. Contact</h2>
                            <p className="text-sm">
                                For questions about these Terms, contact us at{" "}
                                <a href="mailto:legal@homecastr.com" className="text-primary hover:underline">legal@homecastr.com</a>.
                            </p>
                        </section>
                    </div>
                </div>
            </div>
        </div>
    )
}
