interface Props {
    schemaVersion: string
    originYear: number
    minForecastYear?: number
    maxForecastYear?: number
}

export function MethodCaveat({ schemaVersion, originYear, minForecastYear, maxForecastYear }: Props) {
    return (
        <section id="method" className="space-y-4">
            {/* Always visible caveat */}
            <div className="glass-panel rounded-xl p-4 text-xs text-muted-foreground leading-relaxed">
                <p>
                    Forecasts are probabilistic model outputs, not guarantees. The model uses historical data and economic indicators; it does not observe property-specific conditions. This is not financial, investment, or real estate advice.{' '}
                    <a href="/methodology" className="text-primary hover:underline">Learn more about our methodology</a>.
                </p>
            </div>
            
            {/* Collapsed detailed methodology */}
            <details className="glass-panel rounded-xl">
                <summary className="p-4 cursor-pointer text-sm font-medium text-muted-foreground hover:text-foreground transition-colors flex items-center gap-2">
                    <svg className="w-4 h-4 transition-transform details-open:rotate-90" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                    </svg>
                    Methodology &amp; Data Sources
                </summary>
                <div className="px-5 pb-5 space-y-4 text-sm text-muted-foreground leading-relaxed">
                    <div className="grid gap-4 sm:grid-cols-2">
                        <div className="space-y-1">
                            <p className="text-xs uppercase tracking-wider text-muted-foreground">Model</p>
                            <p className="text-foreground/70">Homecastr World Model (Schrödinger Bridge v12)</p>
                        </div>
                        <div className="space-y-1">
                            <p className="text-xs uppercase tracking-wider text-muted-foreground">Forecast Horizon</p>
                            <p className="text-foreground/70">{minForecastYear && maxForecastYear ? `${minForecastYear} to ${maxForecastYear}` : `1 to 5 years from origin year ${originYear}`}</p>
                        </div>
                        <div className="space-y-1">
                            <p className="text-xs uppercase tracking-wider text-muted-foreground">Schema Version</p>
                            <p className="text-foreground/70 font-mono text-xs">{schemaVersion}</p>
                        </div>
                        <div className="space-y-1">
                            <p className="text-xs uppercase tracking-wider text-muted-foreground">Citation</p>
                            <p className="text-foreground/70 text-xs italic">
                                Hardesty Lewis, D. ({new Date().getFullYear()}). Homecastr Home Price Forecast. Retrieved from{' '}
                                <a href="https://www.homecastr.com" target="_blank" rel="noopener noreferrer" className="hover:text-primary transition-colors underline decoration-muted-foreground/50 underline-offset-2">
                                    homecastr.com
                                </a>.
                            </p>
                        </div>
                    </div>

                    <hr className="border-border" />

                    <div className="space-y-2">
                        <p className="text-xs uppercase tracking-wider text-muted-foreground">Data Sources</p>
                        <ul className="space-y-1 text-xs text-foreground/80">
                            <li>
                                <a href="https://www.census.gov/programs-surveys/acs" target="_blank" rel="noopener noreferrer" className="hover:text-primary transition-colors underline decoration-muted-foreground/50 underline-offset-2">
                                    U.S. Census Bureau. American Community Survey (ACS)
                                </a>
                                , 5-Year Estimates, Table B25077 (Median Home Value), Census Tract level.
                            </li>
                            <li>
                                <a href="https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html" target="_blank" rel="noopener noreferrer" className="hover:text-primary transition-colors underline decoration-muted-foreground/50 underline-offset-2">
                                    U.S. Census Bureau. TIGER/Line Shapefiles
                                </a>
                                , Census Tract and ZCTA boundaries.
                            </li>
                            <li>
                                <a href="https://fred.stlouisfed.org/" target="_blank" rel="noopener noreferrer" className="hover:text-primary transition-colors underline decoration-muted-foreground/50 underline-offset-2">
                                    Federal Reserve Bank of St. Louis (FRED)
                                </a>
                                . 30-Year Fixed Rate Mortgage Average (MORTGAGE30US), Unemployment Rate (UNRATE), Consumer Price Index (CPIAUCSL).
                            </li>
                        </ul>
                    </div>
                </div>
            </details>
        </section>
    )
}
