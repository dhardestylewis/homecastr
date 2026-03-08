interface Props {
    schemaVersion: string
    originYear: number
}

export function MethodCaveat({ schemaVersion, originYear }: Props) {
    return (
        <section id="method" className="space-y-4">
            <h2 className="text-xl font-semibold text-foreground">Methodology &amp; Caveats</h2>

            <div className="glass-panel rounded-xl p-5 space-y-4 text-sm text-muted-foreground leading-relaxed">
                <div className="grid gap-4 sm:grid-cols-2">
                    <div className="space-y-1">
                        <p className="text-xs uppercase tracking-wider text-muted-foreground/60">Model</p>
                        <p className="text-foreground/70">Homecastr World Model (Schrödinger Bridge v12)</p>
                    </div>
                    <div className="space-y-1">
                        <p className="text-xs uppercase tracking-wider text-muted-foreground/60">Forecast Horizon</p>
                        <p className="text-foreground/70">1 to 5 years from origin year {originYear}</p>
                    </div>
                    <div className="space-y-1">
                        <p className="text-xs uppercase tracking-wider text-muted-foreground/60">Schema Version</p>
                        <p className="text-foreground/70 font-mono text-xs">{schemaVersion}</p>
                    </div>
                    <div className="space-y-1">
                        <p className="text-xs uppercase tracking-wider text-muted-foreground/60">Citation</p>
                        <p className="text-foreground/70 text-xs italic">
                            Hardesty Lewis, D. ({new Date().getFullYear()}). Homecastr Home Price Forecast. Retrieved from{' '}
                            <a href="https://devpost.com/software/homecastr" target="_blank" rel="noopener noreferrer" className="hover:text-primary transition-colors underline decoration-border underline-offset-2">
                                homecastr.com
                            </a>.
                        </p>
                    </div>
                </div>

                <hr className="border-border" />

                {/* Data Sources — formal references */}
                <div className="space-y-2">
                    <p className="text-xs uppercase tracking-wider text-muted-foreground/60">Data Sources</p>
                    <ul className="space-y-1 text-xs text-foreground/60">
                        <li>
                            <a href="https://www.census.gov/programs-surveys/acs" target="_blank" rel="noopener noreferrer" className="hover:text-primary transition-colors underline decoration-border underline-offset-2">
                                U.S. Census Bureau. American Community Survey (ACS)
                            </a>
                            , 5-Year Estimates, Table B25077 (Median Home Value), Census Tract level.
                        </li>
                        <li>
                            <a href="https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html" target="_blank" rel="noopener noreferrer" className="hover:text-primary transition-colors underline decoration-border underline-offset-2">
                                U.S. Census Bureau. TIGER/Line Shapefiles
                            </a>
                            , Census Tract and ZCTA boundaries.
                        </li>
                        <li>
                            <a href="https://fred.stlouisfed.org/" target="_blank" rel="noopener noreferrer" className="hover:text-primary transition-colors underline decoration-border underline-offset-2">
                                Federal Reserve Bank of St. Louis (FRED)
                            </a>
                            . 30-Year Fixed Rate Mortgage Average (MORTGAGE30US), Unemployment Rate (UNRATE), Consumer Price Index (CPIAUCSL).
                        </li>
                    </ul>
                </div>

                <hr className="border-border" />

                <p className="text-xs text-muted-foreground/70 leading-relaxed">
                    Forecasts are probabilistic model outputs, not guarantees. The model uses historical transaction data and economic indicators; it does not observe property-specific conditions, renovations, zoning changes, or localized events. Areas with sparse history will have wider forecast ranges. Comparable alternatives are algorithmically derived from forecast similarity. This content should not be construed as financial, investment, or real estate advice.
                </p>
            </div>
        </section>
    )
}
