import type { Metadata } from "next"
import Link from "next/link"
import { ArrowLeft, ArrowRight } from "lucide-react"

export const metadata: Metadata = {
    title: "Engineering & Methodology | Homecastr",
    description: "Architecture, spatial generative models, calibration testing, and inference optimization behind Homecastr's probabilistic forecasts.",
}

export default function MethodologyPage() {
    return (
        <div className="h-screen overflow-y-auto bg-background text-foreground font-sans selection:bg-primary/20">
            <header className="py-4 px-6 md:px-12 border-b border-border/40 sticky top-0 bg-background/80 backdrop-blur-md z-10">
                <Link href="/" className="inline-flex items-center gap-2 text-sm font-medium text-muted-foreground hover:text-foreground transition-colors group">
                    <ArrowLeft className="w-4 h-4 group-hover:-translate-x-1 transition-transform" /> Back to Homecastr
                </Link>
            </header>
            
            {/* Tech Blog Header */}
            <div className="bg-muted/30 border-b border-border/40 py-16 px-6">
                 <div className="max-w-3xl mx-auto">
                    <div className="flex items-center gap-2 text-xs font-semibold text-primary uppercase tracking-wider mb-4">
                        <span>Engineering Blog</span>
                        <span className="w-1 h-1 rounded-full bg-primary/50"></span>
                        <span>Machine Learning</span>
                    </div>
                    <h1 className="text-4xl md:text-5xl font-extrabold tracking-tight mb-6 leading-tight">
                        Architecting Nationwide Probabilistic Forecasts at the Parcel Level
                    </h1>
                    <p className="text-xl text-muted-foreground leading-relaxed">
                        How we evaluated against a Zillow-based baseline using spatial generative models, calibration testing, and faster inference.
                    </p>
                    <div className="flex items-center gap-4 mt-8 pt-8 border-t border-border/40">
                         <div className="w-12 h-12 rounded-full overflow-hidden border border-border/50">
                             <img src="/dhl.jpg" alt="Author Headshot" className="w-full h-full object-cover" />
                         </div>
                         <div>
                            <p className="font-semibold">David Hardesty Lewis</p>
                            <p className="text-sm text-muted-foreground">ML Engineer & Founder</p>
                         </div>
                    </div>
                </div>
            </div>

            <main className="max-w-3xl mx-auto py-16 px-6">
                <div className="prose prose-neutral dark:prose-invert prose-lg max-w-none">
                    
                    <p className="lead text-xl text-muted-foreground">
                        Many consumer real-estate products present a single forecast value. We instead estimate a distribution over future parcel values, because local housing markets are noisy, heterogeneous, and exposed to shocks that point forecasts compress into one number. Homecastr's pipeline produces <strong>probabilistic forecasts</strong> for over 150 million parcels.
                    </p>

                    <h2 className="text-3xl font-bold mt-16 mb-6">1. Data and MLOps</h2>
                    <p>
                        At national scale, manual ETL does not hold up. Our pipeline ingests county assessment rolls, parcel geometries, and macro features from sources including NYC RPAD roll data and Florida DOR property records.
                    </p>
                    <ul>
                        <li><strong className="text-foreground">Spatial Indexing:</strong> We use Uber's H3 hexagonal grid to standardize neighborhood context and accelerate nearby-parcel lookups across inconsistent county geometries.</li>
                        <li><strong className="text-foreground">Feature Store:</strong> We store 114 spatial and macroeconomic features in PostGIS, Supabase, and Redis to support both batch inference and low-latency serving.</li>
                    </ul>

                    {/* Architecture Diagram */}
                    <div className="my-12 p-6 md:p-8 border border-border/50 rounded-2xl bg-muted/20 relative overflow-hidden not-prose">
                        <div className="absolute inset-0 bg-grid-white/5 bg-[size:20px_20px] [mask-image:linear-gradient(to_bottom,white,transparent)] dark:bg-grid-white/5"></div>
                        <div className="relative z-10 flex flex-col md:flex-row items-stretch justify-between gap-2 md:gap-4">
                            
                            {/* Ingestion */}
                            <div className="flex flex-col items-center flex-1 w-full relative group">
                                <div className="bg-card border border-border/50 rounded-xl p-5 w-full h-full text-center shadow-sm group-hover:border-primary/50 transition-colors flex flex-col justify-center">
                                    <div className="text-[10px] font-bold uppercase text-muted-foreground mb-3 tracking-wider">Data Ingestion</div>
                                    <div className="font-semibold text-sm">Supabase / PostGIS</div>
                                    <div className="text-xs text-muted-foreground mt-2">Florida DOR & NYC RPAD</div>
                                </div>
                                <div className="h-6 w-px bg-border md:hidden my-2"></div>
                                <ArrowRight className="hidden md:block absolute -right-3 top-1/2 -translate-y-1/2 text-muted-foreground/50 w-5 h-5 z-20 bg-background rounded-full" />
                            </div>
                            
                            {/* Feature Store */}
                            <div className="flex flex-col items-center flex-1 w-full relative group">
                                <div className="bg-card border border-border/50 rounded-xl p-5 w-full h-full text-center shadow-md ring-1 ring-primary/10 group-hover:ring-primary/30 transition-shadow flex flex-col justify-center">
                                    <div className="text-[10px] font-bold uppercase text-primary mb-3 tracking-wider">H3 Feature Store</div>
                                    <div className="font-semibold text-sm">114 Spatial Features</div>
                                    <div className="text-xs text-muted-foreground mt-2">Redis Batch Retrieval</div>
                                </div>
                                <div className="h-6 w-px bg-border md:hidden my-2"></div>
                                <ArrowRight className="hidden md:block absolute -right-3 top-1/2 -translate-y-1/2 text-muted-foreground/50 w-5 h-5 z-20 bg-background rounded-full" />
                            </div>

                            {/* Model */}
                            <div className="flex flex-col items-center flex-1 w-full relative group">
                                <div className="bg-primary/5 border border-primary/30 rounded-xl p-5 w-full h-full text-center shadow-md group-hover:bg-primary/10 transition-colors flex flex-col justify-center">
                                    <div className="text-[10px] font-bold uppercase text-primary mb-3 tracking-wider">Model Inference</div>
                                    <div className="font-semibold text-sm">Schrödinger Bridge</div>
                                    <div className="text-xs text-primary/80 mt-2">FT-Transformer</div>
                                </div>
                                <div className="h-6 w-px bg-border md:hidden my-2"></div>
                                <ArrowRight className="hidden md:block absolute -right-3 top-1/2 -translate-y-1/2 text-muted-foreground/50 w-5 h-5 z-20 bg-background rounded-full" />
                            </div>

                            {/* Serving */}
                            <div className="flex flex-col items-center flex-1 w-full group">
                                <div className="bg-card border border-border/50 rounded-xl p-5 w-full h-full text-center shadow-sm group-hover:border-primary/50 transition-colors flex flex-col justify-center">
                                    <div className="text-[10px] font-bold uppercase text-muted-foreground mb-3 tracking-wider">Serving Tier</div>
                                    <div className="font-semibold text-sm">Explainable SHAP</div>
                                    <div className="text-xs text-muted-foreground mt-2">P10, P50, P90 Arrays</div>
                                </div>
                            </div>
                            
                        </div>
                    </div>

                    <h2 className="text-3xl font-bold mt-16 mb-6">2. Model Architecture</h2>
                    <p>
                        In our experiments, gradient-boosted baselines did not capture multi-horizon uncertainty as well as a generative approach. Our current model combines an <strong>FT-Transformer</strong> tabular encoder (a self-attention model over heterogeneous features) with learned <strong>spatial tokens</strong> that summarize nearby parcel context, and a Schrödinger Bridge diffusion decoder for multi-horizon uncertainty.
                    </p>
                    <p>
                        We use <strong>DDIM (Denoising Diffusion Implicit Models) sampling</strong> to generate percentile paths (P10, P50, P90) directly, with loss normalized independently at each forecast horizon, instead of fitting uncertainty after the point forecast is produced.
                    </p>

                    <h2 className="text-3xl font-bold mt-16 mb-6">3. Evaluation</h2>
                    <p>
                        Skipping offline evaluation raises the risk of unnoticed performance drift. We evaluate each model candidate on held-out years across two axes: point-forecast accuracy against an industry baseline, and probabilistic calibration.
                    </p>
                    
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 my-8 not-prose">
                        <div className="p-6 rounded-xl bg-muted/30 border border-border/50">
                            <h3 className="text-sm font-bold uppercase tracking-wider text-muted-foreground mb-2">1-Year Error (vs. Zillow Baseline)</h3>
                            <div className="flex items-baseline gap-2">
                                <span className="text-4xl font-extrabold text-primary">8.0%</span>
                                <span className="text-sm text-muted-foreground line-through decoration-destructive/50">8.4%</span>
                            </div>
                            <p className="text-sm text-muted-foreground mt-2">Lower 1-year held-out error than the Zillow baseline in our test set.</p>
                        </div>
                        <div className="p-6 rounded-xl bg-muted/30 border border-border/50">
                            <h3 className="text-sm font-bold uppercase tracking-wider text-muted-foreground mb-2">Long-Horizon Stability</h3>
                            <div className="flex items-baseline gap-2">
                                <span className="text-4xl font-extrabold text-primary">25%</span>
                                <span className="text-sm font-medium text-muted-foreground">MdAE</span>
                            </div>
                            <p className="text-sm text-muted-foreground mt-2">Median Absolute Error remained roughly stable over a 4-year forecast horizon.</p>
                        </div>
                    </div>

                    <p>
                        Beyond standard error metrics, all model candidates pass through a calibration suite we call <strong>Calibration Packets</strong>—a set of diagnostics that check Probability Integral Transform (PIT) behavior, empirical interval coverage, and tail calibration before we promote a model to production.
                    </p>

                    <h2 className="text-3xl font-bold mt-16 mb-6">4. Explainable Forecasts</h2>
                    <p>
                        Accuracy alone is not enough if users cannot understand <em>why</em> a forecast changed. We run a post-hoc attribution step we call <strong>Surrogate SHAP</strong>: a gradient-based method that extracts approximate local feature attributions from the FT-Transformer layers and maps them to readable variables (e.g., interest rates, local zoning changes, demographic shifts).
                    </p>
                    <p>
                        These attributions feed the "Explainable Forecasts" UI on the platform, giving users per-forecast breakdowns of which inputs contributed most to the predicted trajectory.
                    </p>

                    <h2 className="text-3xl font-bold mt-16 mb-6">5. Serving and Cost</h2>
                    <p>
                        A model is less useful if inference cost or latency is too high for production. Once the offline pipeline completes, we split the workload into deterministic shards and run them in parallel to produce the full set of probabilistic arrays.
                    </p>
                    <p>
                        On the frontend, we use a three-tier server-side cache (Supabase, GCS, and Google APIs) with localized LRU caches for Street View images. This reduces our effective image cost to <strong>$0.007 per image</strong> and keeps typical render times below one second on tested mobile devices.
                    </p>
                    
                    <hr className="my-16 border-border/40" />
                    
                    <p className="italic text-muted-foreground text-center">
                        Interested in the engineering challenges behind Homecastr? Feel free to <a href="https://linkedin.com/in/dhardestylewis" target="_blank" rel="noopener noreferrer" className="text-primary hover:underline font-medium">connect on LinkedIn</a>.
                    </p>

                </div>
            </main>
        </div>
    )
}
