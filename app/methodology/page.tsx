import type { Metadata } from "next"
import Link from "next/link"
import { ArrowLeft, ArrowRight } from "lucide-react"

export const metadata: Metadata = {
    title: "Engineering & Methodology | Homecastr",
    description: "Deep dive into the architecture, spatial diffusion models, and evaluation rigor powering Homecastr's probabilistic forecasts.",
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
                        How we beat Zillow's benchmark by leveraging spatial diffusion models, rigorous calibration, and optimized inference engines.
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
                        Most real estate platforms treat forecasting as a point-estimation problem. Homecastr operates on the premise that single-point estimates (e.g., "this home will be worth exactly $450,000 next year") are fundamentally misleading due to thermodynamic noise in local markets. Instead, we architected an end-to-end pipeline that outputs rigorous <strong>probabilistic trajectories</strong> for over 150 million parcels.
                    </p>

                    <h2 className="text-3xl font-bold mt-16 mb-6">1. MLOps & Data Architecture</h2>
                    <p>
                        Scale requires dropping manual ETL processes in favor of automated, spatial DAGs. Our pipeline ingests hundreds of disparate sources—from localized NYC RPAD roll data to Florida DOR geometric dumps.
                    </p>
                    <ul>
                        <li><strong className="text-foreground">Spatial Indexing:</strong> We leverage Uber's H3 topological grid to map messy county geometries into consistent feature spaces, enabling fast neighbor lookups.</li>
                        <li><strong className="text-foreground">Feature Store:</strong> A centralized repository aggregates 114 spatial and macroeconomic features, pushing transformations into Supabase and Redis for low-latency batch retrieval.</li>
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

                    <h2 className="text-3xl font-bold mt-16 mb-6">2. Schrödinger Bridge Architecture</h2>
                    <p>
                        Predicting trajectories subject to macro-economic shocks requires moving beyond standard gradient boosting. We operate a Schrödinger Bridge architecture relying on <strong>spatial inducing tokens</strong> and FT-Transformer backbones.
                    </p>
                    <p>
                        Our objective is to balance deterministic predictive precision with calibrated generative trajectories. We employ <strong>DDIM (Denoising Diffusion Implicit Models) sampling</strong>, normalized across per-horizon loss surfaces. This allows us to explicitly model the uncertainty bands (P10 downside, P90 upside) rather than guessing at confidence intervals post-hoc.
                    </p>

                    <h2 className="text-3xl font-bold mt-16 mb-6">3. Evaluation Rigor & The Benchmark</h2>
                    <p>
                        Shipping a model to production without rigorous offline evaluation is a recipe for silent degradation. We evaluate model performance across two primary axes: predictive accuracy against industry standards, and probabilistic calibration.
                    </p>
                    
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 my-8 not-prose">
                        <div className="p-6 rounded-xl bg-muted/30 border border-border/50">
                            <h3 className="text-sm font-bold uppercase tracking-wider text-muted-foreground mb-2">1-Year Error (vs. Zillow)</h3>
                            <div className="flex items-baseline gap-2">
                                <span className="text-4xl font-extrabold text-primary">8.0%</span>
                                <span className="text-sm text-muted-foreground line-through decoration-destructive/50">8.4%</span>
                            </div>
                            <p className="text-sm text-muted-foreground mt-2">Homecastr outperformed the baseline benchmark in held-out test sets.</p>
                        </div>
                        <div className="p-6 rounded-xl bg-muted/30 border border-border/50">
                            <h3 className="text-sm font-bold uppercase tracking-wider text-muted-foreground mb-2">Long-Horizon Stability</h3>
                            <div className="flex items-baseline gap-2">
                                <span className="text-4xl font-extrabold text-primary">25%</span>
                                <span className="text-sm font-medium text-muted-foreground">MdAE</span>
                            </div>
                            <p className="text-sm text-muted-foreground mt-2">Median Absolute Error maintained stably over a 4-year forecast horizon.</p>
                        </div>
                    </div>

                    <p>
                        Beyond standard error metrics, all model candidates must pass through our <strong>Calibration Packets</strong> framework. This suite optimizes for specific statistical guarantees, including Probability Integral Transform (PIT) uniformity, sharp interval coverage, and tail risk validation.
                    </p>

                    <h2 className="text-3xl font-bold mt-16 mb-6">4. Explainable Forecasts via Surrogate SHAP</h2>
                    <p>
                        Black-box deep learning models are notoriously difficult to trust in high-stakes environments. High predictive accuracy is useless if we cannot explain <em>why</em> a trajectory shifted.
                    </p>
                    <p>
                        We implemented a post-hoc attribution script utilizing <strong>gradient-based Surrogate SHAP</strong>. This allows us to extract feature gradients directly from the FT-Transformer layers and map them backward to raw, human-readable variables (e.g., "Interest Rates", "Local Zoning", "Demographics"). This powers the "Explainable Forecasts" UI visible on the platform, directly aligning model weights with human intuition.
                    </p>

                    <h2 className="text-3xl font-bold mt-16 mb-6">5. Production Serving & Latency Optimization</h2>
                    <p>
                        A robust model is worthless if it costs too much to run or takes too long to load. Once the offline pipeline completes, we run a deterministic, shard-level parallelized inference engine to dump the probabilistic arrays.
                    </p>
                    <p>
                        On the frontend, every millisecond counts. We deployed a three-tier server-side caching architecture (Supabase + GCS + Google APIs) backed by localized LRU caches for our Google Street View integration. This drops our effective image cost to <strong>$0.007 per image</strong> while guaranteeing sub-second render times, even on mobile.
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
