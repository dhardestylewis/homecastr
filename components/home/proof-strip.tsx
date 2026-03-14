export function ProofStrip() {
  return (
    <section className="border-y border-border bg-muted/30 overflow-hidden">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-10">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-8 md:gap-12">
          {/* Quantified proof points - safer claims */}
          <div className="text-center md:text-left">
            <div className="text-3xl md:text-4xl font-bold tracking-tight">1M+</div>
            <div className="text-sm text-muted-foreground mt-1">Properties Covered</div>
          </div>
          
          <div className="text-center md:text-left">
            <div className="text-3xl md:text-4xl font-bold tracking-tight">1–5yr</div>
            <div className="text-sm text-muted-foreground mt-1">Forecast Horizons</div>
          </div>
          
          <div className="text-center md:text-left">
            <div className="text-3xl md:text-4xl font-bold tracking-tight">Published</div>
            <div className="text-sm text-muted-foreground mt-1">Accuracy Metrics</div>
          </div>
          
          <div className="text-center md:text-left">
            <div className="text-3xl md:text-4xl font-bold tracking-tight">P10/P50/P90</div>
            <div className="text-sm text-muted-foreground mt-1">Probability Bands</div>
          </div>
        </div>
      </div>
    </section>
  )
}
