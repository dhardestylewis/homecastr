export function ProofStrip() {
  return (
    <section className="border-y border-border bg-muted/30 overflow-hidden">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-10">
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-6 md:gap-12 w-full">
          {/* Quantified proof points - safer claims */}
          <div className="text-center md:text-left flex flex-col justify-center">
            <div className="text-2xl sm:text-3xl md:text-4xl font-bold tracking-tight">1M+</div>
            <div className="text-xs sm:text-sm text-muted-foreground mt-1">Properties</div>
          </div>
          
          <div className="text-center md:text-left flex flex-col justify-center">
            <div className="text-2xl sm:text-3xl md:text-4xl font-bold tracking-tight">1–5yr</div>
            <div className="text-xs sm:text-sm text-muted-foreground mt-1">Horizons</div>
          </div>
          
          <div className="text-center md:text-left flex flex-col justify-center">
            <div className="text-2xl sm:text-3xl md:text-4xl font-bold tracking-tight">Published</div>
            <div className="text-xs sm:text-sm text-muted-foreground mt-1">Accuracy</div>
          </div>
          
          <div className="text-center md:text-left flex flex-col justify-center">
            <div className="text-2xl sm:text-3xl md:text-4xl font-bold tracking-tight whitespace-nowrap">P10/P50/P90</div>
            <div className="text-xs sm:text-sm text-muted-foreground mt-1">Confidence</div>
          </div>
        </div>
      </div>
    </section>
  )
}
