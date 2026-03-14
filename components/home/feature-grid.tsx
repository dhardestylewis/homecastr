import { TrendingUp, Layers, Map, BarChart3 } from "lucide-react"

const features = [
  {
    icon: TrendingUp,
    title: "Future value, not just today",
    description: "Traditional estimates tell you current worth. We show 1-5 year forward projections."
  },
  {
    icon: Layers,
    title: "Range of outcomes",
    description: "Conservative, expected, and upside scenarios. P10/P50/P90 bands calibrated from thousands of simulations."
  },
  {
    icon: Map,
    title: "Property-level specificity",
    description: "Not just ZIP codes. Every parcel has its own forecast, because the house next door can have a different outlook."
  },
  {
    icon: BarChart3,
    title: "Explainable forecasts",
    description: "See what drives the numbers. Rate expectations, local demand, supply dynamics, and more."
  }
]

// SVG illustrations for each feature
function FutureTrendSVG() {
  return (
    <svg viewBox="0 0 200 80" className="w-full h-20 mt-4">
      {/* Historical line */}
      <path
        d="M 20 55 L 60 50 L 100 45"
        fill="none"
        stroke="currentColor"
        strokeWidth={2}
        strokeOpacity={0.3}
      />
      {/* Future projection with fan */}
      <path
        d="M 100 45 L 140 35 L 180 20 L 180 55 L 140 50 L 100 45 Z"
        fill="hsl(var(--accent))"
        fillOpacity={0.15}
      />
      <path
        d="M 100 45 L 140 38 L 180 30"
        fill="none"
        stroke="hsl(var(--accent))"
        strokeWidth={2}
      />
      {/* Now marker */}
      <line x1="100" y1="20" x2="100" y2="65" stroke="currentColor" strokeWidth={1} strokeDasharray="3 3" strokeOpacity={0.3} />
      <text x="100" y="75" textAnchor="middle" className="text-[8px] fill-muted-foreground">Now</text>
      <circle cx="180" cy="30" r="3" fill="hsl(var(--accent))" />
    </svg>
  )
}

function RangeBarsSVG() {
  return (
    <svg viewBox="0 0 200 80" className="w-full h-20 mt-4">
      {/* Range background */}
      <rect x="30" y="25" width="140" height="30" rx="4" fill="hsl(var(--accent))" fillOpacity={0.1} />
      
      {/* P10 */}
      <line x1="50" y1="20" x2="50" y2="60" stroke="currentColor" strokeWidth={2} strokeOpacity={0.3} />
      <text x="50" y="70" textAnchor="middle" className="text-[7px] fill-muted-foreground font-mono">P10</text>
      
      {/* P50 */}
      <line x1="100" y1="20" x2="100" y2="60" stroke="hsl(var(--accent))" strokeWidth={3} />
      <text x="100" y="70" textAnchor="middle" className="text-[7px] fill-accent font-mono font-bold">P50</text>
      
      {/* P90 */}
      <line x1="150" y1="20" x2="150" y2="60" stroke="currentColor" strokeWidth={2} strokeOpacity={0.3} />
      <text x="150" y="70" textAnchor="middle" className="text-[7px] fill-muted-foreground font-mono">P90</text>
      
      {/* Labels */}
      <text x="50" y="15" textAnchor="middle" className="text-[7px] fill-muted-foreground">$268K</text>
      <text x="100" y="15" textAnchor="middle" className="text-[7px] fill-foreground font-semibold">$345K</text>
      <text x="150" y="15" textAnchor="middle" className="text-[7px] fill-muted-foreground">$425K</text>
    </svg>
  )
}

function ParcelGridSVG() {
  const parcels = [
    { x: 10, y: 10, w: 40, h: 30, opacity: 0.6 },
    { x: 55, y: 10, w: 50, h: 30, opacity: 0.3 },
    { x: 110, y: 10, w: 40, h: 30, opacity: 0.8 },
    { x: 155, y: 10, w: 35, h: 30, opacity: 0.2 },
    { x: 10, y: 45, w: 50, h: 30, opacity: 0.4 },
    { x: 65, y: 45, w: 45, h: 30, opacity: 0.7 },
    { x: 115, y: 45, w: 35, h: 30, opacity: 0.15 },
    { x: 155, y: 45, w: 35, h: 30, opacity: 0.5 },
  ]
  
  return (
    <svg viewBox="0 0 200 80" className="w-full h-20 mt-4">
      {parcels.map((p, i) => (
        <rect
          key={i}
          x={p.x}
          y={p.y}
          width={p.w}
          height={p.h}
          rx={2}
          fill="hsl(var(--accent))"
          fillOpacity={p.opacity}
          stroke="hsl(var(--accent))"
          strokeWidth={0.5}
          strokeOpacity={0.3}
        />
      ))}
    </svg>
  )
}

function AttributionSVG() {
  return (
    <svg viewBox="0 0 200 80" className="w-full h-20 mt-4">
      {/* Baseline */}
      <line x1="100" y1="10" x2="100" y2="75" stroke="currentColor" strokeWidth={1} strokeDasharray="2 2" strokeOpacity={0.2} />
      
      {/* Positive factors */}
      <rect x="100" y="12" width="60" height="12" rx="2" fill="hsl(142 70% 45%)" fillOpacity={0.5} />
      <text x="95" y="21" textAnchor="end" className="text-[7px] fill-muted-foreground">Rate cuts</text>
      
      <rect x="100" y="28" width="40" height="12" rx="2" fill="hsl(142 70% 45%)" fillOpacity={0.35} />
      <text x="95" y="37" textAnchor="end" className="text-[7px] fill-muted-foreground">Demand</text>
      
      {/* Negative factor */}
      <rect x="60" y="44" width="40" height="12" rx="2" fill="hsl(0 70% 55%)" fillOpacity={0.4} />
      <text x="95" y="53" textAnchor="end" className="text-[7px] fill-muted-foreground">Supply</text>
      
      {/* Net */}
      <rect x="100" y="60" width="50" height="12" rx="2" fill="hsl(var(--accent))" fillOpacity={0.6} />
      <text x="95" y="69" textAnchor="end" className="text-[7px] fill-accent font-semibold">Net</text>
    </svg>
  )
}

const illustrations = [FutureTrendSVG, RangeBarsSVG, ParcelGridSVG, AttributionSVG]

export function FeatureGrid() {
  return (
    <section className="py-20 md:py-28">
      <div className="max-w-6xl mx-auto px-6">
        <div className="max-w-2xl mb-12">
          <h2 className="text-2xl md:text-3xl font-bold tracking-tight mb-4">
            Property forecasts that actually help you plan.
          </h2>
          <p className="text-muted-foreground leading-relaxed">
            Traditional home valuation tools give you a single number for today. Homecastr gives you a range of outcomes for the future.
          </p>
        </div>

        <div className="grid md:grid-cols-2 gap-6">
          {features.map((feature, index) => {
            const Illustration = illustrations[index]
            return (
              <div
                key={feature.title}
                className="group p-6 rounded-lg border border-border bg-card hover:bg-muted/30 transition-colors"
              >
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-9 h-9 rounded-md bg-muted flex items-center justify-center">
                    <feature.icon className="w-4.5 h-4.5 text-muted-foreground group-hover:text-foreground transition-colors" />
                  </div>
                  <h3 className="font-semibold">{feature.title}</h3>
                </div>
                <p className="text-sm text-muted-foreground leading-relaxed">
                  {feature.description}
                </p>
                <Illustration />
              </div>
            )
          })}
        </div>
      </div>
    </section>
  )
}
