// TypeScript types for the Real Estate Investment Dashboard
// Aligned with backend/backend/contract.py and backend/backend/types.py

// ============================================================================
// Backend Contract Types (from contract.py)
// ============================================================================

export const FEATURE_COLS = [
  "land_val",
  "bld_val",
  "parcel_age",
  "gross_SF",
  "maint_spend",
  "qa_cd_numeric",
  "total_baths",
  "insurance_proxy",
  "has_pool_xf",
  "has_garage_xf",
  "total_xf_count",
  "avg_xf_grade",
  "neighborhood_cluster",
  "years_since_last_permit",
  "permit_in_last_5yr",
  "yr_scaled",
  "beds_proxy",
  "baths_proxy",
  "condition_proxy",
  "amenity_score",
  "quality_score",
  "permit_recency",
] as const

export type FeatureColumn = (typeof FEATURE_COLS)[number]

export interface FinancialParams {
  vacancy_rate: number // default 0.05
  rent_growth_annual: number // default 0.03
  expense_growth_annual: number // default 0.025
  expense_ratio: number // default 0.35
  ltv_ratio: number // default 0.75
  interest_rate: number // default 0.045
  amortization_period: number // default 30
  exit_cap_rate_spread: number // default 0.005
  disposition_cost_pct: number // default 0.04
  cap_rate: number // default 0.05
  growth_rate: number // default 0.03
}

export const DEFAULT_FINANCIAL_PARAMS: FinancialParams = {
  vacancy_rate: 0.05,
  rent_growth_annual: 0.03,
  expense_growth_annual: 0.025,
  expense_ratio: 0.35,
  ltv_ratio: 0.75,
  interest_rate: 0.045,
  amortization_period: 30,
  exit_cap_rate_spread: 0.005,
  disposition_cost_pct: 0.04,
  cap_rate: 0.05,
  growth_rate: 0.03,
}

export interface ProformaOutput {
  acct: string
  yr: number
  property_type: string
  neighborhood_cluster: number
  gross_sf: number
  predicted_value: number
  noi: number
  monthly_rent: number
  loan_amount: number
  annual_debt_service: number
  dscr: number
  breakeven_occ: number
  cap_rate: number
  qa_cd_numeric: number
  has_pool_xf: number
  has_garage_xf: number
  years_since_last_permit: number
  permit_in_last_5yr: number
  amenity_score: number
  quality_score: number
  permit_recency: number
  liquidity_rank: number
}

export interface RiskScoredOutput extends ProformaOutput {
  medAE: number
  tail_gap: number
  tail_gap_z: number
  medAE_z: number
  inv_dscr: number
  inv_dscr_z: number
  R: number // Composite risk score
  alert_triggered: boolean
  score: number // Final investment score
  hard_fail?: boolean
}

// ============================================================================
// API Request/Response Types (from api.py)
// ============================================================================

export interface PredictRow {
  acct: string
  yr: number
  land_val?: number
  bld_val?: number
  gross_SF?: number
  parcel_age?: number
  neighborhood_cluster?: number
  [key: string]: string | number | undefined
}

export interface PredictRequest {
  rows: PredictRow[]
  include_proforma: boolean
  include_risk: boolean
}

export interface PredictResponse {
  n: number
  rows: RiskScoredOutput[]
}

export interface ScoreLatestYearRequest {
  include_risk: boolean
  diversify_portfolio: boolean
  target_sale: number
  target_rent: number
  constraints: Record<string, number>
}

export interface ScoreLatestYearResponse {
  latest_year: number
  n: number
  sale_n?: number
  rent_n?: number
  rows?: RiskScoredOutput[]
  sale_units?: RiskScoredOutput[]
  rent_units?: RiskScoredOutput[]
}

// ============================================================================
// Manifest Schema (tile layer configuration)
// ============================================================================

export interface Manifest {
  tileSources: TileSource[]
  parcelSource?: TileSource
  fields: FieldConfig
  thresholds: Thresholds
  fanChartAvailable: boolean
  defaultCenter: [number, number]
  defaultZoom: number
  financialParams?: FinancialParams
}

export interface TileSource {
  resolution: number
  url: string
  minZoom: number
  maxZoom: number
  type: "h3" | "parcel"
}

export interface FieldConfig {
  opportunity: {
    field: string
    label: string
    unit: string
    format: "percent" | "currency" | "number"
  }
  reliability: {
    field: string
    label: string
    min: number
    max: number
  }
}

export interface Thresholds {
  stabilityWarning: number
  robustnessWarning: number
  minSupport: number
  reliabilityBins: number[]
  alertThreshold: number // default 3.0
  dscrMin: number // default 1.05
  breakevenMax: number // default 0.92
  permitAgeMax: number // default 30
  qualityMinForOldPermit: number // default 2
}

// ============================================================================
// Feature Properties (from H3/parcel tiles)
// ============================================================================

export interface FeatureProperties {
  id: string
  O: number // Opportunity score (predicted CAGR or IRR)
  R: number // Reliability score (0-1)
  n_accts: number
  med_mean_ape_pct: number
  med_mean_pred_cv_pct: number
  stability_flag: boolean
  robustness_flag: boolean
  has_data: boolean // New field: true if property_count > 0, false for empty coverage cells
  med_n_years?: number
  med_predicted_value?: number
  med_noi?: number
  med_dscr?: number
  med_score?: number
}

// ============================================================================
// Details Response (Inspector Drawer)
// ============================================================================

export interface DetailsResponse {
  id: string
  locationLabel: string
  opportunity: {
    value: number | null
    unit: string
    trend?: "up" | "down" | "stable"
  }
  reliability: {
    value: number
    components: ReliabilityComponents
  }
  metrics: {
    n_accts: number
    med_n_years: number | null  // v6: not computed
    med_mean_ape_pct: number | null
    med_mean_pred_cv_pct: number | null  // v6: not computed
  }
  proforma?: {
    predicted_value: number | null
    noi: number | null
    monthly_rent: number | null
    dscr: number | null
    breakeven_occ: number | null
    cap_rate: number | null
    liquidity_rank: number | null  // v6: not computed
  }
  riskScoring?: {
    R: number | null
    tail_gap_z: number | null  // v6: not stored
    medAE_z: number | null  // v6: not stored
    inv_dscr_z: number | null  // v6: not stored
    alert_triggered: boolean
    score: number | null
  }
  stressTests?: StressTests
  fanChart?: FanChartData
}

export interface ReliabilityComponents {
  accuracy_term: number | null
  confidence_term: number | null
  stability_term: number | null
  robustness_term: number | null  // v6: always null
  support_term: number | null
}

export interface StressTests {
  horizon_degradation: StressTestResult
  fold_dispersion: StressTestResult
  drift_score: StressTestResult
  ablation_impact: StressTestResult
}

export interface StressTestResult {
  value: number
  threshold: number
  status: "pass" | "warn" | "fail"
}

export interface FanChartData {
  years: number[]
  p10: number[]
  p50: number[]
  p90: number[]
  y_med: number[]
}

// ============================================================================
// Filter State
// ============================================================================

export interface FilterState {
  reliabilityMin: number
  nAcctsMin: number
  medNYearsMin: number
  showUnderperformers: boolean
  highlightWarnings: boolean
  layerOverride?: number
  dscrMin?: number
  scoreMin?: number
  excludeAlerts?: boolean
  excludeHardFails?: boolean
}

// ============================================================================
// Map State & URL Params
// ============================================================================

export interface MapState {
  center: [number, number]
  zoom: number
  selectedId: string | null
  hoveredId: string | null
}

export interface QueryParams {
  id?: string
  rMin?: string
  nMin?: string
  yMin?: string
  underperf?: string
  warnings?: string
  layer?: string
  lat?: string
  lng?: string
  zoom?: string
  dscrMin?: string
  scoreMin?: string
  noAlerts?: string
}

// ============================================================================
// Portfolio Types (from portfolio.py)
// ============================================================================

export interface DiversifyConstraints {
  neighborhood_cluster?: number // default 40
  property_type?: number // default 60
}

export interface PortfolioResult {
  sale_units: RiskScoredOutput[]
  rent_units: RiskScoredOutput[]
  unit_matrix: RiskScoredOutput[]
}
