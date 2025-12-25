import type { FeatureProperties, DetailsResponse, RiskScoredOutput, FinancialParams } from "./types"

// Property types from backend
const PROPERTY_TYPES = [
  "Free Standing Home - 3+BD",
  "Free Standing Home - 1-2BD",
  "Attached Home/Townhome - 1-2BD",
  "Attached Home/Townhome - 3+BD",
  "Unknown",
]

// Quality codes from backend
const QA_CODES = ["A+", "A", "B", "C", "D", "E", "F"]
const QA_NUMERIC: Record<string, number> = { "A+": 6, A: 5, B: 4, C: 3, D: 2, E: 1, F: 0 }

// Default financial parameters
const DEFAULT_FINANCIAL_PARAMS: FinancialParams = {
  cap_rate: 0.05,
  expense_ratio: 0.3,
  ltv_ratio: 0.7,
  interest_rate: 0.04,
  amortization_period: 30,
}

function computeProforma(
  predictedValue: number,
  grossSf: number,
  fin: FinancialParams = { ...DEFAULT_FINANCIAL_PARAMS },
): Partial<RiskScoredOutput> {
  const noi = predictedValue * fin.cap_rate
  const expenseRatio = fin.expense_ratio
  const potentialIncome = noi / (1 - expenseRatio)
  const monthlyRent = potentialIncome / 12

  const loanAmount = predictedValue * fin.ltv_ratio
  const r = fin.interest_rate / 12
  const n = fin.amortization_period * 12
  const annualDebtService =
    loanAmount > 0 ? ((loanAmount * (r * Math.pow(1 + r, n))) / (Math.pow(1 + r, n) - 1)) * 12 : 0
  const dscr = annualDebtService > 0 ? noi / annualDebtService : Number.POSITIVE_INFINITY
  const breakevenOcc =
    potentialIncome > 0 ? (potentialIncome * expenseRatio + annualDebtService) / potentialIncome : Number.NaN

  return {
    predicted_value: predictedValue,
    noi,
    monthly_rent: monthlyRent,
    loan_amount: loanAmount,
    annual_debt_service: annualDebtService,
    dscr,
    breakeven_occ: breakevenOcc,
    cap_rate: fin.cap_rate,
    gross_sf: grossSf,
  }
}

function computeRiskScoring(
  proforma: Partial<RiskScoredOutput>,
  medAE: number,
  tailGap: number,
  amenityScore: number,
  qualityScore: number,
  permitRecency: number,
): Partial<RiskScoredOutput> {
  // Simplified z-score (using MAD-like normalization)
  const tailGapZ = tailGap / 50000 // Normalized by typical spread
  const medAEZ = (medAE - 15000) / 10000
  const invDscr = 1 / Math.max(0.01, proforma.dscr || 1)
  const invDscrZ = (invDscr - 0.8) / 0.3

  const wTail = 1.0
  const wMed = 1.2
  const wInv = 1.0

  const R = tailGapZ * wTail + medAEZ * wMed + invDscrZ * wInv
  const alertTriggered = Math.abs(R) > 3.0

  const score =
    Math.min(5, proforma.dscr || 0) * 2.0 - R * 5.0 + amenityScore * 0.15 + qualityScore * 0.1 + permitRecency * 0.1

  return {
    medAE,
    tail_gap: tailGap,
    tail_gap_z: tailGapZ,
    medAE_z: medAEZ,
    inv_dscr: invDscr,
    inv_dscr_z: invDscrZ,
    R,
    alert_triggered: alertTriggered,
    score,
  }
}

export function generateMockRiskScoredOutput(count = 50): RiskScoredOutput[] {
  const results: RiskScoredOutput[] = []

  for (let i = 0; i < count; i++) {
    const acct = `ACCT-${(10000000 + i).toString()}`
    const yr = 2024
    const propertyType = PROPERTY_TYPES[Math.floor(Math.random() * PROPERTY_TYPES.length)]
    const neighborhoodCluster = Math.floor(Math.random() * 50)
    const grossSf = Math.floor(Math.random() * 2500) + 800
    const qaCd = QA_CODES[Math.floor(Math.random() * QA_CODES.length)]
    const qaCdNumeric = QA_NUMERIC[qaCd]

    const predictedValue = Math.floor(Math.random() * 400000) + 100000
    const proforma = computeProforma(predictedValue, grossSf)

    const amenityScore = Math.random() * 10
    const qualityScore = Math.random() * 10
    const permitRecency = Math.random() * 5
    const medAE = Math.random() * 30000 + 5000
    const tailGap = (Math.random() - 0.5) * 100000

    const riskScoring = computeRiskScoring(proforma, medAE, tailGap, amenityScore, qualityScore, permitRecency)

    const yearsLastPermit = Math.floor(Math.random() * 40)
    const hardFail =
      (proforma.dscr || 0) < 1.05 || (proforma.breakeven_occ || 1) > 0.92 || (yearsLastPermit > 30 && qaCdNumeric < 2)

    results.push({
      acct,
      yr,
      property_type: propertyType,
      neighborhood_cluster: neighborhoodCluster,
      gross_sf: grossSf,
      predicted_value: predictedValue,
      noi: proforma.noi || 0,
      monthly_rent: proforma.monthly_rent || 0,
      loan_amount: proforma.loan_amount || 0,
      annual_debt_service: proforma.annual_debt_service || 0,
      dscr: proforma.dscr || 0,
      breakeven_occ: proforma.breakeven_occ || 0,
      cap_rate: proforma.cap_rate || 0.05,
      qa_cd_numeric: qaCdNumeric,
      has_pool_xf: Math.random() < 0.2 ? 1 : 0,
      has_garage_xf: Math.random() < 0.7 ? 1 : 0,
      years_since_last_permit: yearsLastPermit,
      permit_in_last_5yr: yearsLastPermit <= 5 ? 1 : 0,
      amenity_score: amenityScore,
      quality_score: qualityScore,
      permit_recency: permitRecency,
      liquidity_rank: Math.random() * 100,
      medAE: riskScoring.medAE || 0,
      tail_gap: riskScoring.tail_gap || 0,
      tail_gap_z: riskScoring.tail_gap_z || 0,
      medAE_z: riskScoring.medAE_z || 0,
      inv_dscr: riskScoring.inv_dscr || 0,
      inv_dscr_z: riskScoring.inv_dscr_z || 0,
      R: riskScoring.R || 0,
      alert_triggered: riskScoring.alert_triggered || false,
      score: riskScoring.score || 0,
      hard_fail: hardFail,
    })
  }

  return results
}

// Generate mock H3 features for development (aggregated tile data)
export function generateMockFeatures(count = 50): FeatureProperties[] {
  const features: FeatureProperties[] = []

  for (let i = 0; i < count; i++) {
    const O = Math.random() * 20 - 5 // -5% to 15% CAGR
    const R = Math.random() * 0.8 + 0.2 // 0.2 to 1.0 reliability
    const nAccts = Math.floor(Math.random() * 500) + 10

    const medPredictedValue = Math.floor(Math.random() * 300000) + 150000
    const medNoi = medPredictedValue * 0.05
    const medDscr = 1.0 + Math.random() * 0.8

    features.push({
      id: `mock-h3-${i.toString(16).padStart(15, "0")}`,
      O: Math.round(O * 10) / 10,
      R: Math.round(R * 100) / 100,
      n_accts: nAccts,
      med_mean_ape_pct: Math.round(Math.random() * 15 * 10) / 10,
      med_mean_pred_cv_pct: Math.round(Math.random() * 20 * 10) / 10,
      stability_flag: Math.random() < 0.15,
      robustness_flag: Math.random() < 0.1,
      med_n_years: Math.round((Math.random() * 8 + 2) * 10) / 10,
      med_predicted_value: medPredictedValue,
      med_noi: Math.round(medNoi),
      med_dscr: Math.round(medDscr * 100) / 100,
      med_score: Math.round((Math.random() * 10 + 2) * 10) / 10,
    })
  }

  return features
}

// Mock GeoJSON for development (US-centered hexagons)
export function generateMockGeoJSON() {
  const features = generateMockFeatures(100)
  const baseLatLng: [number, number] = [-97.7431, 30.2672] // Austin, TX

  return {
    type: "FeatureCollection" as const,
    features: features.map((props, i) => ({
      type: "Feature" as const,
      properties: props,
      geometry: {
        type: "Polygon" as const,
        coordinates: [generateHexCoords(baseLatLng, i)],
      },
    })),
  }
}

function generateHexCoords(center: [number, number], index: number): [number, number][] {
  const row = Math.floor(index / 10)
  const col = index % 10
  const size = 0.02
  const offsetX = col * size * 1.5
  const offsetY = row * size * 1.732 + (col % 2 ? size * 0.866 : 0)

  const cx = center[0] + offsetX
  const cy = center[1] + offsetY

  const coords: [number, number][] = []
  for (let angle = 0; angle < 360; angle += 60) {
    const rad = (angle * Math.PI) / 180
    coords.push([cx + Math.cos(rad) * size * 0.5, cy + Math.sin(rad) * size * 0.5])
  }
  coords.push(coords[0])

  return coords
}

export function generateMockDetailsResponse(id: string): DetailsResponse {
  const mockOutput = generateMockRiskScoredOutput(1)[0]

  return {
    id,
    locationLabel: `Neighborhood Cluster ${mockOutput.neighborhood_cluster}`,
    opportunity: {
      value: Math.round((mockOutput.score / 10) * 100) / 100,
      unit: "score",
      trend: mockOutput.score > 7 ? "up" : mockOutput.score < 4 ? "down" : "stable",
    },
    reliability: {
      value: Math.max(0, Math.min(1, 1 - Math.abs(mockOutput.R) / 10)),
      components: {
        accuracy_term: 0.25 - mockOutput.medAE_z * 0.05,
        confidence_term: 0.2,
        stability_term: mockOutput.tail_gap_z < 1 ? 0.2 : 0.1,
        robustness_term: 0.2,
        support_term: 0.15,
      },
    },
    metrics: {
      n_accts: Math.floor(Math.random() * 100) + 10,
      med_n_years: Math.round((Math.random() * 6 + 3) * 10) / 10,
      med_mean_ape_pct: Math.round((mockOutput.medAE / mockOutput.predicted_value) * 100 * 10) / 10,
      med_mean_pred_cv_pct: Math.round(Math.random() * 15 * 10) / 10,
    },
    proforma: {
      predicted_value: mockOutput.predicted_value,
      noi: mockOutput.noi,
      monthly_rent: mockOutput.monthly_rent,
      dscr: mockOutput.dscr,
      breakeven_occ: mockOutput.breakeven_occ,
      cap_rate: mockOutput.cap_rate,
      liquidity_rank: mockOutput.liquidity_rank,
    },
    riskScoring: {
      R: mockOutput.R,
      tail_gap_z: mockOutput.tail_gap_z,
      medAE_z: mockOutput.medAE_z,
      inv_dscr_z: mockOutput.inv_dscr_z,
      alert_triggered: mockOutput.alert_triggered,
      score: mockOutput.score,
    },
    fanChart: {
      years: [2024, 2025, 2026, 2027, 2028],
      p10: [
        mockOutput.predicted_value * 0.9,
        mockOutput.predicted_value * 0.88,
        mockOutput.predicted_value * 0.85,
        mockOutput.predicted_value * 0.82,
        mockOutput.predicted_value * 0.78,
      ],
      p50: [
        mockOutput.predicted_value,
        mockOutput.predicted_value * 1.03,
        mockOutput.predicted_value * 1.06,
        mockOutput.predicted_value * 1.09,
        mockOutput.predicted_value * 1.12,
      ],
      p90: [
        mockOutput.predicted_value * 1.1,
        mockOutput.predicted_value * 1.15,
        mockOutput.predicted_value * 1.22,
        mockOutput.predicted_value * 1.3,
        mockOutput.predicted_value * 1.4,
      ],
      y_med: [
        mockOutput.predicted_value,
        mockOutput.predicted_value * 1.03,
        mockOutput.predicted_value * 1.06,
        mockOutput.predicted_value * 1.09,
        mockOutput.predicted_value * 1.12,
      ],
    },
    stressTests: {
      horizon_degradation: {
        value: Math.random() * 0.15,
        threshold: 0.2,
        status: Math.random() < 0.8 ? "pass" : "warn",
      },
      fold_dispersion: { value: Math.random() * 0.1, threshold: 0.15, status: Math.random() < 0.85 ? "pass" : "warn" },
      drift_score: { value: Math.random() * 0.08, threshold: 0.1, status: Math.random() < 0.9 ? "pass" : "warn" },
      ablation_impact: { value: Math.random() * 0.12, threshold: 0.15, status: Math.random() < 0.85 ? "pass" : "fail" },
    },
  }
}
