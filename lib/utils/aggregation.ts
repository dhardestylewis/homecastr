
import type { FeatureProperties, DetailsResponse } from "@/lib/types"

/**
 * Aggregates properties from multiple hexes.
 * Uses arithmetic mean for numeric values (value, growth, reliability).
 * Sums counts (n_accts).
 */
export function aggregateProperties(propsList: FeatureProperties[]): FeatureProperties {
    if (propsList.length === 0) throw new Error("Cannot aggregate empty list")
    if (propsList.length === 1) return propsList[0]

    const count = propsList.length
    const first = propsList[0]

    // Initialize sums
    let sumValue = 0
    let sumGrowth = 0
    let sumReliability = 0
    let sumAccts = 0
    let sumLat = 0
    let sumLng = 0

    let validValueCount = 0

    propsList.forEach(p => {
        if (p.med_predicted_value) {
            sumValue += p.med_predicted_value
            validValueCount++
        }
        sumGrowth += p.O
        sumReliability += p.R
        sumAccts += p.n_accts
        // Approx centroid if needed, though ID is main identifier
        // We can't easily valid ID for aggregate, so we might use "agg-id"
    })

    return {
        ...first, // Inherit non-numeric metadata from first
        id: "aggregated-selection",
        med_predicted_value: validValueCount > 0 ? sumValue / validValueCount : 0,
        O: sumGrowth / count,
        R: count > 0 ? sumReliability / count : 0, // Average confidence? Or min? Average seems fair for a group.
        n_accts: sumAccts,
        has_data: true
    }
}

/**
 * Aggregates detailed responses (Fan Charts & History).
 */
export function aggregateDetails(detailsList: DetailsResponse[]): DetailsResponse {
    if (detailsList.length === 0) throw new Error("Cannot aggregate empty details")
    if (detailsList.length === 1) return detailsList[0]

    const count = detailsList.length

    // Initialize sums for metrics
    let sumValue = 0
    let validValueCount = 0
    let sumNOI = 0
    let validNOICount = 0
    let sumOpportunity = 0
    let sumReliability = 0
    let sumAccts = 0

    // Iterate to sum up metrics
    detailsList.forEach(d => {
        // Value
        if (d.proforma?.predicted_value) {
            sumValue += d.proforma.predicted_value
            validValueCount++
        }
        // NOI
        if (d.proforma?.noi) {
            sumNOI += d.proforma.noi
            validNOICount++
        }
        // Opportunity
        if (d.opportunity?.value) {
            sumOpportunity += d.opportunity.value
        }
        // Reliability
        if (d.reliability?.value) {
            sumReliability += d.reliability.value
        }
        // Accounts
        if (d.metrics?.n_accts) {
            sumAccts += d.metrics.n_accts
        }
    })

    // Aggregate Historical Values (Array of numbers)
    const firstHist = detailsList[0].historicalValues
    const aggHistorical = Array.isArray(firstHist) && firstHist.length > 0
        ? firstHist.map((_, idx) => {
            const sum = detailsList.reduce((acc, d) => {
                const val = Array.isArray(d.historicalValues) ? d.historicalValues[idx] : 0
                return acc + (val || 0)
            }, 0)
            return sum / count
        })
        : []

    // Aggregate Fan Chart - output must match FanChartData: {years, p10, p50, p90, y_med}
    const firstFan = detailsList[0].fanChart
    let aggFan: { years: number[], p10: number[], p50: number[], p90: number[], y_med: number[] } | undefined

    if (firstFan && Array.isArray(firstFan.p10) && firstFan.p10.length > 0) {
        const numYears = firstFan.p10.length
        const years = firstFan.years || []
        const p10Agg: number[] = []
        const p50Agg: number[] = []
        const p90Agg: number[] = []
        const yMedAgg: number[] = []

        for (let i = 0; i < numYears; i++) {
            let sumP10 = 0, sumP50 = 0, sumP90 = 0, sumYMed = 0
            let validCount = 0

            detailsList.forEach(d => {
                if (d.fanChart && Array.isArray(d.fanChart.p10)) {
                    sumP10 += d.fanChart.p10[i] || 0
                    sumP50 += d.fanChart.p50[i] || 0
                    sumP90 += d.fanChart.p90[i] || 0
                    sumYMed += d.fanChart.y_med?.[i] || d.fanChart.p50[i] || 0
                    validCount++
                }
            })

            p10Agg.push(validCount > 0 ? sumP10 / validCount : 0)
            p50Agg.push(validCount > 0 ? sumP50 / validCount : 0)
            p90Agg.push(validCount > 0 ? sumP90 / validCount : 0)
            yMedAgg.push(validCount > 0 ? sumYMed / validCount : 0)
        }

        aggFan = { years, p10: p10Agg, p50: p50Agg, p90: p90Agg, y_med: yMedAgg }
    }

    return {
        id: "aggregated-selection",
        locationLabel: "Selected Area (Avg)",
        opportunity: {
            value: sumOpportunity / count,
            unit: detailsList[0].opportunity.unit,
            trend: (sumOpportunity / count) >= 0 ? "up" : "down"
        },
        reliability: {
            value: sumReliability / count,
            components: detailsList[0].reliability.components // Just take first for structure
        },
        metrics: {
            n_accts: sumAccts,
            med_mean_ape_pct: 0,
            med_n_years: 0,
            med_mean_pred_cv_pct: 0
        },
        proforma: {
            predicted_value: validValueCount > 0 ? sumValue / validValueCount : 0,
            noi: validNOICount > 0 ? sumNOI / validNOICount : 0,
            monthly_rent: 0, // Could aggregate if needed
            dscr: 0,
            breakeven_occ: 0,
            cap_rate: 0,
            liquidity_rank: 0
        },
        historicalValues: aggHistorical,
        fanChart: aggFan
    } as DetailsResponse
}

