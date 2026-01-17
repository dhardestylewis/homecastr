
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

    // Aggregate Historical Values (Array of numbers)
    // Assume all have same length/years
    const firstHist = detailsList[0].historicalValues
    const aggHistorical = firstHist.map((_, idx) => {
        const sum = detailsList.reduce((acc, d) => acc + (d.historicalValues[idx] || 0), 0)
        return sum / count
    })

    // Aggregate Fan Chart
    // Fan Chart is array of { year, p10, p50, p90 }
    const firstFan = detailsList[0].fanChart
    const aggFan = firstFan.map((point, idx) => {
        let sumP10 = 0
        let sumP50 = 0
        let sumP90 = 0

        detailsList.forEach(d => {
            const dp = d.fanChart[idx]
            sumP10 += dp?.p10 || 0
            sumP50 += dp?.p50 || 0
            sumP90 += dp?.p90 || 0
        })

        return {
            year: point.year,
            p10: sumP10 / count,
            p50: sumP50 / count,
            p90: sumP90 / count
        }
    })

    return {
        h3Id: "aggregated-selection",
        historicalValues: aggHistorical,
        fanChart: aggFan,
        reliability: detailsList[0].reliability // Placeholder
    }
}
