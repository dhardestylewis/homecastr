
import { NextResponse } from 'next/server';
import { getH3DataBatch } from '@/app/actions/h3-data-batch';

export async function GET() {
    try {
        // Test batch fetching for a mix of past and future years
        const years = [2024, 2026];
        const batchData = await getH3DataBatch(7, years);

        return NextResponse.json({
            requested_years: years,
            results: Object.keys(batchData).map(y => ({
                year: y,
                count: batchData[Number(y)].length,
                sample: batchData[Number(y)].slice(0, 1)
            }))
        });
    } catch (error) {
        return NextResponse.json({ error: String(error) }, { status: 500 });
    }
}
