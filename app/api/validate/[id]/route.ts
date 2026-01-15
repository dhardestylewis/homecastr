import { NextRequest, NextResponse } from "next/server"
import { getH3CellDetails } from "@/app/actions/h3-details"

export async function GET(
    request: NextRequest,
    { params }: { params: Promise<{ id: string }> }
) {
    const id = (await params).id

    if (!id) {
        return NextResponse.json({ error: "Missing ID" }, { status: 400 })
    }

    try {
        const details = await getH3CellDetails(id, 2026)

        if (!details) {
            return NextResponse.json({ error: "Not Found" }, { status: 404 })
        }

        return NextResponse.json(details)
    } catch (error) {
        return NextResponse.json({ error: "Internal Error" }, { status: 500 })
    }
}
