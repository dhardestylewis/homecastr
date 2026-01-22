
import { NextResponse } from "next/server"
import { Pool } from "pg"

export async function GET() {
    const databaseUrl = process.env.DATABASE_URL
    if (!databaseUrl) {
        return NextResponse.json({ error: "DATABASE_URL not set" }, { status: 500 })
    }

    const pool = new Pool({ connectionString: databaseUrl })

    try {
        const client = await pool.connect()
        try {
            // Test simple query
            const res = await client.query("SELECT PostGIS_Full_Version()")
            return NextResponse.json({ version: res.rows[0] })

            // If that works, test the MVT query with dummy values
            // ...
        } finally {
            client.release()
            await pool.end()
        }
    } catch (e: any) {
        return NextResponse.json({ error: e.message, stack: e.stack }, { status: 500 })
    }
}
