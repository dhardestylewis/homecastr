import { NextResponse } from "next/server"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

export const dynamic = "force-dynamic"

/**
 * Cron job to keep the Vercel serverless function warm and maintain
 * an active database connection pool.
 *
 * Configured in vercel.json to run every 5 minutes.
 */
export async function GET(request: Request) {
    // Verify the request comes from Vercel Cron
    const authHeader = request.headers.get("authorization")
    if (authHeader !== \`Bearer \${process.env.CRON_SECRET}\`) {
        // Return 401, but don't leak whether the secret is set
        return new NextResponse("Unauthorized", { status: 401 })
    }

    try {
        const supabase = getSupabaseAdmin()

        // Execute a fast, trivial query to keep the DB connection alive
        const { error } = await supabase.rpc("ping") // Or a simple table query if no ping RPC

        if (error && error.code !== "42883") { // Ignore "function does not exist"
             // Fallback to a simple table access to ensure the connection works
             await supabase.from("metrics_state_forecast").select("origin_year").limit(1)
        }

        return NextResponse.json({
            status: "ok",
            message: "Keepalive successful",
            timestamp: new Date().toISOString(),
        })
    } catch (e: any) {
        console.error("[CRON-KEEPALIVE] Error:", e)
        return NextResponse.json({ error: e.message }, { status: 500 })
    }
}
