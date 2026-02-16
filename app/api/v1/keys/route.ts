import { NextRequest, NextResponse } from "next/server"
import { randomUUID } from "crypto"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

export async function POST(req: NextRequest) {
    try {
        const body = await req.json()
        const email = body.email

        if (!email || !email.includes("@")) {
            return NextResponse.json({ error: "Valid email is required." }, { status: 400 })
        }

        // Generate a prefixed API key
        const key = `hc_${randomUUID().replace(/-/g, "")}`

        // Persist to Supabase
        const supabase = getSupabaseAdmin()
        const { error } = await supabase.from("api_keys").insert({ email, key })

        if (error) {
            console.error("[API Keys] Insert error:", error)
            return NextResponse.json({ error: "Failed to store API key." }, { status: 500 })
        }

        return NextResponse.json({ key, email })
    } catch (error: any) {
        console.error("[API Keys] Error:", error)
        return NextResponse.json({ error: "Failed to generate key." }, { status: 500 })
    }
}
