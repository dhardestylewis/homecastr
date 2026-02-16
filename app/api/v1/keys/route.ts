import { NextRequest, NextResponse } from "next/server"
import { randomUUID } from "crypto"

export async function POST(req: NextRequest) {
    try {
        const body = await req.json()
        const email = body.email

        if (!email || !email.includes("@")) {
            return NextResponse.json({ error: "Valid email is required." }, { status: 400 })
        }

        // Generate a prefixed API key
        const key = `hc_${randomUUID().replace(/-/g, "")}`

        // TODO: When ready to enforce auth, store this in Supabase:
        // await supabase.from("api_keys").insert({ email, key, created_at: new Date().toISOString() })

        return NextResponse.json({ key, email })
    } catch (error: any) {
        return NextResponse.json({ error: "Failed to generate key." }, { status: 500 })
    }
}
