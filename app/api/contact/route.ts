import { NextRequest, NextResponse } from "next/server"
import { getSupabaseAdmin } from "@/lib/supabase/admin"
import { Resend } from "resend"

const resend = process.env.RESEND_API_KEY
    ? new Resend(process.env.RESEND_API_KEY)
    : null

const DANIEL_EMAIL = "daniel@homecastr.com"

export async function POST(req: NextRequest) {
    try {
        const { name, email, message } = await req.json()

        if (!name?.trim() && !email?.trim()) {
            return NextResponse.json(
                { error: "Please provide at least a name or email." },
                { status: 400 }
            )
        }

        // 1. Store in Supabase (always works, even without Resend)
        try {
            const sb = getSupabaseAdmin()
            await sb.from("contact_submissions").insert({
                name: name?.trim() || null,
                email: email?.trim() || null,
                message: message?.trim() || null,
                created_at: new Date().toISOString(),
            })
        } catch (dbErr) {
            // Table might not exist yet — log but don't fail the request
            console.warn("[contact] Supabase insert failed (table may not exist):", dbErr)
        }

        // 2. Send notification email to Daniel
        if (resend) {
            await resend.emails.send({
                from: "Homecastr <notifications@homecastr.com>",
                replyTo: email?.trim() || undefined,
                to: DANIEL_EMAIL,
                subject: `Homecastr — Custom Forecast Analysis${name ? ` (${name})` : ""}`,
                html: `
          <p>${name || "Someone"} reached out via <a href="https://homecastr.com">homecastr.com</a>:</p>
          ${message ? `<blockquote style="border-left: 3px solid #ccc; padding-left: 12px; color: #555;">${message}</blockquote>` : `<p style="color: #888;"><em>No message provided.</em></p>`}
          <p style="color: #888; font-size: 12px; margin-top: 16px;">${name || "Visitor"}${email ? ` · ${email}` : ""}</p>
        `,
            })

            // 3. Send confirmation to the sender (if they provided an email)
            if (email?.trim()) {
                await resend.emails.send({
                    from: "Homecastr <notifications@homecastr.com>",
                    to: email.trim(),
                    subject: "We received your inquiry — Homecastr",
                    html: `
            <h2>Thanks for reaching out!</h2>
            <p>Hi${name ? ` ${name}` : ""},</p>
            <p>We received your message and will get back to you within 24 hours.</p>
            ${message ? `<p><strong>Your message:</strong></p><blockquote>${message}</blockquote>` : ""}
            <p>In the meantime, feel free to explore forecasts at <a href="https://homecastr.com">homecastr.com</a>.</p>
            <br />
            <p>Best,<br />Daniel Hardesty Lewis<br />Founder, Homecastr</p>
          `,
                })
            }
        }

        return NextResponse.json({ success: true })
    } catch (err) {
        console.error("[contact] Error:", err)
        return NextResponse.json(
            { error: "Failed to send message. Please try again." },
            { status: 500 }
        )
    }
}
