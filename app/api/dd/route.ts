import { NextRequest } from "next/server";

export const runtime = "edge";

export async function POST(req: NextRequest) {
  const ddforward = req.nextUrl.searchParams.get("ddforward");

  if (!ddforward) {
    return new Response("Missing ddforward parameter", { status: 400 });
  }

  const site = (process.env.NEXT_PUBLIC_DD_SITE || "datadoghq.com").replace(/\\r/g, "").trim();
  const intakeUrl = `https://browser-intake-${site}${ddforward}`.replace(/ /g, "%20").replace(/\\r/g, "");
  
  const headers = new Headers();
  
  const contentType = req.headers.get('content-type');
  if (contentType) {
    headers.set('content-type', contentType);
  }
  
  const forwardedFor = req.headers.get('x-forwarded-for') || req.headers.get('x-real-ip');
  if (forwardedFor) {
    headers.set('x-forwarded-for', forwardedFor);
  }
  
  const userAgent = req.headers.get('user-agent');
  if (userAgent) {
    headers.set('user-agent', userAgent);
  }

  try {
    const response = await fetch(intakeUrl, {
      method: 'POST',
      headers,
      body: req.body,
      // Pass the Duplex attribute which is often needed for edge streams forwarding
      // @ts-ignore
      duplex: 'half'
    });

    return new Response(response.body, {
      status: response.status,
      headers: response.headers,
    });
  } catch (error) {
    console.error("Datadog Proxy Fetch Error:", error);
    return new Response("Internal Server Error forwarding to Datadog", { status: 500 });
  }
}
