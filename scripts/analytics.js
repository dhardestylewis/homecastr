#!/usr/bin/env node
require('dotenv').config({ path: '.env.local' });

/**
 * Run this script to generate an instant snapshot of 7-day usage telemetry from PostHog.
 * Usage: node scripts/analytics.js
 */

async function queryPostHog() {
    const key = process.env.POSTHOG_PERSONAL_API_KEY;
    const projectId = '308631';

    if (!key) {
        console.error("❌ POSTHOG_PERSONAL_API_KEY is missing from .env.local");
        process.exit(1);
    }

    // Helper to query HogQL
    const req = async (queryText) => {
        const res = await fetch(`https://us.posthog.com/api/projects/${projectId}/query/`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ query: { kind: "HogQLQuery", query: queryText } })
        });
        if (!res.ok) {
            console.error("API Error:", res.status, await res.text());
            process.exit(1);
        }
        return res.json();
    };

    console.log("\n=======================================================");
    console.log("   📈 Homecastr Telemetry Snapshot (Last 7 Days)       ");
    console.log("=======================================================\n");

    // 1. Time Series: Pageviews by Day
    console.log("📅 PAGEVIEWS BY DAY");
    console.log("-----------------------");
    const tsData = await req("select toDate(timestamp) as day, count() from events where event = '$pageview' and timestamp > now() - interval 7 day group by day order by day asc");
    if (tsData.results) {
        tsData.results.forEach(r => console.log(`${r[0]}: ${r[1].toString().padStart(4)} views`));
    }

    // 2. Engagement: Unique users and average sessions
    console.log("\n👥 AUDIENCE ENGAGEMENT");
    console.log("-----------------------");
    const uniqueUsers = await req("select count(distinct person_id) from events where event = '$pageview' and timestamp > now() - interval 7 day");
    const totalViews = await req("select count() from events where event = '$pageview' and timestamp > now() - interval 7 day");
    
    if (uniqueUsers.results && totalViews.results) {
        const uu = uniqueUsers.results[0][0];
        const pv = totalViews.results[0][0];
        console.log(`Unique Users (7d):    ${uu}`);
        console.log(`Total Pageviews (7d): ${pv}`);
        console.log(`Avg Views per User:   ${(pv / (uu || 1)).toFixed(1)}`);
    }

    // 3. Top Pages/Paths
    console.log("\n🔥 TOP PAGES & PATHS");
    console.log("-----------------------");
    const pathsData = await req("select properties.$pathname, count() as c from events where event = '$pageview' and timestamp > now() - interval 7 day group by properties.$pathname order by c desc limit 7");
    if (pathsData.results) {
        pathsData.results.forEach(r => console.log(`${r[1].toString().padStart(4)} - ${r[0]}`));
    }

    // 4. Top Referrers
    console.log("\n🔗 TOP TRAFFIC SOURCES");
    console.log("-----------------------");
    const refData = await req("select properties.$referring_domain, count() as c from events where event = '$pageview' and timestamp > now() - interval 7 day group by properties.$referring_domain order by c desc limit 5");
    if (refData.results) {
        refData.results.forEach(r => {
            const source = r[0] ? r[0] : (r[1] > 100 ? "Direct / Bookmarked" : "Direct / Other");
            console.log(`${r[1].toString().padStart(4)} - ${source}`);
        });
    }

    // 5. User Devices (Mobile vs Desktop)
    console.log("\n📱 DEVICE TYPES");
    console.log("-----------------------");
    const deviceData = await req("select properties.$device_type, count() as c from events where event = '$pageview' and timestamp > now() - interval 7 day group by properties.$device_type order by c desc limit 3");
    if (deviceData.results) {
        deviceData.results.forEach(r => console.log(`${r[1].toString().padStart(4)} - ${r[0] || 'Unknown'}`));
    }
    
    console.log("\n=======================================================\n");
}

queryPostHog().catch(console.error);
