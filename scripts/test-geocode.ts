
async function testGeocode(query: string, suffix: string) {
    const fullQuery = query + suffix;
    const params = new URLSearchParams({
        q: fullQuery,
        format: "json",
        limit: "3",
        addressdetails: "1",
        viewbox: "-95.96,30.17,-94.90,29.50",
        bounded: "1",
    });
    const res = await fetch(`https://nominatim.openstreetmap.org/search?${params}`, {
        headers: { "User-Agent": "HomecastrUI/1.0" },
    });
    const data = await res.json();
    console.log(`Results for "${fullQuery}":`);
    data.forEach((item, i) => {
        console.log(`${i + 1}. ${item.display_name} (${item.lat}, ${item.lon}) type=${item.type} class=${item.class}`);
    });
}

async function run() {
    await testGeocode("Pasadena", ", Houston, TX");
    await testGeocode("Pasadena", ", TX");
    await testGeocode("The Heights", ", Houston, TX");
    await testGeocode("Sugar Land", ", Houston, TX");
    await testGeocode("Sugar Land", ", TX");
}

run();
