
// Native fetch is available in Node 18+

const BASE_URL = 'http://localhost:3000/api/validate';
const TEST_IDS = [
    '89446ca9b2fffff', // Known valid
    '8a446ca997a7fff', // Known invalid
    '8a446ca997a7fff', // Known invalid (that caused crash) - Now appears to be working?
];

async function validateH3Id(id) {
    try {
        const res = await fetch(`${BASE_URL}/${id}`);
        if (res.ok) {
            const data = await res.json();
            console.log(`✅ ID ${id}: Found`);
            // Debug the supposedly invalid ID
            if (id === '8a446ca997a7fff') {
                console.log('DEBUG DATA for 8a4:', JSON.stringify(data, null, 2));
            }
            return true;
        } else if (res.status === 404) {
            console.error(`❌ ID ${id}: Not Found (404)`);
            return false;
        } else {
            console.error(`⚠️ ID ${id}: Error ${res.status}`);
            return false;
        }
    } catch (err) {
        console.error(`ERROR Checking ${id}:`, err.message);
        return false;
    }
}

console.log("Starting validation...");

(async () => {
    let successCount = 0;
    for (const id of TEST_IDS) {
        if (await validateH3Id(id)) {
            successCount++;
        }
    }
    console.log(`\nValidation complete. ${successCount}/${TEST_IDS.length} IDs found.`);
})();
