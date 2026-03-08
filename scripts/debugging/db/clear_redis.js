require('dotenv').config({ path: '.env.local' });
const { createClient } = require('redis');

async function flushCache() {
    const client = createClient({ url: process.env.REDIS_URL });
    client.on('error', err => console.log('Redis Client Error', err));
    await client.connect();
    await client.flushDb();
    console.log('Flushed Redis DB!');
    await client.quit();
}
flushCache();
