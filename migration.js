const { Client } = require('pg');

// User provided connection string
const connectionString = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:6543/postgres?sslmode=require&supa=base-pooler.x";

const client = new Client({
  connectionString: connectionString,
});

async function runMigration() {
  try {
    console.log("Connecting to Supabase...");
    await client.connect();
    console.log("Connected.");

    const sql = `
      -- Create function to fetch compact H3 grid data for client-side rendering
      create or replace function get_h3_compact_grid(
        min_lat float,
        min_lng float,
        max_lat float,
        max_lng float,
        resolution int,
        year int
      )
      returns table (
        h3_id text,
        o float,
        r float
      )
      language plpgsql
      as $$
      begin
        return query
        select
          h.h3_id,
          h.opportunity,
          h.reliability
        from
          h3_precomputed_hex_details h
        where
          h.forecast_year = year
          and h.h3_res = resolution
          and h.lat >= min_lat
          and h.lat <= max_lat
          and h.lng >= min_lng
          and h.lng <= max_lng;
      end;
      $$;
    `;

    console.log("Running SQL...");
    await client.query(sql);
    console.log("Successfully created function: get_h3_compact_grid");

  } catch (err) {
    console.error("Migration failed:", err);
  } finally {
    await client.end();
  }
}

runMigration();
