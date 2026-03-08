"""
Migrate MVT functions from forecast_20260220_7f31c6e4 → forecast_queue.

Creates all required MVT tile-rendering functions in the forecast_queue schema
so the frontend can serve tiles from forecast_queue directly.

One-time migration script. Safe to re-run (CREATE OR REPLACE).
"""
import psycopg2

CONN_STR = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require&options=-c%20statement_timeout%3D600000"
SCHEMA = "forecast_queue"

def main():
    conn = psycopg2.connect(CONN_STR)
    conn.autocommit = True
    cur = conn.cursor()

    print(f"Migrating MVT functions to schema: {SCHEMA}")

    # ─── Drop conflicting functions from prior runs ───
    print("  Dropping any conflicting function signatures...")
    drop_fns = [
        "mvt_choropleth_forecast",
        "mvt_choropleth_history",
        "mvt_zip3_choropleth",
        "mvt_zcta_choropleth",
        "mvt_tract_choropleth",
        "mvt_tabblock_choropleth",
        "mvt_neighborhood_choropleth",
        "mvt_unsd_choropleth",
        "mvt_parcel_choropleth",
    ]
    for fn in drop_fns:
        # Cascade drop all overloads of this function name
        cur.execute(f"""
            DO $$
            DECLARE r record;
            BEGIN
              FOR r IN
                SELECT p.oid::regprocedure::text AS sig
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = '{SCHEMA}' AND p.proname = '{fn}'
              LOOP
                EXECUTE 'DROP FUNCTION IF EXISTS ' || r.sig || ' CASCADE';
              END LOOP;
            END $$;
        """)
    print("  Done — cleared conflicting signatures")

    # ─── 0) Helper: _pick_geom_table ───
    print("  Creating _pick_geom_table...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}._pick_geom_table(p_preferred text, p_fallback text)
        RETURNS text
        LANGUAGE plpgsql STABLE
        AS $$
        DECLARE
          v_has boolean := false;
          v_sql text;
        BEGIN
          IF to_regclass(p_preferred) IS NOT NULL THEN
            v_sql := format('SELECT EXISTS (SELECT 1 FROM %s LIMIT 1)', p_preferred);
            EXECUTE v_sql INTO v_has;
            IF coalesce(v_has, false) THEN
              RETURN p_preferred;
            END IF;
          END IF;
          RETURN p_fallback;
        END;
        $$;
    """)

    # ─── 1) Generic MVT builder: _mvt_forecast_generic ───
    print("  Creating _mvt_forecast_generic...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}._mvt_forecast_generic(
          p_layer_name       text,
          p_geom_fqtn        text,
          p_geom_key_col     text,
          p_metrics_fqtn     text,
          p_metrics_key_col  text,
          z                  integer,
          x                  integer,
          y                  integer,
          p_origin_year      integer,
          p_horizon_m        integer,
          p_series_kind      text default 'forecast',
          p_variant_id       text default '__forecast__',
          p_run_id           text default null,
          p_backtest_id      text default null,
          p_limit            integer default null
        )
        RETURNS bytea
        LANGUAGE plpgsql STABLE
        AS $$
        DECLARE
          v_sql text;
          v_mvt bytea;
          v_limit_sql text := '';
          v_hist_fqtn text;
        BEGIN
          IF p_limit IS NOT NULL AND p_limit > 0 THEN
            v_limit_sql := format(' limit %s', p_limit);
          END IF;

          v_hist_fqtn := replace(p_metrics_fqtn, '_forecast', '_history');

          IF p_horizon_m <= 0 THEN
            -- HISTORICAL MODE
            PERFORM 1 FROM pg_catalog.pg_class c
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              WHERE (n.nspname || '.' || c.relname) = v_hist_fqtn;
            IF NOT FOUND THEN RETURN ''::bytea; END IF;

            v_sql := format($fmt$
              WITH bounds AS (
                SELECT ST_TileEnvelope($1,$2,$3) AS b3857,
                       ST_Transform(ST_TileEnvelope($1,$2,$3),4326) AS b4326
              ), src AS (
                SELECT g.%1$I::text AS id, $4 AS origin_year, $5 AS horizon_m,
                  ($4+($5/12))::integer AS forecast_year,
                  coalesce(h_past.p50,h_past.value) AS value,
                  null::double precision AS p10, null::double precision AS p25,
                  coalesce(h_past.p50,h_past.value) AS p50,
                  null::double precision AS p75, null::double precision AS p90,
                  null::bigint AS n,
                  least(100, greatest(-50,
                    round((100.0*(coalesce(f_now.p50,f_now.value)-coalesce(h_past.p50,h_past.value))
                      /nullif(coalesce(h_past.p50,h_past.value),0))::numeric,1)
                  )) AS growth_pct,
                  'historical'::text AS series_kind, null::text AS variant_id,
                  null::text AS run_id, null::text AS backtest_id,
                  null::text AS model_version, null::date AS as_of_date,
                  null::integer AS n_scenarios, false AS is_backtest,
                  ST_AsMVTGeom(ST_Transform(g.geom,3857),bounds.b3857,4096,256,true) AS geom
                FROM %2$s g
                JOIN %3$s h_past ON h_past.%4$I=g.%1$I
                  AND h_past.year=($4+($5/12))::integer
                LEFT JOIN %5$s f_now ON f_now.%4$I=g.%1$I
                  AND f_now.origin_year=$4 AND f_now.horizon_m=12
                  AND f_now.series_kind=$6 AND f_now.variant_id=$7
                CROSS JOIN bounds
                WHERE g.geom && bounds.b4326 AND ST_Intersects(g.geom,bounds.b4326)
                %6$s
              ) SELECT ST_AsMVT(src,%7$L,4096,'geom') FROM src
            $fmt$,
              p_geom_key_col,
              p_geom_fqtn,
              v_hist_fqtn,
              p_metrics_key_col,
              p_metrics_fqtn,
              v_limit_sql,
              p_layer_name
            );
          ELSE
            -- FORECAST MODE
            v_sql := format($fmt$
              WITH bounds AS (
                SELECT ST_TileEnvelope($1,$2,$3) AS b3857,
                       ST_Transform(ST_TileEnvelope($1,$2,$3),4326) AS b4326
              ), src AS (
                SELECT g.%1$I::text AS id, m.origin_year, m.horizon_m,
                  coalesce(m.forecast_year,m.origin_year+((m.horizon_m+11)/12))::integer AS forecast_year,
                  m.value, m.p10, m.p25, coalesce(m.p50,m.value) AS p50, m.p75, m.p90, m.n,
                  least(100, greatest(-50,
                    round((100.0*(coalesce(m.p50,m.value)-coalesce(f_now.p50,f_now.value))
                      /nullif(coalesce(f_now.p50,f_now.value),0))::numeric,1)
                  )) AS growth_pct,
                  m.series_kind, m.variant_id, m.run_id, m.backtest_id,
                  m.model_version, m.as_of_date, m.n_scenarios, m.is_backtest,
                  ST_AsMVTGeom(ST_Transform(g.geom,3857),bounds.b3857,4096,256,true) AS geom
                FROM %2$s g
                JOIN %3$s m ON m.%4$I=g.%1$I
                LEFT JOIN %3$s f_now ON f_now.%4$I=g.%1$I
                  AND f_now.origin_year=$4 AND f_now.horizon_m=12
                  AND f_now.series_kind=$6 AND f_now.variant_id=$7
                CROSS JOIN bounds
                WHERE g.geom && bounds.b4326 AND ST_Intersects(g.geom,bounds.b4326)
                  AND m.origin_year=$4 AND m.horizon_m=$5
                  AND m.series_kind=$6 AND m.variant_id=$7
                  AND ($8 IS NULL OR m.run_id=$8) AND ($9 IS NULL OR m.backtest_id=$9)
                %5$s
              ) SELECT ST_AsMVT(src,%6$L,4096,'geom') FROM src
            $fmt$,
              p_geom_key_col,
              p_geom_fqtn,
              p_metrics_fqtn,
              p_metrics_key_col,
              v_limit_sql,
              p_layer_name
            );
          END IF;

          EXECUTE v_sql
            USING z, x, y, p_origin_year, p_horizon_m, p_series_kind, p_variant_id, p_run_id, p_backtest_id
            INTO v_mvt;

          RETURN coalesce(v_mvt, ''::bytea);
        END;
        $$;
    """)

    # ─── 2) Generic MVT builder: _mvt_history_generic ───
    print("  Creating _mvt_history_generic...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}._mvt_history_generic(
          p_layer_name       text,
          p_geom_fqtn        text,
          p_geom_key_col     text,
          p_metrics_fqtn     text,
          p_metrics_key_col  text,
          z                  integer,
          x                  integer,
          y                  integer,
          p_year             integer,
          p_series_kind      text default 'history',
          p_variant_id       text default '__history__',
          p_run_id           text default null,
          p_backtest_id      text default null,
          p_limit            integer default null
        )
        RETURNS bytea
        LANGUAGE plpgsql STABLE
        AS $$
        DECLARE
          v_sql text;
          v_mvt bytea;
          v_limit_sql text := '';
        BEGIN
          IF p_limit IS NOT NULL AND p_limit > 0 THEN
            v_limit_sql := format(' limit %s', p_limit);
          END IF;

          v_sql := format($fmt$
            WITH bounds AS (
              SELECT ST_TileEnvelope($1,$2,$3) AS b3857,
                     ST_Transform(ST_TileEnvelope($1,$2,$3),4326) AS b4326
            ), src AS (
              SELECT g.%1$I::text AS id, m.year,
                m.value, coalesce(m.p50, m.value) AS p50, m.n,
                m.series_kind, m.variant_id, m.run_id, m.backtest_id,
                m.model_version, m.as_of_date,
                ST_AsMVTGeom(ST_Transform(g.geom,3857),bounds.b3857,4096,256,true) AS geom
              FROM %2$s g
              JOIN %3$s m ON m.%4$I=g.%1$I
              CROSS JOIN bounds
              WHERE g.geom && bounds.b4326 AND ST_Intersects(g.geom,bounds.b4326)
                AND m.year=$4 AND m.series_kind=$5 AND m.variant_id=$6
                AND ($7 IS NULL OR m.run_id=$7) AND ($8 IS NULL OR m.backtest_id=$8)
              %5$s
            ) SELECT ST_AsMVT(src,%6$L,4096,'geom') FROM src
          $fmt$,
            p_geom_key_col,
            p_geom_fqtn,
            p_metrics_fqtn,
            p_metrics_key_col,
            v_limit_sql,
            p_layer_name
          );

          EXECUTE v_sql
            USING z, x, y, p_year, p_series_kind, p_variant_id, p_run_id, p_backtest_id
            INTO v_mvt;

          RETURN coalesce(v_mvt, ''::bytea);
        END;
        $$;
    """)

    # ─── 3) Forecast leaf functions ───
    levels = [
        ("zcta",         "zcta5",           "zcta5",           "'public.geo_zcta20_us_z7'",    "'public.geo_zcta20_us'"),
        ("tract",        "geoid",           "tract_geoid20",   "'public.geo_tract20_tx_z10'",  "'public.geo_tract20_tx'"),
        ("tabblock",     "geoid20",         "tabblock_geoid20","'public.geo_tabblock20_tx_z13'","'public.geo_tabblock20_tx'"),
        ("neighborhood", "neighborhood_id", "neighborhood_id", "'public.geo_neighborhood_tx_z12'","'public.geo_neighborhood_tx'"),
    ]

    for layer, geom_key, metric_key, pref_geom, fall_geom in levels:
        print(f"  Creating mvt_{layer}_choropleth_forecast...")
        cur.execute(f"""
            CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_{layer}_choropleth_forecast(
              z integer, x integer, y integer,
              p_origin_year integer, p_horizon_m integer,
              p_series_kind text default 'forecast',
              p_variant_id  text default '__forecast__',
              p_run_id      text default null,
              p_backtest_id text default null
            ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
            DECLARE v_geom text;
            BEGIN
              v_geom := {SCHEMA}._pick_geom_table({pref_geom}, {fall_geom});
              RETURN {SCHEMA}._mvt_forecast_generic(
                '{layer}', v_geom, '{geom_key}',
                '{SCHEMA}.metrics_{layer}_forecast', '{metric_key}',
                z,x,y,p_origin_year,p_horizon_m,
                p_series_kind,p_variant_id,p_run_id,p_backtest_id,null
              );
            END;
            $$;
        """)

    # UNSD (no simplified geom table)
    print("  Creating mvt_unsd_choropleth_forecast...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_unsd_choropleth_forecast(
          z integer, x integer, y integer,
          p_origin_year integer, p_horizon_m integer,
          p_series_kind text default 'forecast',
          p_variant_id  text default '__forecast__',
          p_run_id      text default null,
          p_backtest_id text default null
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        BEGIN
          RETURN {SCHEMA}._mvt_forecast_generic(
            'unsd', 'public.geo_unsd23_tx', 'geoid',
            '{SCHEMA}.metrics_unsd_forecast', 'unsd_geoid',
            z,x,y,p_origin_year,p_horizon_m,
            p_series_kind,p_variant_id,p_run_id,p_backtest_id,null
          );
        END;
        $$;
    """)

    # Parcel (has limit param)
    print("  Creating mvt_parcel_choropleth_forecast...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_parcel_choropleth_forecast(
          z integer, x integer, y integer,
          p_origin_year integer, p_horizon_m integer,
          p_series_kind text default 'forecast',
          p_variant_id  text default '__forecast__',
          p_run_id      text default null,
          p_backtest_id text default null,
          p_limit       integer default 3500
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        DECLARE v_geom text;
        BEGIN
          v_geom := {SCHEMA}._pick_geom_table('public.geo_parcel_poly_z17', 'public.geo_parcel_poly');
          RETURN {SCHEMA}._mvt_forecast_generic(
            'parcel', v_geom, 'acct',
            '{SCHEMA}.metrics_parcel_forecast', 'acct',
            z,x,y,p_origin_year,p_horizon_m,
            p_series_kind,p_variant_id,p_run_id,p_backtest_id,p_limit
          );
        END;
        $$;
    """)

    # ─── 4) History leaf functions ───
    for layer, geom_key, metric_key, pref_geom, fall_geom in levels:
        print(f"  Creating mvt_{layer}_choropleth_history...")
        cur.execute(f"""
            CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_{layer}_choropleth_history(
              z integer, x integer, y integer,
              p_year integer,
              p_series_kind text default 'history',
              p_variant_id  text default '__history__',
              p_run_id      text default null,
              p_backtest_id text default null
            ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
            DECLARE v_geom text;
            BEGIN
              v_geom := {SCHEMA}._pick_geom_table({pref_geom}, {fall_geom});
              RETURN {SCHEMA}._mvt_history_generic(
                '{layer}', v_geom, '{geom_key}',
                '{SCHEMA}.metrics_{layer}_history', '{metric_key}',
                z,x,y,p_year,
                p_series_kind,p_variant_id,p_run_id,p_backtest_id,null
              );
            END;
            $$;
        """)

    print("  Creating mvt_unsd_choropleth_history...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_unsd_choropleth_history(
          z integer, x integer, y integer,
          p_year integer,
          p_series_kind text default 'history',
          p_variant_id  text default '__history__',
          p_run_id      text default null,
          p_backtest_id text default null
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        BEGIN
          RETURN {SCHEMA}._mvt_history_generic(
            'unsd', 'public.geo_unsd23_tx', 'geoid',
            '{SCHEMA}.metrics_unsd_history', 'unsd_geoid',
            z,x,y,p_year,
            p_series_kind,p_variant_id,p_run_id,p_backtest_id,null
          );
        END;
        $$;
    """)

    print("  Creating mvt_parcel_choropleth_history...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_parcel_choropleth_history(
          z integer, x integer, y integer,
          p_year integer,
          p_series_kind text default 'history',
          p_variant_id  text default '__history__',
          p_run_id      text default null,
          p_backtest_id text default null,
          p_limit       integer default 3500
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        DECLARE v_geom text;
        BEGIN
          v_geom := {SCHEMA}._pick_geom_table('public.geo_parcel_poly_z17', 'public.geo_parcel_poly');
          RETURN {SCHEMA}._mvt_history_generic(
            'parcel', v_geom, 'acct',
            '{SCHEMA}.metrics_parcel_history', 'acct',
            z,x,y,p_year,
            p_series_kind,p_variant_id,p_run_id,p_backtest_id,p_limit
          );
        END;
        $$;
    """)

    # ─── 5) ZIP3 choropleth function ───
    print("  Creating mvt_zip3_choropleth...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_zip3_choropleth(
          z int, x int, y int,
          p_origin_year int DEFAULT 2025,
          p_horizon_m int DEFAULT 12,
          p_series_kind text DEFAULT 'forecast',
          p_variant_id text DEFAULT '__forecast__'
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $func$
        DECLARE
          result bytea;
          bbox geometry;
        BEGIN
          bbox := ST_TileEnvelope(z, x, y);
          SELECT ST_AsMVT(tile, 'zip3', 4096, 'geom') INTO result
          FROM (
            SELECT
              g.zip3 AS id,
              ST_AsMVTGeom(g.geom, bbox, 4096, 256, true) AS geom,
              COALESCE(f.p50, 0) AS p50
            FROM public.geo_zip3_us g
            LEFT JOIN {SCHEMA}.metrics_zip3_forecast f
              ON f.zip3 = g.zip3
              AND f.origin_year = p_origin_year
              AND f.horizon_m = p_horizon_m
            WHERE g.geom && bbox
              AND ST_Intersects(g.geom, bbox)
          ) tile;
          RETURN result;
        END;
        $func$;
    """)

    # ─── 6) ZCTA choropleth (standalone, used in old router) ───
    print("  Creating mvt_zcta_choropleth...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_zcta_choropleth(
          z int, x int, y int,
          p_origin_year int DEFAULT 2025,
          p_horizon_m int DEFAULT 12,
          p_series_kind text DEFAULT 'forecast',
          p_variant_id text DEFAULT '__forecast__'
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        DECLARE v_geom text;
        BEGIN
          v_geom := {SCHEMA}._pick_geom_table('public.geo_zcta20_us_z7', 'public.geo_zcta20_us');
          RETURN {SCHEMA}._mvt_forecast_generic(
            'zcta', v_geom, 'zcta5',
            '{SCHEMA}.metrics_zcta_forecast', 'zcta5',
            z,x,y,p_origin_year,p_horizon_m,
            p_series_kind,p_variant_id,null,null,null
          );
        END;
        $$;
    """)

    # Same aliases used by wire_zip3_mvt
    print("  Creating mvt_tract_choropleth...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_tract_choropleth(
          z int, x int, y int,
          p_origin_year int DEFAULT 2025,
          p_horizon_m int DEFAULT 12,
          p_series_kind text DEFAULT 'forecast',
          p_variant_id text DEFAULT '__forecast__'
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        DECLARE v_geom text;
        BEGIN
          v_geom := {SCHEMA}._pick_geom_table('public.geo_tract20_tx_z10', 'public.geo_tract20_tx');
          RETURN {SCHEMA}._mvt_forecast_generic(
            'tract', v_geom, 'geoid',
            '{SCHEMA}.metrics_tract_forecast', 'tract_geoid20',
            z,x,y,p_origin_year,p_horizon_m,
            p_series_kind,p_variant_id,null,null,null
          );
        END;
        $$;
    """)

    print("  Creating mvt_tabblock_choropleth...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_tabblock_choropleth(
          z int, x int, y int,
          p_origin_year int DEFAULT 2025,
          p_horizon_m int DEFAULT 12,
          p_series_kind text DEFAULT 'forecast',
          p_variant_id text DEFAULT '__forecast__'
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        DECLARE v_geom text;
        BEGIN
          v_geom := {SCHEMA}._pick_geom_table('public.geo_tabblock20_tx_z13', 'public.geo_tabblock20_tx');
          RETURN {SCHEMA}._mvt_forecast_generic(
            'tabblock', v_geom, 'geoid20',
            '{SCHEMA}.metrics_tabblock_forecast', 'tabblock_geoid20',
            z,x,y,p_origin_year,p_horizon_m,
            p_series_kind,p_variant_id,null,null,null
          );
        END;
        $$;
    """)

    print("  Creating mvt_neighborhood_choropleth...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_neighborhood_choropleth(
          z int, x int, y int,
          p_origin_year int DEFAULT 2025,
          p_horizon_m int DEFAULT 12,
          p_series_kind text DEFAULT 'forecast',
          p_variant_id text DEFAULT '__forecast__'
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        DECLARE v_geom text;
        BEGIN
          v_geom := {SCHEMA}._pick_geom_table('public.geo_neighborhood_tx_z12', 'public.geo_neighborhood_tx');
          RETURN {SCHEMA}._mvt_forecast_generic(
            'neighborhood', v_geom, 'neighborhood_id',
            '{SCHEMA}.metrics_neighborhood_forecast', 'neighborhood_id',
            z,x,y,p_origin_year,p_horizon_m,
            p_series_kind,p_variant_id,null,null,null
          );
        END;
        $$;
    """)

    print("  Creating mvt_unsd_choropleth...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_unsd_choropleth(
          z int, x int, y int,
          p_origin_year int DEFAULT 2025,
          p_horizon_m int DEFAULT 12,
          p_series_kind text DEFAULT 'forecast',
          p_variant_id text DEFAULT '__forecast__'
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        BEGIN
          RETURN {SCHEMA}._mvt_forecast_generic(
            'unsd', 'public.geo_unsd23_tx', 'geoid',
            '{SCHEMA}.metrics_unsd_forecast', 'unsd_geoid',
            z,x,y,p_origin_year,p_horizon_m,
            p_series_kind,p_variant_id,null,null,null
          );
        END;
        $$;
    """)

    print("  Creating mvt_parcel_choropleth...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_parcel_choropleth(
          z int, x int, y int,
          p_origin_year int DEFAULT 2025,
          p_horizon_m int DEFAULT 12,
          p_series_kind text DEFAULT 'forecast',
          p_variant_id text DEFAULT '__forecast__',
          p_limit integer DEFAULT 3500
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        DECLARE v_geom text;
        BEGIN
          v_geom := {SCHEMA}._pick_geom_table('public.geo_parcel_poly_z17', 'public.geo_parcel_poly');
          RETURN {SCHEMA}._mvt_forecast_generic(
            'parcel', v_geom, 'acct',
            '{SCHEMA}.metrics_parcel_forecast', 'acct',
            z,x,y,p_origin_year,p_horizon_m,
            p_series_kind,p_variant_id,null,null,p_limit
          );
        END;
        $$;
    """)

    # ─── 7) Router: mvt_choropleth_forecast (with zip3 at z<=4) ───
    print("  Creating mvt_choropleth_forecast (router)...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_choropleth_forecast(
          z integer, x integer, y integer,
          p_origin_year integer,
          p_horizon_m integer,
          p_level_override text default null,
          p_series_kind text default 'forecast',
          p_variant_id  text default '__forecast__',
          p_run_id      text default null,
          p_backtest_id text default null,
          p_parcel_limit integer default 3500
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        BEGIN
          IF p_level_override IS NOT NULL THEN
            CASE p_level_override
              WHEN 'zip3' THEN
                RETURN {SCHEMA}.mvt_zip3_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
              WHEN 'zcta' THEN
                RETURN {SCHEMA}.mvt_zcta_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
              WHEN 'tract' THEN
                RETURN {SCHEMA}.mvt_tract_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
              WHEN 'tabblock' THEN
                RETURN {SCHEMA}.mvt_tabblock_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
              WHEN 'parcel' THEN
                RETURN {SCHEMA}.mvt_parcel_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_parcel_limit);
              WHEN 'neighborhood' THEN
                RETURN {SCHEMA}.mvt_neighborhood_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
              WHEN 'unsd' THEN
                RETURN {SCHEMA}.mvt_unsd_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
              ELSE
                NULL;
            END CASE;
          END IF;

          -- Auto-route by zoom level
          IF z <= 4 THEN
            RETURN {SCHEMA}.mvt_zip3_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
          ELSIF z <= 7 THEN
            RETURN {SCHEMA}.mvt_zcta_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
          ELSIF z <= 11 THEN
            RETURN {SCHEMA}.mvt_tract_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
          ELSIF z <= 16 THEN
            RETURN {SCHEMA}.mvt_tabblock_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
          ELSE
            RETURN {SCHEMA}.mvt_parcel_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_parcel_limit);
          END IF;
        END;
        $$;
    """)

    # ─── 8) Router: mvt_choropleth_history ───
    print("  Creating mvt_choropleth_history (router)...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_choropleth_history(
          z integer, x integer, y integer,
          p_year integer,
          p_level_override text default null,
          p_series_kind text default 'history',
          p_variant_id  text default '__history__',
          p_run_id      text default null,
          p_backtest_id text default null,
          p_parcel_limit integer default 3500
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        BEGIN
          IF p_level_override IS NOT NULL THEN
            CASE lower(p_level_override)
              WHEN 'zcta' THEN
                RETURN {SCHEMA}.mvt_zcta_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
              WHEN 'tract' THEN
                RETURN {SCHEMA}.mvt_tract_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
              WHEN 'tabblock' THEN
                RETURN {SCHEMA}.mvt_tabblock_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
              WHEN 'unsd' THEN
                RETURN {SCHEMA}.mvt_unsd_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
              WHEN 'neighborhood' THEN
                RETURN {SCHEMA}.mvt_neighborhood_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
              WHEN 'parcel' THEN
                RETURN {SCHEMA}.mvt_parcel_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id,p_parcel_limit);
              ELSE
                RETURN ''::bytea;
            END CASE;
          END IF;

          IF z <= 7 THEN
            RETURN {SCHEMA}.mvt_zcta_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
          ELSIF z <= 11 THEN
            RETURN {SCHEMA}.mvt_tract_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
          ELSIF z <= 16 THEN
            RETURN {SCHEMA}.mvt_tabblock_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
          ELSE
            RETURN {SCHEMA}.mvt_parcel_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id,p_parcel_limit);
          END IF;
        END;
        $$;
    """)

    # ─── 9) Parcel lotlines ───
    print("  Creating mvt_parcel_lotlines...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_parcel_lotlines(
          z integer, x integer, y integer,
          p_limit integer default 6000
        ) RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        DECLARE
          v_geom_table text;
          v_mvt bytea;
        BEGIN
          v_geom_table := {SCHEMA}._pick_geom_table('public.geo_parcel_poly_z17', 'public.geo_parcel_poly');
          EXECUTE format($fmt$
            WITH bounds AS (
              SELECT ST_TileEnvelope($1,$2,$3) AS b3857,
                     ST_Transform(ST_TileEnvelope($1,$2,$3),4326) AS b4326
            ), src AS (
              SELECT g.acct::text AS id,
                ST_AsMVTGeom(ST_Transform(ST_Boundary(g.geom),3857),bounds.b3857,4096,256,true) AS geom
              FROM %1$s g CROSS JOIN bounds
              WHERE g.geom && bounds.b4326 AND ST_Intersects(g.geom,bounds.b4326)
              LIMIT %2$s
            ) SELECT ST_AsMVT(src,'parcel_lotlines',4096,'geom') FROM src
          $fmt$, v_geom_table, greatest(p_limit,1))
          USING z, x, y INTO v_mvt;
          RETURN coalesce(v_mvt, ''::bytea);
        END;
        $$;
    """)

    # ─── 10) Grants ───
    print("  Granting permissions...")
    cur.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO anon, authenticated, service_role;")
    cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {SCHEMA} TO anon, authenticated, service_role;")
    cur.execute(f"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {SCHEMA} TO anon, authenticated, service_role;")
    cur.execute(f"""
        ALTER DEFAULT PRIVILEGES IN SCHEMA {SCHEMA}
        GRANT SELECT ON TABLES TO anon, authenticated, service_role;
    """)
    cur.execute(f"""
        ALTER DEFAULT PRIVILEGES IN SCHEMA {SCHEMA}
        GRANT EXECUTE ON FUNCTIONS TO anon, authenticated, service_role;
    """)

    # ─── 11) PostgREST reload ───
    print("  Reloading PostgREST schema cache...")
    cur.execute("NOTIFY pgrst, 'reload schema';")

    conn.close()
    print("DONE — all MVT functions created in forecast_queue")


if __name__ == "__main__":
    main()
