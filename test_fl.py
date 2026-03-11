import os, psycopg2
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
def get_db_connection():
    with open('.env.local') as f:
        for line in f:
            if line.startswith('POSTGRES_URL_NON_POOLING='):
                url = line.split('=', 1)[1].strip().strip('\"').strip('\'')
                return psycopg2.connect(url)
conn = get_db_connection()
cur = conn.cursor()
try:
    cur.execute("SELECT COUNT(*) FROM forecast_queue.metrics_parcel_forecast WHERE jurisdiction = 'florida_dor'")
    print('Florida forecast rows:', cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM forecast_queue.metrics_parcel_forecast WHERE jurisdiction = 'nyc'")
    print('NYC forecast rows:', cur.fetchone()[0])
except Exception as e:
    print('Error:', e)
