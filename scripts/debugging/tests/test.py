import urllib.request, json

def load_env():
    env = {}
    with open('.env.local', 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.startswith('\ufeff'): line = line[1:]
            if not line or line.startswith('#'): continue
            if '=' in line:
                k, v = line.split('=', 1)
                env[k.strip()] = v.strip().strip('\"\'')
    return env

try:
    env = load_env()
    url = env.get('NEXT_PUBLIC_SUPABASE_URL', '')
    key = env.get('SUPABASE_SERVICE_ROLE_KEY', '')
    headers = {'apikey': key, 'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'}
    q = 'select=county_name,city,neighborhood_name,tract_geoid20&tract_geoid20=eq.51147045401&limit=1'
    req = urllib.request.Request(f'{url}/rest/v1/parcel_ladder_v1?{q}', headers=headers)
    res = json.loads(urllib.request.urlopen(req).read().decode())
    print('parcel_ladder_v1 match:', res)
except Exception as e:
    print('Exception:', e)
