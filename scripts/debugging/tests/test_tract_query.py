import os, json, urllib.request, urllib.parse

def load_env():
    env = {}
    try:
        with open('.env.local', 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('\ufeff'): line = line[1:]
                if not line or line.startswith('#'): continue
                if '=' in line:
                    k, v = line.split('=', 1)
                    env[k.strip()] = v.strip().strip('\"\'')
    except Exception as e:
        print('Error loading env:', e)
    return env

env = load_env()
url = env.get('NEXT_PUBLIC_SUPABASE_URL', '')
key = env.get('SUPABASE_SERVICE_ROLE_KEY', '')

print('URL found:', bool(url))
print('Key found:', bool(key))

headers = {'apikey': key, 'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'}
q = urllib.parse.urlencode({'select': 'tract_geoid20', 'tract_geoid20': 'like.51147%', 'limit': '15', 'horizon_m': 'eq.12'})

try:
    req = urllib.request.Request(f'{url}/rest/v1/metrics_tract_forecast?{q}', headers=headers)
    res = json.loads(urllib.request.urlopen(req).read().decode())
    print(f'Count found in 51147: {len(res)}')
    for r in res:
        print(r)
except Exception as e:
    print('Request error:', e)
