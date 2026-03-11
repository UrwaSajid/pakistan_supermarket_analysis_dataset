import json
from pathlib import Path

s = json.loads(Path('config/imtiaz_session_karachi.json').read_text())

print('=== COOKIES ===')
for c in s.get('cookies', []):
    print(f"  {c['name']}={c['value'][:50]}  domain={c['domain']}  httpOnly={c['httpOnly']}  path={c['path']}")

print()
print('=== ORIGINS / LOCALSTORAGE ===')
for o in s.get('origins', []):
    print(f"  origin={o['origin']}")
    for ls in o.get('localStorage', []):
        print(f"    {ls['name']}={str(ls['value'])[:80]}")
