import json

s = json.load(open(r"D:\data_science_market\config\imtiaz_api_sample.json"))
for url, data in s.items():
    if "/api/" in url:
        ep = url.split("/api/")[1].split("?")[0]
    else:
        ep = url[-50:]
    print(f"=== {ep} ===  {url.split('?')[1][:80] if '?' in url else ''}")
    d = data.get("data")
    if isinstance(d, list) and d and isinstance(d[0], dict):
        print(f"  list[{len(d)}] first keys: {list(d[0].keys())[:15]}")
        if "sub-section" in url.lower():
            print(f"  FULL sub-section[0]:")
            print(json.dumps(d[0], indent=2)[:800])
    elif isinstance(d, dict):
        items = d.get("items") or d.get("products") or []
        print(f"  dict keys: {list(d.keys())[:10]}")
        if items:
            print(f"  items[{len(items)}] first keys: {list(items[0].keys())[:15]}")
    else:
        print(f"  data: {str(d)[:100]}")
    print()
