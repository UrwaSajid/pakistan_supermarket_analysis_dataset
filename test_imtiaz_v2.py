"""Quick test of the new ImtiazScraper v2"""
import sys
sys.path.insert(0, r"D:\data_science_market")

from scrapers.imtiaz_scraper_v2 import ImtiazScraper

s = ImtiazScraper(city="karachi", max_workers=1)
records = s.scrape()
print(f"\n=== RESULTS: {len(records)} records ===")
if records:
    r = records[0]
    print(f"Columns: {list(r.keys())}")
    print(f"\nSample record:")
    for k, v in r.items():
        if str(v):
            print(f"  {k}: {v!r}")
    
    # Check for prices
    priced = [r for r in records if r.get("price", 0) > 0]
    print(f"\nRecords with price > 0: {len(priced)}/{len(records)}")
    
    # Check for product_url
    with_url = [r for r in records if r.get("product_url")]
    print(f"Records with product_url: {len(with_url)}/{len(records)}")
