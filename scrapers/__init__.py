# scrapers/__init__.py
from .metro_scraper      import MetroScraper
from .imtiaz_scraper     import ImtiazScraper
from .naheed_scraper     import NaheedScraper
from .alfatah_scraper    import AlfatahScraper
from .chaseup_scraper    import ChaseUpScraper
from .springs_scraper    import SpringsScraper

SCRAPER_REGISTRY: dict[str, type] = {
    "metro":     MetroScraper,
    "imtiaz":    ImtiazScraper,
    "naheed":    NaheedScraper,
    "alfatah":   AlfatahScraper,
    "chaseup":   ChaseUpScraper,
    "springs":   SpringsScraper,
}

__all__ = list(SCRAPER_REGISTRY.keys()) + ["SCRAPER_REGISTRY"]
