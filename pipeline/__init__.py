# pipeline/__init__.py
# Lazy imports only — do not auto-import orchestrator here to avoid
# triggering the full SCRAPER_REGISTRY chain on simple pipeline usage.
from .cleaner   import DataCleaner
from .matcher   import EntityMatcher
from .validator import DataValidator

__all__ = [
    "PipelineOrchestrator",
    "DataCleaner",
    "EntityMatcher",
    "ScraperWorkerPool",
    "ScrapeJob",
]
