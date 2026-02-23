from dataclasses import dataclass, field
from typing import Any


@dataclass
class SearchConfig:
    keywords: str = ""
    client_type: str = ""
    location: str = ""
    max_scrolls: int = 15
    results_limit: int = 100
    search_type: str = "maps"
    dork_query: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "keywords": self.keywords,
            "client_type": self.client_type,
            "location": self.location,
            "max_scrolls": self.max_scrolls,
            "results_limit": self.results_limit,
            "search_type": self.search_type,
            "dork_query": self.dork_query,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SearchConfig":
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})


SEARCH_CONFIG = SearchConfig()
