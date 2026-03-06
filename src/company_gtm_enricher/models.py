from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

ENRICHMENT_COLUMN_MAP = {
    "hq_city": "HQ City",
    "hq_state": "HQ State",
    "country": "Country",
    "approximate_annual_revenue": "Approximate Annual Revenue",
    "current_total_funding": "Current Total Funding",
}

AUDIT_COLUMN_MAP = {
    "confidence": "Enrichment Confidence",
    "source_urls": "Source URLs",
    "status": "Enrichment Status",
    "notes": "Research Notes",
}


@dataclass(frozen=True)
class CompanyEnrichment:
    company_name: str
    hq_city: str = ""
    hq_state: str = ""
    country: str = ""
    approximate_annual_revenue: str = ""
    current_total_funding: str = ""
    confidence: str = ""
    source_urls: List[str] = field(default_factory=list)
    status: str = "ok"
    notes: str = ""

    def to_flat_dict(self, include_audit_columns: bool = True) -> Dict[str, str]:
        row = {
            ENRICHMENT_COLUMN_MAP["hq_city"]: self.hq_city,
            ENRICHMENT_COLUMN_MAP["hq_state"]: self.hq_state,
            ENRICHMENT_COLUMN_MAP["country"]: self.country,
            ENRICHMENT_COLUMN_MAP["approximate_annual_revenue"]: self.approximate_annual_revenue,
            ENRICHMENT_COLUMN_MAP["current_total_funding"]: self.current_total_funding,
        }
        if include_audit_columns:
            row.update(
                {
                    AUDIT_COLUMN_MAP["confidence"]: self.confidence,
                    AUDIT_COLUMN_MAP["source_urls"]: ", ".join(self.source_urls),
                    AUDIT_COLUMN_MAP["status"]: self.status,
                    AUDIT_COLUMN_MAP["notes"]: self.notes,
                }
            )
        return row

    @classmethod
    def empty(cls, company_name: str, status: str = "empty", notes: str = "") -> "CompanyEnrichment":
        return cls(company_name=company_name, status=status, notes=notes)
