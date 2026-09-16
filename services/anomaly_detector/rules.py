from dataclasses import dataclass
from typing import Optional

@dataclass
class SLORule:
    name: str
    metric_name: str
    condition: str          # 'gt' or 'lt'
    threshold: float
    window_seconds: int
    severity: str           # 'P1', 'P2', 'P3'
    description: str

# Standard deterministic production SLO rules
DEFAULT_SLO_RULES = [
    SLORule(
        name="HighHttpErrorRate",
        metric_name="http_5xx_rate",
        condition="gt",
        threshold=0.05,     # > 5% error rate breaches SLO
        window_seconds=10,
        severity="P1",
        description="HTTP 5xx error rate exceeded 5% threshold"
    ),
    SLORule(
        name="HighP95Latency",
        metric_name="p95_latency_seconds",
        condition="gt",
        threshold=1.5,      # > 1.5s p95 latency
        window_seconds=15,
        severity="P2",
        description="p95 latency exceeded 1.5 second threshold"
    ),
    SLORule(
        name="HighMemoryUsage",
        metric_name="memory_bytes",
        condition="gt",
        threshold=350 * 1024 * 1024,  # > 350 MB
        window_seconds=10,
        severity="P2",
        description="Process RSS memory exceeded 350MB saturation limit"
    )
]
