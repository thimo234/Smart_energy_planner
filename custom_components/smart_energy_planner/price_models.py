"""Price planner data models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class PlannerWindow:
    start: datetime
    end: datetime
    price: float
