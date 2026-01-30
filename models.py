from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class RuleCondition(BaseModel):
    parameter: str
    operator: str
    value: float
    unit: str
    source: Optional[str] = None

class CompositeRule(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    severity: str  # 'critical', 'warning', 'info'
    category: str
    rule_type: str  # 'simple', 'composite', 'rate_change', 'historical'
    enabled: bool = True
    conditions: List[RuleCondition]
    logical_operator: Optional[str] = None  # 'AND', 'OR'
    time_window_minutes: Optional[int] = None
    aggregation_type: Optional[str] = None  # 'avg', 'sum', 'min', 'max'
    applies_to: str = 'all'
    region_id: Optional[str] = None
    cluster_id: Optional[str] = None
    site_id: Optional[str] = None

class EvaluationResult(BaseModel):
    triggered: bool
    value: Optional[float] = None
    threshold: Optional[float] = None
    message: str
    reason: Optional[str] = None
    conditions_met: Optional[int] = None
    total_conditions: Optional[int] = None
    samples: Optional[int] = None
    rate_of_change: Optional[float] = None

class Alarm(BaseModel):
    id: str
    timestamp: datetime
    site: str
    region: str
    severity: str
    category: str
    message: str
    status: str = 'active'
    composite_rule_id: Optional[str] = None
    asset_id: Optional[int] = None
    conditions_met: Optional[int] = None
    total_conditions: Optional[int] = None
    samples: Optional[int] = None
    rate_of_change: Optional[float] = None
