from pydantic import BaseModel
from datetime import datetime


class TelemetryEvent(BaseModel):
    event_id: str
    source_id: str
    metric_name: str
    value: float
    timestamp: datetime


class Alert(BaseModel):
    alert_id: str
    source_id: str
    metric_name: str
    value: float
    threshold: float
    severity: str
    triggered_at: datetime
