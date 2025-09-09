## app/schemas.py

from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional
from datetime import date, datetime

class Event(BaseModel):
    patient_id: str
    first_name: str
    last_name: str
    dob: date
    zip: str
    event_ts: datetime
    systolic_bp: float
    diastolic_bp: float
    heart_rate: float

class CleanEvent(BaseModel):
    patient_key: str = Field(description="HMACed patient_id")
    dob_year: Optional[int]
    zip3: Optional[str]
    event_date: Optional[date]
    systolic_bp: float
    diastolic_bp: float
    heart_rate: float
    outlier_flags: Optional[str] = None
