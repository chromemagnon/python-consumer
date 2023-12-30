
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class IOTDeviceMeasurementDTO(BaseModel):
    """
    Data Transfer Object representing an IoT device measurement.
    """
    device_identifier: str = Field(..., example="de0d880d-c5eb-4d9a-96ca-4542167dcc0b")
    temperature: float = Field(..., gt=-273, example=25.0)
    measurement_time: datetime = Field(...)


