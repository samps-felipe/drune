from pydantic import BaseModel
import datetime

class ValidationResult(BaseModel):
    pipeline_name: str
    table_name: str
    key_column_name: str
    key_column_value: str
    constraint_name: str
    constraint_action: str
    message: str
    created_at: datetime.datetime