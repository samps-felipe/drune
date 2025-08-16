from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field, model_validator

# Define reserved column names used by the framework
class ProjectModel(BaseModel):
    """Defines a validation rule to be applied to a column."""
    engine: str
    pipelines: str
    