from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field

class TypeDefault(BaseModel):
    """Default type transformation settings."""
    format: Optional[str] = None
    try_cast: bool = False
    transform: Optional[str] = None
    options: Dict[str, Any] = {}
    

# Model for default target options
class SourceDefault(BaseModel):
    """Default options for file-based sources/targets."""
    format: Optional[str] = Field(..., description="Input format, e.g., 'delta', 'parquet'.")
    options: Dict[str, Any] = {}

# Model for default target options
class TargetDefault(BaseModel):
    """Default options for file-based sources/targets."""
    format: Optional[str] = Field(..., description="Output format, e.g., 'delta', 'parquet'.")
    options: Dict[str, Any] = {}
    mode: Optional[
        Literal[
        'append', 
        'overwrite', 
        'merge', 
        'overwrite_partition', 
        'overwrite_where']
        ] = Field('append', description="Write mode for the target, e.g., 'append', 'overwrite', 'merge', 'overwrite_partition', 'overwrite_where'.")