from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field, model_validator

# Define reserved column names used by the framework
RESERVED_COLUMN_NAMES = ["id"]

# Pipeline Section
class TypeDafault(BaseModel):
    """Default type transformation settings."""
    type: str
    format: Optional[str] = None
    try_cast: bool = False
    transform: List[str] = []

class PipelineDefaults(BaseModel):
    """Default settings for the pipeline."""
    patterns: Dict[str, str] = {}
    types: List[TypeDafault] = []

class PipelineSpec(BaseModel):
    """Base class for pipeline specifications."""
    name: str
    description: Optional[str] = None
    # engine: str = Field(..., description="Name of the engine to use for processing.")
    metadata: Optional[str] = None
    defaults: Optional[PipelineDefaults] = None

# Column Section
class ConstraintSpec(BaseModel):
    """Defines a constraint to be applied to a column."""
    rule: str = Field(..., description="The rule to apply, e.g., 'not_null', 'unique', 'pattern:regex'.")
    on_fail: Literal['fail', 'drop', 'warn', 'set_null'] = 'fail'  # Action to take if the constraint fails

class ColumnSpec(BaseModel):
    """Specification for a single column in a pipeline."""
    name: Optional[str] = Field(None, description="Original column name in the source.")
    rename: Optional[str] = Field(None, description="New name for the column after processing.")
    description: Optional[str] = Field(None, description="Description of the column.")
    type: str = Field(..., description="Data type of the column.")
    optional: Optional[bool] = Field(False, description="If True, the source column can be missing.")
    transform: Optional[str] = Field(None, description="List of transformations to apply to the column in format 'func:arg1,arg2,...'.")
    constraints: List[ConstraintSpec] = Field([], description="List of constraints to apply to the column.")
    format: Optional[str] = None
    try_cast: bool = False


    @model_validator(mode="before")
    def set_name_or_rename(cls, values):
        """Ensures that either 'name' or 'rename' is present."""

        name = values.get('name')
        rename = values.get('rename')
        if name is None and rename is None:
            raise ValueError("Each column must have either a 'name' or 'rename' field.")
        if name is None:
            values['name'] = rename
        elif rename is None:
            values['rename'] = name
        return values


    @model_validator(mode="after")
    def check_reserved_names(cls, values):
        """Validates that the final column name is not a reserved name."""
        final_name = getattr(values, 'rename', None) or getattr(values, 'name', None)

        if final_name.lower() in RESERVED_COLUMN_NAMES:
            raise ValueError(
                f"The column name '{final_name}' is reserved by the framework. "
                f"Reserved names are: {RESERVED_COLUMN_NAMES}"
            )
        
        if final_name.startswith("_"):
            raise ValueError(
                "Column names cannot start with an underscore ('_')."
                f"Found: '{final_name}'"
            )
        
        return values

# Target Section
class InheritsSpec(BaseModel):
    """Specifies the parent table to inherit schema from."""
    from_name: str = Field(..., description="Name of the parent table to inherit schema from.", alias='from')
    columns: Optional[List[str]] = Field(None, description="List of columns to include from the inherited schema. If not provided, all columns will be included.")
    exclude: Optional[List[str]] = Field(None, description="List of columns to exclude from the inherited schema.")

class TargetSchemaSpec(BaseModel):
    """Schema specification for the target table."""
    primary_key: List[str] = Field([], description="List of primary key columns.")
    partition_by: Optional[List[str]] = Field(None, description="List of columns to partition the data by.")

    inherits: Optional[List[InheritsSpec]] = Field(None, description="Specification for inheriting schema from another table.")
    columns: List[ColumnSpec] = Field([], description="List of columns in the schema.")

class SCDConfig(BaseModel):
    """Configuration for Slowly Changing Dimension (SCD)."""
    type: Literal['2'] = '2'
    track_columns: Optional[List[str]] = Field(None, description="Columns to track for changes. If not provided, all non-PK columns will be tracked.")

class TargetSpec(BaseModel):
    """Configuration for the data target."""
    name: str = Field(..., description="Name of the target table.")
    description: Optional[str] = None
    type: Literal['file', 'query', 'table'] = Field(..., description="Type of the target, e.g., 'file', 'query', 'table'.")
    format: str = Field(..., description="Output format, e.g., 'delta', 'parquet'.")
    path: Optional[str] = None
    table: Optional[str] = None
    query: Optional[str] = None
    mode: Literal[
        'append', 
        'overwrite', 
        'merge', 
        'overwrite_partition', 
        'overwrite_where'] = Field('append', description="Write mode for the target, e.g., 'append', 'overwrite', 'merge', 'overwrite_partition', 'overwrite_where'.")
    overwrite_condition: Optional[str] = Field(None, description="Condition for overwriting data, applicable for 'overwrite_where' mode.")
    scd: Optional[SCDConfig] = Field(None, description="Configuration for Slowly Changing Dimension (SCD).")
    options: Dict[str, Any] = Field(default_factory=dict, description="Additional options for the target.")

    schema_spec: TargetSchemaSpec = Field(..., description="Schema specification for the target table.", alias='schema')

# Source Section
class SourceSchemaSpec(BaseModel):
    """Schema specification for the source table."""
    primary_key: List[str] = Field([], description="List of primary key columns.")
    columns: List[ColumnSpec] = Field([], description="List of columns in the schema.")
class SourceSpec(BaseModel):
    """Configuration for the data source."""
    name: str = Field(..., description="Name of the source.")
    type: Literal['file', 'query', 'table'] = Field(..., description="Type of the source, e.g., 'file', 'query', 'table'.")
    format: str = Field(..., description="Input format, e.g., 'delta', 'parquet'.")
    path: Optional[str] = None
    table: Optional[str] = None
    query: Optional[str] = None
    options: Dict[str, Any] = {}
    
    schema_spec: Optional[SourceSchemaSpec] = Field(None, description="Schema specification for the source table.", alias='schema')

class StepConfig(BaseModel):
    """Configuration for a generic pipeline step."""
    name: str = Field(..., description="Name of the step, used for logging and tracking.")
    description: Optional[str] = None
    type: str = Field(..., description="Type of the step, e.g., 'read', 'transform', 'validate', 'write'.")
    params: Dict[str, Any] = {}

class PipelineModel(BaseModel):
    """Main configuration model for a pipeline."""
    pipeline: PipelineSpec
    target: TargetSpec
    sources: List[SourceSpec] = Field(..., description="List of data sources for the pipeline.")
    steps: Optional[List[StepConfig]] = Field([], description="List of pipeline steps.")