from pyexpat import model
from typing import List, Optional, Dict, Any, Literal
from click import Option
from pydantic import BaseModel, Field, model_validator

from .defaults_model import TypeDefault


# Define reserved column names used by the framework
RESERVED_COLUMN_NAMES = ["id"]

class PipelineDefaults(BaseModel):
    """Default settings for the pipeline."""
    # patterns: Dict[str, str] = {}
    types: Dict[str, TypeDefault] = None
    vars: Dict[str, Any] = {}


# Column Section
class ConstraintSpec(BaseModel):
    """Defines a constraint to be applied to a column."""
    rule: str = Field(..., description="The rule to apply, e.g., 'not_null', 'unique', 'pattern:regex'.")
    on_fail: Literal['fail', 'drop', 'warn', 'set_null'] = 'fail'  # Action to take if the constraint fails

class ColumnSpec(BaseModel):
    """Specification for a single column in a pipeline."""
    name: str = Field(None, description="Column name used for renaming.")
    old_name: Optional[str] = Field(None, description="Original name of the column before renaming.", alias='from')
    description: Optional[str] = Field(None, description="Description of the column.")
    type: str = Field(..., description="Data type of the column.")
    optional: Optional[bool] = Field(False, description="If True, the source column can be missing.")
    expression: Optional[str] = Field(None, description="Expression apply to the column in format 'func(arg1,{col},...)'.")
    constraints: List[ConstraintSpec] = Field([], description="List of constraints to apply to the column.")
    format: Optional[str] = None
    try_cast: bool = False

    # @model_validator(mode="after")
    # def set_old_name_from_name(self):
    #     """If 'name' is not provided, use 'old_name' as 'name'."""
    #     if self.old_name is None:
    #         self.old_name = self.name
    #     return self
        

    @model_validator(mode="after")
    def check_reserved_names(self):
        """Validates that the final column name is not a reserved name."""
        final_name = self.name

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
        
        return self

# Source Section
class SourceSchemaSpec(BaseModel):
    """Schema specification for the source table."""
    primary_key: List[str] = Field([], description="List of primary key columns.")
    columns: List[ColumnSpec] = Field([], description="List of columns in the schema.")

class SourceSpec(BaseModel):
    """Configuration for the data source."""
    name: str = Field(..., description="Name of the source.")
    type: Literal['file', 'sql', 'table'] = Field(..., description="Type of the source, e.g., 'file', 'query', 'table'.")
    format: str = Field(..., description="Input format, e.g., 'delta', 'parquet'.")
    path: Optional[str] = ""
    table_name: Optional[str] = None
    query: Optional[str] = None
    options: Dict[str, Any] = {}
    
    schema_spec: Optional[SourceSchemaSpec] = Field(None, description="Schema specification for the source table.", alias='schema')

    @model_validator(mode="after")
    def check_source_type_fields(self):
        if self.type == 'table' and not self.table_name:
            raise ValueError("For source type 'table', 'table_name' must be specified.")
        if self.type == 'sql' and not self.query:
            raise ValueError("For source type 'sql', 'query' must be specified.")
        return self

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
    type: Literal['file', 'table'] = Field(..., description="Type of the target, e.g., 'file', 'query', 'table'.")
    format: str = Field(..., description="Output format, e.g., 'delta', 'parquet'.")
    path: Optional[str] = None
    table: Optional[str] = None
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


class StepConfig(BaseModel):
    """Configuration for a generic pipeline step."""
    name: str = Field(..., description="Name of the step, used for logging and tracking.")
    description: Optional[str] = None
    type: str = Field(..., description="Type of the step, e.g., 'read', 'transform', 'validate', 'write'.")
    params: Dict[str, Any] = {}

class PipelineModel(BaseModel):
    """Main configuration model for a pipeline."""
    pipeline_name: str = Field(..., description="Name of the pipeline.")
    description: Optional[str] = None
    defaults: Optional[PipelineDefaults] = Field(..., description="Default settings for the pipeline.")
    sources: List[SourceSpec] = Field(..., description="List of data sources for the pipeline.")
    target: TargetSpec
    steps: Optional[List[StepConfig]] = Field([], description="List of pipeline steps.")

    def _apply_type_defaults_to_column(self, column: ColumnSpec, type_defaults: Dict[str, TypeDefault]):
        """Dynamically applies default values from a type default to a column."""
        if column.type in type_defaults:
            default_config = type_defaults[column.type]
            # Iterate over the fields of the default config model, excluding unset values
            for field_name, default_value in default_config.model_dump(exclude_unset=True).items():
                # Get the current value on the column
                current_value = getattr(column, field_name, None)
                # Apply default only if the current value is not set (is None, False, or empty list/dict)
                if not current_value and default_value is not None:
                    setattr(column, field_name, default_value)

    @model_validator(mode="after")
    def merge_target_with_sources_schema(self):
        if self.target.schema_spec.inherits:
            for inherit_spec in self.target.schema_spec.inherits:
                source_name_to_inherit = inherit_spec.from_name
                
                # Find the corresponding source
                source_to_inherit_from = next(
                    (s for s in self.sources if s.name == source_name_to_inherit),
                    None
                )

                if source_to_inherit_from and source_to_inherit_from.schema_spec:
                    inherited_columns = []
                    for col in source_to_inherit_from.schema_spec.columns:
                        # Apply include/exclude logic
                        if inherit_spec.columns and col.name not in inherit_spec.columns:
                            continue
                        if inherit_spec.exclude and col.name in inherit_spec.exclude:
                            continue
                        inherited_columns.append(col)
                    
                    # Merge inherited columns with existing target columns,
                    # giving precedence to explicitly defined target columns
                    existing_target_column_names = {c.name for c in self.target.schema_spec.columns}
                    for inherited_col in inherited_columns:
                        if inherited_col.name not in existing_target_column_names:
                            self.target.schema_spec.columns.append(inherited_col)
        return self

    @model_validator(mode="after")
    def merge_defaults_with_pipeline_config(self):
        """
        Merges pipeline-level defaults into source and target configurations.
        This method is called after the model is validated.
        """
        # This validator handles defaults defined inside the pipeline file.
        # Project-level defaults should be merged before this model is even instantiated.
        if not self.defaults or not self.defaults.types:
            return self

        # Collect all columns from sources and the target
        all_columns = []
        for source in self.sources:
            if source.schema_spec and source.schema_spec.columns:
                all_columns.extend(source.schema_spec.columns)
        if self.target and self.target.schema_spec and self.target.schema_spec.columns:
            all_columns.extend(self.target.schema_spec.columns)

        # Apply the type defaults to each column
        for column in all_columns:
            self._apply_type_defaults_to_column(column, self.defaults.types)

        return self