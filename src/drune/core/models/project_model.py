from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

# Model for default transformations by data type
class TypeDefault(BaseModel):
    """Defines default transformations for a data type."""
    string: List[Any] = []
    integer: List[Any] = []
    timestamp: List[Any] = []
    # Add other types as needed

# Model for default source/target options
class FileOptions(BaseModel):
    """Default options for file-based sources/targets."""
    options: Dict[str, Any] = {}
    format: Optional[str] = None
    mode: Optional[str] = None

# Model for global defaults
class GlobalDefaults(BaseModel):
    """Defines global defaults for pipelines."""
    engine: Optional[str] = None
    metadata: Optional[str] = "drune_catalog"
    type_defaults: TypeDefault = Field(default_factory=TypeDefault)
    source_defaults: Dict[str, FileOptions] = Field(default_factory=dict)
    target_defaults: Dict[str, FileOptions] = Field(default_factory=dict)

# Model for project paths
class Paths(BaseModel):
    """Defines default directories for the project."""
    pipelines: str = "pipelines/"
    sources: str = "data/sources/"
    targets: str = "data/targets/"
    logs: str = "logs/"

# Model for a single database profile
class Profile(BaseModel):
    """Configuration for a single connection profile."""
    type: str
    host: str
    port: int
    user: str
    password: str
    dbname: str

class EngineConfig(BaseModel):
    """Engine configuration, allowing for a default engine and specific overrides."""
    name: str 
    options: Dict[str, Any] = Field(default_factory=dict)

# Main model for the drune.yml project file
class ProjectModel(BaseModel):
    """Pydantic model for the drune.yml project configuration file."""
    project_name: str
    description: Optional[str] = None
    version: Optional[str] = None
    engine: EngineConfig = Field(default_factory=EngineConfig)
    profiles: Dict[str, Dict[str, Profile]] = Field(default_factory=dict)
    paths: Paths = Field(default_factory=Paths)
    defaults: GlobalDefaults = Field(default_factory=GlobalDefaults)
    vars: Dict[str, Any] = Field(default_factory=dict)