from typing import List, Literal, Optional, Dict, Any
from pydantic import BaseModel, Field

from .defaults_model import TypeDefault, SourceDefault, TargetDefault

class EngineConfig(BaseModel):
    """Engine configuration, allowing for a default engine and specific overrides."""
    name: str 
    options: Dict[str, Any] = {}

# Model for project paths
class Paths(BaseModel):
    """Defines default directories for the project."""
    pipelines: str = "pipelines/"
    sources: str = "data/sources/"
    targets: str = "data/targets/"

# Model for global defaults
class Defaults(BaseModel):
    """Defines global defaults for pipelines."""
    engine: Optional[EngineConfig] = None
    vars: Dict[str, Any] = {}
    paths: Paths = None
    types: Dict[str, TypeDefault] = None
    sources: Dict[Literal['file', 'query', 'table'], SourceDefault] = None
    targets: Dict[Literal['file', 'table'], TargetDefault] = None


class LoggingConfig(BaseModel):
    """Logging configuration."""
    path: str = "logs/"


# Main model for the drune.yml project file
class ProjectModel(BaseModel):
    """Pydantic model for the drune.yml project configuration file."""
    project_name: str
    description: Optional[str] = None
    version: Optional[str] = None
    profile: Optional[str] = None
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    defaults: Defaults = Field(default_factory=Defaults)
    profiles: Dict[str, Defaults] = {}


    def deep_merge_dicts(self, base: dict, override: dict) -> dict:
        """
        Recursively merges the 'override' dictionary into the 'base' dictionary.
        Keys from 'override' take precedence.
        """
        merged = base.copy()
        for key, value in override.items():
            if key in merged and isinstance(merged.get(key), dict) and isinstance(value, dict):
                merged[key] = self.deep_merge_dicts(merged[key], value)
            else:
                merged[key] = value
        return merged

    def merge_defaults(self, profile):
        """
        Merges a profile's configuration on top of the default configuration.
        """

        if not profile:
            profile = self.profile
        
        base_defaults = self.defaults

        if profile in self.profiles:
            profile_overrides = self.profiles[profile]

            # Convert the Pydantic models to dictionaries, excluding unset values
            # to prevent Pydantic's default factory values from overwriting the base.
            base_dict = base_defaults.model_dump(exclude_unset=True)
            override_dict = profile_overrides.model_dump(exclude_unset=True)
            
            # Perform the deep merge of the dictionaries
            merged_dict = self.deep_merge_dicts(base_dict, override_dict)
            
            # Return a new Defaults object from the merged dictionary
            self.defaults = Defaults(**merged_dict)

        else:
            raise ValueError(f"Profile '{profile}' not found in project")
        
        
    
    