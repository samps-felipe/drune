from typing import Any, Dict, Optional
import yaml
from pathlib import Path
import glob

from drune.core.quality import DataQualityManager

from .engine import BaseEngine

from .steps import StepManager
from .models import PipelineModel
from .models import ProjectModel

from drune.utils.logger import get_logger
from drune.utils.exceptions import ConfigurationError

class Pipeline:
    """
    Represents a single, executable data pipeline.

    It holds the pipeline's state, including its configuration,
    the steps to be executed, and the current progress.
    """

    def __init__(self, project_dir: str, project_config: ProjectModel, engine: BaseEngine):
        """Initializes the Pipeline with the given configuration."""
        self.project_dir = project_dir
        self.project = project_config
        self.engine = engine
        self.step_manager = StepManager(self.engine)
        self.quality_manager = DataQualityManager(self.engine, project_dir, project_config.logging.path)

        
    def load(self, pipeline_path: str):
        self.pipeline_path = pipeline_path

        self.config = self._load_pipeline(pipeline_path)
        self.logger = get_logger(f"pipeline:{self.config.pipeline_name}")

        self._merge_defaults()

        self._sources = {}
        self._target = None

        self.step_manager.load(self.config.steps)

        self.logger.info(f"Pipeline '{self.config.pipeline_name}' loaded.")

        return self
    
    
    def refresh(self):
        """Reloads the pipeline configuration from its source file."""
        self.logger.info(f"Refreshing pipeline configuration from '{self.pipeline_path}'")
        self.config = self._load_pipeline(self.pipeline_path)
        self._merge_defaults()
        self.logger.info(f"Pipeline '{self.config.pipeline_name}' configurations refreshed.")

    def reset(self):
        """Resets the pipeline's execution state."""
        self.logger.info("Resetting pipeline state.")
        self._sources = {}
        self._target = None
        self.step_manager.reset()    
    
    def run(self, stop_at: Optional[str] = None):
        """
        Executes the pipeline from the current state.

        Args:
            stop_at: The name of the step to stop after. If None, runs all steps.
        """        
        return self.step_manager.run(self._sources, self._target, stop_at)

    
    def create(self):
        self.logger.info(f"Starting table creation for: {self.config.pipeline_name}")
        self.engine.create_table(self.config)
        self.logger.info("Table creation finished.")


    def update(self):
        self.logger.info(f"Starting table update for: {self.config.pipeline_name}")
        self.engine.update_table(self.config)
        self.logger.info("Table update finished.")

    
    def read(self, src_paths: Optional[Dict[str, str]] = None, apply_schema: bool = True, apply_constraints: bool = True):
        """Reads data from the source defined in the pipeline configuration."""
        self.logger.info(f"Starting reading sources...")

        src_paths = src_paths or {}

        for source in self.config.sources:
            source_data = None
            dynamic_path = src_paths.get(source.name)

            if source.type == 'file':
                source_data = self._read_file(source, dynamic_path)
            elif source.type == 'table':
                source_data = self._read_table(source)
            elif source.type == 'sql': # Matching 'sql' from the model
                source_data = self._read_query(source)
            else:
                raise ConfigurationError(f"Source type '{source.type}' is not supported by the engine.")
            self._sources[source.name] = source_data

            # Apply schema
            if apply_schema:
                self._sources[source.name] = self.engine.apply_schema(self._sources[source.name], source.schema_spec)

            # Apply constraints
            if apply_constraints:
                self._sources[source.name] = self.quality_manager.apply_schema_constraints(self._sources[source.name], source.schema_spec)
        
        self._target = self._sources[self.config.sources[0].name]

    
    def _read_file(self, source, dynamic_path: Optional[str]) -> Any:
        """Reads a file from the specified source configuration."""
        source = source.model_copy() # Create a copy to avoid modifying the original

        source_path = Path(source.path)

        # Resolve relative paths using the project's default sources path
        if not source_path.is_absolute():
            base_sources_path = Path(self.project_dir) / self.project.defaults.paths.sources
            source_path = base_sources_path / source_path

        # If the path in YAML is a directory (no extension), a dynamic path must be provided
        if not source_path.suffix: # It's a directory
            if not dynamic_path:
                raise ConfigurationError(f"Source '{source.name}' points to a directory ('{source_path}') but no specific file was provided at runtime.")
            source.path = str(source_path / dynamic_path)
        else: # It's a file
            if dynamic_path:
                # If a dynamic path is provided, it replaces the file name
                source.path = str(source_path.with_name(dynamic_path))
            else:
                source.path = str(source_path)
        
        return self.engine.read_file(source)

    
    def _read_table(self, source) -> Any:
        """Reads a table from the database using the engine's read_table method."""
        table_name = source.table
        return self.engine.read_table(table_name)
    
    def _read_query(self, source) -> Any:
        """Executes a SQL query using the engine's execute_query method."""
        query = source.query
        return self.engine.execute_query(query)
    
    def write(self, data, path: Optional[str] = None):
        """Writes data to the target defined in the pipeline configuration."""
        self.logger.info(f"Starting data write for: {self.config.pipeline_name}")
        self.engine.write(data, path)
        self.logger.info("Data write finished.")

    def test(self):
        """Executes the pipeline in test mode."""
        if not self.config.test:
            self.logger.error("Test configuration ('test:') not found in YAML file.")
            raise ConfigurationError("Test configuration is missing.")

        self.logger.info(f"Starting test mode for pipeline: {self.config.pipeline_name}")
        self.engine.test(self.config)
        self.logger.info(f"Test mode for pipeline: {self.config.pipeline_name} finished.")
 
    
    def _load_pipeline(self, path: str) -> PipelineModel:
        """Loads and merges all YAML files in a directory, then parses as PipelineModel."""
        pipelines_path = Path(self.project.defaults.paths.pipelines)
        pipeline_dir = Path(path)
        if pipelines_path and not pipeline_dir.is_absolute():
            pipeline_dir = Path(self.project_dir) / Path(pipelines_path) / path
        else:
            pipeline_dir = Path(path)
        
        # Find all .yml and .yaml files in the directory
        files = list(pipeline_dir.glob("*.yml")) + list(pipeline_dir.glob("*.yaml"))

        merged_dict = {}
        for file_path in files:
            with open(file_path, 'r') as file:
                yml_dict = yaml.safe_load(file) or {}
                merged_dict.update(yml_dict)
        
        pipeline_model = PipelineModel(**merged_dict)

        return pipeline_model
    
    def _merge_defaults(self):
        """
        Merges project-level defaults into the pipeline configuration dynamically.
        This function is now more robust and maintainable.
        """
        pipeline_config = self.config
        project_defaults = self.project.defaults

        # 1. Merge Type Defaults into Columns
        if project_defaults.types:
            # Collect all columns from sources and the target
            all_columns = []
            for source in pipeline_config.sources:
                if source.schema_spec and source.schema_spec.columns:
                    all_columns.extend(source.schema_spec.columns)
            if pipeline_config.target and pipeline_config.target.schema_spec and pipeline_config.target.schema_spec.columns:
                all_columns.extend(pipeline_config.target.schema_spec.columns)

            # Apply the type defaults to each column
            for column in all_columns:
                if column.type in project_defaults.types:
                    default_config = project_defaults.types[column.type]
                    for field, value in default_config.model_dump(exclude_unset=True).items():
                        if not getattr(column, field, None) and value is not None:
                            setattr(column, field, value)

        # 2. Merge Source Defaults
        if project_defaults.sources:
            for source in pipeline_config.sources:
                if source.type in project_defaults.sources:
                    default_config = project_defaults.sources[source.type]
                    for field, value in default_config.model_dump(exclude_unset=True).items():
                        if not getattr(source, field, None) and value is not None:
                            setattr(source, field, value)
        
        # 3. Merge Target Defaults can be added here following the same pattern
        if project_defaults.targets:
            target = pipeline_config.target
            if target.type in project_defaults.targets:
                default_config = project_defaults.targets[target.type]
                for field, value in default_config.model_dump(exclude_unset=True).items():
                    if not getattr(target, field, None) and value is not None:
                        setattr(target, field, value)
                        
        return pipeline_config