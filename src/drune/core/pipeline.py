import os
from typing import Any, Dict, Optional
import yaml
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
        self.quality_manager = DataQualityManager(self.engine, project_dir, project_config.paths.logs)

        
    def load(self, pipeline_path: str):
        self.pipeline_path = pipeline_path

        self.config = self._load_pipeline(pipeline_path)
        self.logger = get_logger(f"pipeline:{self.config.pipeline.name}")

        self._sources = {}
        self._target = None

        self.step_manager.load(self.config.steps)

        self.read_functions = {
            'file': self._read_file,
            'table': self._read_table,
            'query': self._read_query
        }        

        return self
    
    def refresh(self):
        """Reloads the pipeline configuration from its source file."""
        self.logger.info(f"Refreshing pipeline configuration from '{self.pipeline_path}'")
        self.config = self._load_pipeline(self.pipeline_path)
        self.logger.info("Pipeline configurations refreshed.")

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

        for source in self.config.sources:
            source = source.model_copy()  # Create a copy to avoid modifying the original source
            if src_paths and source.name in src_paths:
                if not os.path.isabs(src_paths[source.name]):
                    source.path = os.path.join(source.path, src_paths[source.name])
                else:
                    source.path = src_paths[source.name]
            
            read_function = self.read_functions.get(source.type)

            if not read_function:
                raise ConfigurationError(f"Source type '{source.type}' is not supported by the engine.")

            self._sources[source.name] = read_function(source)

            # Apply schema
            if apply_schema:
                self._sources[source.name] = self.engine.apply_schema(self._sources[source.name], source.schema_spec)

            # Apply constraints
            if apply_constraints:
                self._sources[source.name] = self.quality_manager.apply_schema_constraints(self._sources[source.name], source.schema_spec)
        
        self._target = self._sources[self.config.sources[0].name]

    
    def _read_file(self, source) -> Any:
        """Reads a file from the specified source configuration."""
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
        if self.project.paths.pipelines and not os.path.isabs(path):
            pipeline_dir = os.path.dirname(self.project.paths.pipelines)
            pipeline_dir = os.path.join(self.project_dir, pipeline_dir, path)
        else:
            pipeline_dir = path
        
        # Find all .yml and .yaml files in the directory
        files = glob.glob(os.path.join(pipeline_dir, "*.yml")) + glob.glob(os.path.join(pipeline_dir, "*.yaml"))
        merged_dict = {}
        for file_path in files:
            with open(file_path, 'r') as file:
                yml_dict = yaml.safe_load(file) or {}
                merged_dict.update(yml_dict)
 
        return PipelineModel(**merged_dict)