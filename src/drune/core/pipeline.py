import os
from typing import Any, Dict, Optional
import yaml
from drune.core.quality import DataQualityProcessor
from drune.core.step import get_step
from drune.models import PipelineModel
from drune.models import ProjectModel

from drune.core.engine import get_engine
import drune.engines  as engines # Ensure engines are imported to register them

from drune.core.metadata import get_metadata
import drune.metadata as metadata # Ensure metadata are imported to register them

from drune.utils.logger import get_logger
from drune.utils.exceptions import ConfigurationError
from drune.core.state import PipelineState
import glob



class Pipeline:
    def __init__(self, pipeline_path: str, project_path: str = 'drune.yml'):
        """Initializes the Pipeline with the given configuration."""

        self.project = self._load_project(project_path)
        self.config = self._load_pipeline(pipeline_path)

        engine_class = get_engine(self.config.pipeline.engine)
        self.engine = engine_class(self.project)

        self.logger = get_logger(f"pipeline:{self.config.pipeline.name}")

        self.state = PipelineState()

        self.read_functions = {
            'file': self._read_file,
            'table': self._read_table,
            'query': self._read_query
        }

        log_path = os.path.join(self.project_dir, self.project.paths.logs)

        self.quality = DataQualityProcessor(self.engine, log_path)

    
    def create(self):
        self.logger.info(f"Starting table creation for: {self.config.pipeline_name}")
        self.engine.create_table(self.config)
        self.logger.info("Table creation finished.")

    def update(self):
        self.logger.info(f"Starting table update for: {self.config.pipeline_name}")
        self.engine.update_table(self.config)
        self.logger.info("Table update finished.")

    def run(self, path: Optional[str] = None):
        self.logger.info(f"Starting pipeline run for: {self.config.pipeline_name}")
        self.engine.run(path)
        self.logger.info("Pipeline run finished.")
    
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

            self.state.sources[source.name] = read_function(source)

            # Apply schema
            if apply_schema:
                self.state.sources[source.name] = self.engine.apply_schema(self.state.sources[source.name], source.schema_spec)

            # Apply constraints
            if apply_constraints:
                self.state.sources[source.name] = self.quality.apply_schema_constraints(self.state.sources[source.name], source.schema_spec)       


    
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
 
    
    def run_steps(self, breakpoint: Optional[str] = None):
        """Runs the steps defined in the pipeline configuration."""
        self.logger.info(f"Starting step execution for pipeline: {self.config.pipeline_name}")
        
        df = None
        for step_config in self.config.steps:
            
            step_class = get_step(step_config.type)        
            step_instance = step_class(self.engine)
            df = step_instance.execute(df, **step_config.params)

            if breakpoint and step_config.name == breakpoint:
                self.logger.info(f"Breakpoint reached at step: {step_config.name}")
                break
        
        self.logger.info("Step execution finished.")
        return df
    
    
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

    
    def _load_project(self, path: str) -> ProjectModel:
        """Loads a project configuration from a YAML file."""
        if not os.path.isabs(path):
            path = os.path.abspath(path)

        # get only the dir path
        self.project_dir = os.path.dirname(path)
        
        with open(path, 'r') as file:
            yml_dict = yaml.safe_load(file)
        
        return ProjectModel(**yml_dict)
    
