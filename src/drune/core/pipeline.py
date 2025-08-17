import os
from typing import Optional
import yaml
from drune.core.step import get_step
from drune.models import PipelineModel
from drune.models import ProjectModel

from drune.core.engine import get_engine
import drune.engines  as engines # Ensure engines are imported to register them

from drune.core.metadata import get_Metadata
import drune.metadata as metadata # Ensure metadata are imported to register them

from drune.utils.logger import get_logger
from drune.utils.exceptions import ConfigurationError
import glob

class Pipeline:
    def __init__(self, pipeline_path: str, project_path: str = 'drune.yml'):
        """Initializes the Pipeline with the given configuration."""
        self._load_project(project_path)
        self._load_pipeline(pipeline_path)

        engine_class = get_engine(self.config.engine)
        self.engine = engine_class(self.project)

        self.logger = get_logger(f"pipeline:{self.config.name}")

    
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
    
    def read(self, path: Optional[str] = None):
        """Reads data from the source defined in the pipeline configuration."""
        self.logger.info(f"Starting data read for: {self.config.pipeline_name}")
        data = self.engine.read(path)
        self.logger.info("Data read finished.")
        return data
    
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
            project_dir = os.path.dirname(self.project_path)
            pipeline_dir = os.path.dirname(self.project.paths.pipelines)
            pipeline_dir = os.path.join(project_dir, pipeline_dir, path)
        else:
            pipeline_dir = path
        
        # Find all .yml and .yaml files in the directory
        files = glob.glob(os.path.join(pipeline_dir, "*.yml")) + glob.glob(os.path.join(pipeline_dir, "*.yaml"))
        merged_dict = {}
        for file_path in files:
            with open(file_path, 'r') as file:
                yml_dict = yaml.safe_load(file) or {}
                merged_dict.update(yml_dict)
 
        spec = PipelineModel(**merged_dict)
        
        self.config = spec.pipeline
        self.target = spec.target
        self.sources = spec.sources
        self.steps = spec.steps

    
    def _load_project(self, path: str) -> ProjectModel:
        """Loads a project configuration from a YAML file."""
        self.project_path = path

        with open(path, 'r') as file:
            yml_dict = yaml.safe_load(file)
        
        self.project = ProjectModel(**yml_dict)
    
