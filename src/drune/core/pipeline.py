import os
from typing import Optional
import yaml
from drune.models import PipelineModel
from drune.models import ProjectModel

from drune.core.engine import get_engine
import drune.engines  as engines # Ensure engines are imported to register them
from drune.utils.logger import get_logger
from drune.utils.exceptions import ConfigurationError

class Pipeline:
    def __init__(self, pipeline_path: str, project_path: str = 'drune.yml'):
        """Initializes the Pipeline with the given configuration."""
        self.global_config = self._load_project(project_path)
        
        self.pipeline_path = pipeline_path

        if self.global_config.pipelines and not os.path.isabs(pipeline_path):
            project_dir = os.path.dirname(project_path)
            self.pipeline_path = os.path.join(project_dir, self.global_config.pipelines, pipeline_path)

        self.config = self._load_pipeline(self.pipeline_path)

        engine_class = get_engine(self.config.engine)
        self.engine = engine_class(self.config)
        self.logger = get_logger(f"pipeline.{self.config.pipeline_name}")

    
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
        self.engine.run()
        self.logger.info("Pipeline run finished.")

    def test(self):
        """Executes the pipeline in test mode."""
        if not self.config.test:
            self.logger.error("Test configuration ('test:') not found in YAML file.")
            raise ConfigurationError("Test configuration is missing.")

        self.logger.info(f"Starting test mode for pipeline: {self.config.pipeline_name}")
        self.engine.test(self.config)
        self.logger.info(f"Test mode for pipeline: {self.config.pipeline_name} finished.")
    
    def _load_yml(self, path: str) -> dict:
        """Loads a YAML file and returns its content."""
        
    
    def _load_pipeline(self, path: str) -> PipelineModel:
        """Loads a pipeline configuration from a YAML file."""
        with open(path, 'r') as file:
            yml_dict = yaml.safe_load(file)

        return PipelineModel(**yml_dict)
    
    def _load_project(self, path: str) -> ProjectModel:
        """Loads a project configuration from a YAML file."""
        with open(path, 'r') as file:
            yml_dict = yaml.safe_load(file)

        return ProjectModel(**yml_dict)
