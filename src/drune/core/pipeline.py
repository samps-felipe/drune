from .engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
from ..utils.logger import get_logger
from ..utils.exceptions import ConfigurationError

logger = get_logger(__name__)

class Pipeline:
    def __init__(self, config: PipelineConfig, engine: BaseEngine):
        self.config = config
        self.engine = engine
        self.logger = get_logger(f"pipeline.{self.config.pipeline_name}")
    
    def create(self):
        self.logger.info(f"Starting table creation for: {self.config.pipeline_name}")
        self.engine.create_table(self.config)
        self.logger.info("Table creation finished.")

    def update(self):
        self.logger.info(f"Starting table update for: {self.config.pipeline_name}")
        self.engine.update_table(self.config)
        self.logger.info("Table update finished.")

    def run(self):
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
