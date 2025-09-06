from pathlib import Path
import yaml

from drune.utils.logger import get_logger

from .models import ProjectModel
from .engine import EngineManager
from .pipeline import Pipeline

class Drune:
    """
    The main entry point for a Drune project.

    It reads the main project configuration file (drune.yml) and serves as a
    factory for pipelines within the project.
    """

    def __init__(self, project_config_path: str = '.', profile: str = None):

        self._load_project_config(project_config_path, profile)
        self.logger = get_logger(f"project[{self.project_config.project_name}]")

        self._load_engine()

        self.pipeline = Pipeline(self.project_dir, self.project_config, self.engine)

    def _load_engine(self):
        """Lazily initializes and returns the configured engine."""
        self.logger.info(f"Loading engine: {self.project_config.defaults.engine.name}")
        engine_name = self.project_config.defaults.engine.name
        engine_options = self.project_config.defaults.engine.options

        self.engine = EngineManager.get_engine(engine_name, engine_options)
    
    def _load_project_config(self, path: str, profile: str):
        """Loads a project configuration from a YAML file."""

        project_config_file = Path(path).resolve()
        
        if project_config_file.is_dir():
            for ext in ['yml', 'yaml']:
                config_file = project_config_file / f"drune.{ext}"
                if config_file.is_file():
                    project_config_file = config_file
                    break
            else:
                raise FileNotFoundError(f"Drune project config (drune.yml or drune.yaml) not found in directory: {self.project_config_path}")
            
    
        # get only the dir path
        self.project_dir = project_config_file.parent
        
        with open(project_config_file, 'r') as file:
            yml_dict = yaml.safe_load(file)

        project_config = ProjectModel(**yml_dict)

        project_config.merge_defaults(profile)
        
        self.project_config = project_config