from typing import Dict
import pandas as pd
from drune.core.step import BaseStep, register_step
from drune.utils.exceptions import ConfigurationError

@register_step('read')
class ReadStep(BaseStep):
    """Step responsible for reading data from a source using pandas."""
    def execute(self, sources: Dict[str, pd.DataFrame] = None, **kwargs) -> Dict[str, pd.DataFrame]:
        self.logger.info("--- Step: Read (Pandas) ---")

        sources_config = self.config.sources
        sources = {}

        if not sources_config:
            self.logger.error("No source configuration found.")
            raise ConfigurationError("Source configuration is missing.")
        
        for name, source_config in sources_config.items():
            sources[name] = self._read_source(source_config)

        return sources
        
    def _read_source(self, source_config) -> pd.DataFrame:
        """Reads data from the specified source configuration."""

        if source_config.format == 'csv':
            return pd.read_csv(source_config.path, **source_config.options)
        elif source_config.format == 'json':
            return pd.read_json(source_config.path, **source_config.options)
        elif source_config.format == 'parquet':
            return pd.read_parquet(source_config.path, **source_config.options)
        else:
            raise NotImplementedError(f"Source format '{source_config.format}' is not supported by the pandas engine.")
