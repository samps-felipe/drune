import pandas as pd
from ....core.step import BaseStep, register_step
from ....utils.exceptions import ConfigurationError

@register_step('read')
class ReadStep(BaseStep):
    """Step responsible for reading data from a source using pandas."""
    def execute(self, df: pd.DataFrame = None, **kwargs) -> pd.DataFrame:
        self.logger.info("--- Step: Read (Pandas) ---")

        source_config = self.config.source
        if not source_config:
            raise ConfigurationError("'source' configuration not found.")

        if source_config.format == 'csv':
            return pd.read_csv(source_config.path, **source_config.options)
        elif source_config.format == 'json':
            return pd.read_json(source_config.path, **source_config.options)
        elif source_config.format == 'parquet':
            return pd.read_parquet(source_config.path, **source_config.options)
        else:
            raise NotImplementedError(f"Source format '{source_config.format}' is not supported by the pandas engine.")
