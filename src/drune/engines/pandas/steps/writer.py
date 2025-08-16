import os
from typing import Dict
import pandas as pd
from ....core.step import BaseStep, register_step
from ....utils.exceptions import ConfigurationError

@register_step('write')
class WriteStep(BaseStep):
    """Step responsible for writing a pandas DataFrame to a destination."""
    def execute(self, sources: Dict[str, pd.DataFrame] = None, **kwargs) -> Dict[str, pd.DataFrame]:
        self.logger.info("--- Step: Write (Pandas) ---")

        df = sources.get('_output')

        sink_config = self.config.sink
        
        if not sink_config:
            raise ConfigurationError("'sink' configuration not found.")

        filename = f"{self.config.pipeline_name}.{sink_config.format}"
        output_file_path = os.path.join(sink_config.path, filename)

        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        if sink_config.format == 'csv':
            df.to_csv(output_file_path, index=False)
        elif sink_config.format == 'json':
            df.to_json(output_file_path, orient='records')
        elif sink_config.format == 'parquet':
            df.to_parquet(output_file_path, index=False)
        else:
            raise NotImplementedError(f"Sink format '{sink_config.format}' is not supported by the pandas engine.")
        
        return df
