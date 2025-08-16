from typing import Dict
import pandas as pd
from drune.core.step import BaseStep, register_step

@register_step('transform')
class TransformStep(BaseStep):
    """Step responsible for applying transformations to a pandas DataFrame."""
    def execute(self, sources: Dict[str, pd.DataFrame] = None, **kwargs) -> Dict[str, pd.DataFrame]:
        self.logger.info("--- Step: Transform (Pandas) ---")

        if len(sources) != 1:
            self.logger.error("Transform step expects exactly one source DataFrame.")
            raise ValueError("Transform step requires exactly one source DataFrame.")
        
        # TODO: deal with multiple sources if needed
        # Assuming sources is a dictionary with one DataFrame
        df = sources[list(sources.keys())[0]]

        # Apply column renaming
        rename_map = {
            spec.name: spec.rename
            for spec in self.config.columns
            if spec.name and spec.rename and spec.name in df.columns
        }
        if rename_map:
            df = df.rename(columns=rename_map)

        # Apply transformations
        for spec in self.config.columns:
            final_name = spec.rename or spec.name
            if spec.transform and final_name in df.columns:
                # This is a simplified transformation using eval. 
                # For more complex scenarios, a more robust solution is needed.
                try:
                    df[final_name] = df.eval(spec.transform)
                except Exception as e:
                    self.logger.error(f"Could not apply transform '{spec.transform}' on column '{final_name}': {e}")

        # Select and order columns
        final_columns = [spec.rename or spec.name for spec in self.config.columns]
        df = df[[col for col in final_columns if col in df.columns]]

        sources['_output'] = df
        self.logger.info("Transform step completed successfully.")

        return sources
