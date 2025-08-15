import pandas as pd
from ....core.step import BaseStep, register_step

@register_step('transform')
class TransformStep(BaseStep):
    """Step responsible for applying transformations to a pandas DataFrame."""
    def execute(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        self.logger.info("--- Step: Transform (Pandas) ---")

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

        return df
