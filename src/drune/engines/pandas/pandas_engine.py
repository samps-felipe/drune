import os
from typing import Any, Type, List
from drune.core.engine import BaseEngine, register_engine
from drune.core.step import get_step
from drune.models import ProjectModel, ColumnSpec
from drune.utils.logger import get_logger
from .steps import *
from drune.utils.exceptions import ConfigurationError, ConstraintError

@register_engine('pandas')
class PandasEngine(BaseEngine):
    def __init__(self, config: ProjectModel):
        import pandas as pd
        self.name = 'pandas'
        self.config = config
        self.pd = pd
        self.logger = get_logger("engine:pandas")
    
    def read_file(self, source) -> Any:
        """Reads data from the specified source configuration."""
        self.logger.info(f"Reading source: {source.name}")

        if source.format == 'csv':
            return self.pd.read_csv(source.path, **source.options)
        elif source.format == 'json':
            return self.pd.read_json(source.path, **source.options)
        elif source.format == 'parquet':
            return self.pd.read_parquet(source.path, **source.options)
        else:
            raise NotImplementedError(f"Source format '{source.format}' is not supported by the pandas engine.")
        
    def read_table(self, table_name: str) -> Any:
        """Reads a table from the database using the given table name."""
        raise NotImplementedError("PandasEngine does not support reading tables from a database.")
    
    def execute_query(self, query: str) -> Any:
        """Executes a SQL query and returns the result."""
        raise NotImplementedError("PandasEngine does not support executing SQL queries.")
    
    def write(self, df, target_config):
        self.logger.info("--- Step: Write (Pandas) ---")
        
        if not target_config:
            raise ConfigurationError("'Target' configuration not found.")

        filename = f"{target_config.name}.{target_config.format}"
        output_file_path = os.path.join(target_config.path, filename)

        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        if target_config.format == 'csv':
            df.to_csv(output_file_path, index=False)
        elif target_config.format == 'json':
            df.to_json(output_file_path, orient='records')
        elif target_config.format == 'parquet':
            df.to_parquet(output_file_path, index=False)
        else:
            raise NotImplementedError(f"Sink format '{target_config.format}' is not supported by the pandas engine.")
        
        return df
    
    def _apply_transformations(self, series: Any, transformations: List[str]) -> Any:
        """Applies a list of transformations to a pandas Series."""
        if not isinstance(series.dtype, (self.pd.StringDtype, object)):
             self.logger.warning(f"Transformations can only be applied to string columns. Column '{series.name}' has type {series.dtype}. Skipping.")
             return series
        for transform in transformations:
            if transform == 'trim':
                series = series.str.strip()
            elif transform == 'upper':
                series = series.str.upper()
            elif transform == 'lower':
                series = series.str.lower()
            else:
                self.logger.warning(f"Transformation '{transform}' not supported for column '{series.name}'. Skipping.")
        return series

    def _apply_type_casting(self, series: Any, col_spec: ColumnSpec) -> Any:
        """Applies type casting to a pandas Series based on ColumnSpec."""
        target_type = col_spec.type.lower()
        
        type_map = {
            'str': 'string',
            'int': 'Int64',
            'long': 'Int64',
            'float': 'float64',
            'double': 'float64',
            'bool': 'boolean',
            'date': 'datetime64[ns]',
            'timestamp': 'datetime64[ns]',
        }
        
        pd_type = type_map.get(target_type)

        if not pd_type:
            self.logger.warning(f"Type '{col_spec.type}' not supported for casting column '{series.name}'. Skipping.")
            return series

        if col_spec.try_cast:
            if pd_type in ['Int64', 'float64']:
                series = self.pd.to_numeric(series, errors='coerce')
                if pd_type == 'Int64':
                    if series.isnull().all():
                        return series.astype('object').where(series.notna(), None).astype('Int64')
                    else:
                        return series.astype('Int64')
                else:
                    return series.astype(pd_type)
            elif 'datetime' in pd_type:
                return self.pd.to_datetime(series, errors='coerce')
            else:
                try:
                    return series.astype(pd_type)
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"Could not cast column '{series.name}' to '{pd_type}' with try_cast. Error: {e}")
                    return series
        else:
            try:
                if 'datetime' in pd_type:
                    return self.pd.to_datetime(series, format=col_spec.format)
                else:
                    return series.astype(pd_type)
            except (ValueError, TypeError) as e:
                self.logger.error(f"Failed to cast column '{series.name}' to '{pd_type}'. Error: {e}")
                raise ConfigurationError(f"Failed to cast column '{series.name}' to '{pd_type}'.") from e
                
    def apply_schema(self, df, schema) -> Any:
        """Applies the schema to the DataFrame based on a list of ColumnSpec objects."""
        if not schema or not schema.columns:
            self.logger.info("No schema or columns to apply.")
            return df

        self.logger.info("Applying schema to the DataFrame.")
        
        rename_map = {}
        final_cols = []

        for col_spec in schema.columns:
            source_col_name = col_spec.name
            final_col_name = col_spec.rename or source_col_name

            if source_col_name not in df.columns:
                if col_spec.optional:
                    self.logger.info(f"Optional column '{source_col_name}' not found. Skipping.")
                    continue
                else:
                    raise ConfigurationError(f"Required column '{source_col_name}' not found in DataFrame.")

            # Apply transformations
            if col_spec.transform:
                df[source_col_name] = self._apply_transformations(df[source_col_name], col_spec.transform)

            # Apply type casting
            df[source_col_name] = self._apply_type_casting(df[source_col_name], col_spec)

            if source_col_name != final_col_name:
                rename_map[source_col_name] = final_col_name
            
            final_cols.append(final_col_name)

        # Rename columns
        df.rename(columns=rename_map, inplace=True)

        # Ensure all final columns are in the DataFrame
        for col in final_cols:
            if col not in df.columns:
                 raise ConfigurationError(f"Column '{col}' was expected in the final DataFrame but was not found after processing.")

        # Select and reorder columns
        unspecified_cols = [col for col in df.columns if col not in final_cols]
        df = df[final_cols + unspecified_cols]
        
        self.logger.info("Schema applied successfully.")
        return df
    
    def write_list(self, fails_list: List[Any], path: str):
        """Writes a list of DataFrames to the specified path."""
        if not fails_list:
            self.logger.info("No failed records to write.")
            return

        # Concatenate all failed dataframes into one

        common_columns = list(set.intersection(*[set(df.columns) for df in fails_list]))
        fails_list = [df[common_columns] for df in fails_list]

        all_fails_df = self.pd.concat(fails_list, ignore_index=True)
        
        # Define the output file path
        output_file_path = os.path.join(path, "failed_records.csv")

        # Write to CSV
        if os.path.exists(output_file_path):
            all_fails_df.to_csv(output_file_path, mode='a', header=False, index=False)
        else:
            all_fails_df.to_csv(output_file_path, mode='w', header=True, index=False)
        
        self.logger.info(f"Wrote {len(all_fails_df)} failed records to {output_file_path}")