from pyspark.sql import DataFrame
from ....core.step import BaseStep, register_step
from ....utils.exceptions import ConfigurationError, StepExecutionError

@register_step('read')
class ReadStep(BaseStep):
    """Step responsible for reading data from the source."""
    def execute(self, df: DataFrame = None, **kwargs) -> DataFrame:
        self.logger.info("--- Step: Read (Spark) ---")

        source_config = self.config.source
        pipeline_name = self.config.pipeline_name
        pipeline_type = self.config.pipeline_type

        if pipeline_type == 'gold':
            self.logger.info("Gold pipeline does not read from files. Skipping READ step.")
            return None

        try:
            if not source_config:
                raise ConfigurationError("'source' configuration not found for a pipeline that requires file reading.")
            
            reader = self.engine.spark.read
            if source_config.format:
                reader = reader.format(source_config.format)
            if source_config.options:
                reader.options(**source_config.options)

            if source_config.path:
                df = reader.load(source_config.path)
            elif source_config.table:
                df = reader.table(source_config.table)
            elif source_config.query:
                df = self.engine.spark.sql(source_config.query)
            else:
                raise ConfigurationError("Either 'path', 'table' or 'query' must be specified in the source configuration.")
            
        except Exception as e:
            self.logger.exception("Failure in READ step for pipeline '%s'", pipeline_name)
            raise StepExecutionError(f"Failure in READ step: {e}") from e
    
        if source_config.expected_columns:
            num_actual_columns = len(df.columns)
            if num_actual_columns != source_config.expected_columns:
                delimiter = source_config.options.get('delimiter', '[NOT SPECIFIED]')
                msg = (
                    f"Schema validation failed! File was read with {num_actual_columns} columns, "
                    f"but {source_config.expected_columns} were expected. "
                    f"Please check if the delimiter ('{delimiter}') is correct."
                )
                self.logger.error(msg)
                raise ConfigurationError(msg)
        
        self.logger.info("Read and initial schema validation completed.")
        return df
