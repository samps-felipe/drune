from pyspark.sql import DataFrame
from ....core.step import BaseStep, register_step
from ....utils.exceptions import ConfigurationError
from .reader import ReadStep
from .transformer import TransformStep
from .validator import ValidateStep

@register_step('test')
class TestStep(BaseStep):
    def execute(self, df: DataFrame = None, **kwargs) -> None:
        """Executes the pipeline in test mode."""
        if not self.config.test:
            self.logger.error("Test configuration ('test:') not found in YAML file.")
            raise ConfigurationError("Test configuration is missing.")

        self.logger.info(f"--- Step: Test (Spark) ---")
        
        test_run_config = self.config.copy(deep=True)
        test_run_config.source.path = self.config.test.source_data.path
        
        reader = ReadStep(self.engine)
        transformer = TransformStep(self.engine)
        validator = ValidateStep(self.engine)

        self.logger.debug("Executing steps with test data...")
        df_source = reader.execute()
        df_transformed = transformer.execute(df_source)
        df_actual_output, _ = validator.execute(df_transformed)

        self.logger.info(f"Loading expected results from: {self.config.test.expected_results_data.table}")
        df_expected_output = self.engine.spark.read.table(self.config.test.expected_results_data.table)

        volatile_cols = ["updated_at", "created_at", "start_date", "end_date", "log_timestamp"]
        actual_to_compare = df_actual_output.drop(*[c for c in volatile_cols if c in df_actual_output.columns])
        expected_to_compare = df_expected_output.drop(*[c for c in volatile_cols if c in df_expected_output.columns])

        self.logger.info("Comparing actual result with expected result...")
        are_equal = self.engine.compare_dataframes(actual_to_compare, expected_to_compare)

        if are_equal:
            self.logger.info("TEST PASSED: The actual result matches the expected result.")
        else:
            self.logger.error("TEST FAILED: The actual result is different from the expected result.")
            self.engine.show_differences(actual_to_compare, expected_to_compare)
            raise AssertionError("The test result does not match the expected result.")