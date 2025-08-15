import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from drune.core.step import BaseStep, register_step, get_step
from drune.core.engine import BaseEngine
from drune.models.pydantic_models import PipelineConfig, SourceConfig, SinkConfig, ColumnSpec, DefaultsConfig, ValidationRuleConfig, TestConfig, ExpectedResultsDataConfig, SourceDataConfig, TransformationStepConfig

from drune.utils.exceptions import ConfigurationError, StepExecutionError, ValidationError

# Import specific step implementations for testing
from drune.engines.pandas.steps.reader import ReadStep as PandasReadStep
from drune.engines.pandas.steps.transformer import TransformStep as PandasTransformStep
from drune.engines.pandas.steps.validator import ValidateStep as PandasValidateStep
from drune.engines.pandas.steps.writer import WriteStep as PandasWriteStep

from drune.engines.spark.steps.reader import ReadStep as SparkReadStep
from drune.engines.spark.steps.tester import TestStep as SparkTestStep
from drune.engines.spark.steps.transformer import TransformStep as SparkTransformStep
from drune.engines.spark.steps.validator import ValidateStep as SparkValidateStep
from drune.engines.spark.steps.writer import WriterStep as SparkWriterStep


# Fixtures
@pytest.fixture
def mock_engine():
    engine = MagicMock(spec=BaseEngine)
    engine.config = MagicMock(spec=PipelineConfig)
    engine.config.source = MagicMock(spec=SourceConfig)
    engine.config.sink = MagicMock(spec=SinkConfig)
    engine.config.columns = []
    engine.config.defaults = DefaultsConfig()
    engine.config.pipeline_name = "test_pipeline"
    engine.config.pipeline_type = "silver"
    engine.config.test = MagicMock(spec=TestConfig)
    engine.config.test.source_data = MagicMock(spec=SourceDataConfig)
    engine.config.test.expected_results_data = MagicMock(spec=ExpectedResultsDataConfig)
    return engine

@pytest.fixture
def mock_spark_engine(spark: SparkSession):
    engine = MagicMock(spec=BaseEngine)
    engine.spark = spark
    engine.config = MagicMock(spec=PipelineConfig)
    engine.config.source = MagicMock(spec=SourceConfig)
    engine.config.sink = MagicMock(spec=SinkConfig)
    engine.config.columns = []
    engine.config.defaults = DefaultsConfig()
    engine.config.pipeline_name = "test_pipeline"
    engine.config.pipeline_type = "silver"
    engine.config.test = MagicMock(spec=TestConfig)
    engine.config.test.source_data = MagicMock(spec=SourceDataConfig)
    engine.config.test.expected_results_data = MagicMock(spec=ExpectedResultsDataConfig)
    engine._get_final_column_name = MagicMock(side_effect=lambda spec, defaults: spec.rename or spec.name)
    engine.compare_dataframes = MagicMock(return_value=True)
    engine.show_differences = MagicMock()
    engine.execute_gold_transformation = MagicMock()
    return engine

@pytest.fixture
def dummy_pandas_df():
    return pd.DataFrame({"col1": [1, 2, 3], "col2": ["A", "B", "C"]})

@pytest.fixture
def dummy_spark_df(spark: SparkSession):
    schema = StructType([
        StructField("col1", IntegerType(), True),
        StructField("col2", StringType(), True)
    ])
    return spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], schema)


# Test register_step and get_step
def test_step_registration():
    class MyStep(BaseStep):
        def execute(self, previous_result, options=None): pass

    register_step("my_test_step", MyStep)
    retrieved_step = get_step("my_test_step")
    assert retrieved_step == MyStep

    @register_step("another_test_step")
    class AnotherStep(BaseStep):
        def execute(self, previous_result, options=None): pass

    retrieved_another_step = get_step("another_test_step")
    assert retrieved_another_step == AnotherStep

    # Test overwriting
    class OverwriteStep(BaseStep):
        def execute(self, previous_result, options=None): pass

    register_step("my_test_step", OverwriteStep)
    retrieved_overwritten_step = get_step("my_test_step")
    assert retrieved_overwritten_step == OverwriteStep

def test_get_non_existent_step():
    assert get_step("non_existent_step") is None

# Test BaseStep abstract methods
def test_base_step_instantiation_fails():
    with pytest.raises(TypeError):
        BaseStep(engine=MagicMock(spec=BaseEngine))


# --- Pandas Steps Tests ---

# Pandas ReadStep
def test_pandas_read_step_csv(mock_engine: MagicMock, dummy_pandas_df: pd.DataFrame):
    mock_engine.config.source.format = "csv"
    mock_engine.config.source.path = "dummy.csv"
    mock_engine.config.source.options = {"header": True}
    with patch('pandas.read_csv', return_value=dummy_pandas_df) as mock_read_csv:
        step = PandasReadStep(mock_engine)
        result_df = step.execute()
        mock_read_csv.assert_called_once_with("dummy.csv", header=True)
        pd.testing.assert_frame_equal(result_df, dummy_pandas_df)

def test_pandas_read_step_unsupported_format(mock_engine: MagicMock):
    mock_engine.config.source.format = "unsupported"
    mock_engine.config.source.path = "dummy.xyz"
    step = PandasReadStep(mock_engine)
    with pytest.raises(NotImplementedError, match="Source format 'unsupported' is not supported by the pandas engine."):
        step.execute()

def test_pandas_read_step_missing_source_config(mock_engine: MagicMock):
    mock_engine.config.source = None
    step = PandasReadStep(mock_engine)
    with pytest.raises(ConfigurationError, match="'source' configuration not found."):
        step.execute()

# Pandas TransformStep
def test_pandas_transform_step_rename_and_eval(mock_engine: MagicMock):
    df_input = pd.DataFrame({"old_col1": [1, 2], "old_col2": ["a", "b"]})
    mock_engine.config.columns = [
        ColumnSpec(name="old_col1", rename="new_col1", type="int"),
        ColumnSpec(name="old_col2", rename="new_col2", type="string", transform="old_col2.str.upper()")
    ]
    step = PandasTransformStep(mock_engine)
    result_df = step.execute(df_input)
    expected_df = pd.DataFrame({"new_col1": [1, 2], "new_col2": ["A", "B"]})
    pd.testing.assert_frame_equal(result_df, expected_df)

# Pandas WriteStep
def test_pandas_write_step_csv(mock_engine: MagicMock, dummy_pandas_df: pd.DataFrame):
    mock_engine.config.sink.format = "csv"
    mock_engine.config.sink.path = "./temp_output"
    mock_engine.config.pipeline_name = "test_pipeline"
    
    with patch('pandas.DataFrame.to_csv') as mock_to_csv, patch('os.makedirs') as mock_makedirs, patch('os.path.dirname', return_value='./temp_output'):
         step = PandasWriteStep(mock_engine)
         step.execute(dummy_pandas_df)
         mock_to_csv.assert_called_once_with('./temp_output/test_pipeline.csv', index=False)
         mock_makedirs.assert_called_once_with('./temp_output', exist_ok=True)


# --- Spark Steps Tests ---

# Spark ReadStep
def test_spark_read_step_path(mock_spark_engine: MagicMock, dummy_spark_df: Any):
    mock_spark_engine.config.source.path = "/tmp/data.csv"
    mock_spark_engine.config.source.format = "csv"
    mock_spark_engine.config.pipeline_type = "silver"
    with patch.object(mock_spark_engine.spark.read, 'format', return_value=MagicMock(load=MagicMock(return_value=dummy_spark_df))) as mock_read_format:
        step = SparkReadStep(mock_spark_engine)
        result_df = step.execute()
        mock_read_format.assert_called_once_with("csv")
        mock_read_format.return_value.load.assert_called_once_with("/tmp/data.csv")
        assert result_df == dummy_spark_df

def test_spark_read_step_gold_pipeline_skips_read(mock_spark_engine: MagicMock):
    mock_spark_engine.config.pipeline_type = "gold"
    step = SparkReadStep(mock_spark_engine)
    result = step.execute()
    assert result is None

def test_spark_read_step_expected_columns_mismatch(mock_spark_engine: MagicMock, dummy_spark_df: Any):
    mock_spark_engine.config.source.path = "/tmp/data.csv"
    mock_spark_engine.config.source.format = "csv"
    mock_spark_engine.config.source.expected_columns = 5 # Mismatch
    mock_spark_engine.config.pipeline_type = "silver"
    with patch.object(mock_spark_engine.spark.read, 'format', return_value=MagicMock(load=MagicMock(return_value=dummy_spark_df))):
        step = SparkReadStep(mock_spark_engine)
        with pytest.raises(ConfigurationError, match="Schema validation failed!"):
            step.execute()

# Spark TransformStep
def test_spark_transform_step_silver(mock_spark_engine: MagicMock, dummy_spark_df: Any):
    mock_spark_engine.config.pipeline_type = "silver"
    mock_spark_engine.config.columns = [
        ColumnSpec(name="col1", rename="new_col1", type="integer"),
        ColumnSpec(name="col2", rename="new_col2", type="string", transform="upper(col2)")
    ]
    with (patch.object(dummy_spark_df, 'select', return_value=dummy_spark_df) as mock_select,
          patch('declarative_data_framework.engines.spark.steps.transformer.sha2', return_value=MagicMock()),
          patch('declarative_data_framework.engines.spark.steps.transformer.concat_ws', return_value=MagicMock()),
          patch('declarative_data_framework.engines.spark.steps.transformer.current_timestamp', return_value=MagicMock())):
        step = SparkTransformStep(mock_spark_engine)
        result_df = step.execute(dummy_spark_df)
        mock_select.assert_called_once()
        assert result_df == dummy_spark_df

def test_spark_transform_step_gold(mock_spark_engine: MagicMock):
    mock_spark_engine.config.pipeline_type = "gold"
    mock_spark_engine.config.transformation = [
        TransformationStepConfig(name="step1", type="sql", sql="SELECT 1 as col")
    ]
    mock_spark_engine.execute_gold_transformation.return_value = MagicMock(spec=DataFrame)
    mock_spark_engine.config.columns = [
        ColumnSpec(name="col", type="integer")
    ]
    with patch('declarative_data_framework.engines.spark.steps.transformer.sha2', return_value=MagicMock()),
         patch('declarative_data_framework.engines.spark.steps.transformer.concat_ws', return_value=MagicMock()),
         patch('declarative_data_framework.engines.spark.steps.transformer.current_timestamp', return_value=MagicMock()):
        step = SparkTransformStep(mock_spark_engine)
        result_df = step.execute(None) # Gold transform doesn't take df as input
        mock_spark_engine.execute_gold_transformation.assert_called_once_with(mock_spark_engine.config)
        assert result_df is not None

# Spark TestStep
def test_spark_test_step_success(mock_spark_engine: MagicMock):
    mock_spark_engine.config.test.source_data.path = "/tmp/test_source.csv"
    mock_spark_engine.config.test.expected_results_data.table = "expected_table"
    mock_spark_engine.compare_dataframes.return_value = True

    mock_read_step = MagicMock(spec=SparkReadStep)
    mock_transform_step = MagicMock(spec=SparkTransformStep)
    mock_validate_step = MagicMock(spec=SparkValidateStep)

    mock_read_step.execute.return_value = MagicMock(spec=DataFrame)
    mock_transform_step.execute.return_value = MagicMock(spec=DataFrame)
    mock_validate_step.execute.return_value = (MagicMock(spec=DataFrame), MagicMock(spec=DataFrame)) # df, log_df

    with patch('declarative_data_framework.engines.spark.steps.tester.ReadStep', return_value=mock_read_step),
         patch('declarative_data_framework.engines.spark.steps.tester.TransformStep', return_value=mock_transform_step),
         patch('declarative_data_framework.engines.spark.steps.tester.ValidateStep', return_value=mock_validate_step),
         patch.object(mock_spark_engine.spark.read, 'table', return_value=MagicMock(spec=DataFrame)):
        step = SparkTestStep(mock_spark_engine)
        step.execute()
        mock_spark_engine.compare_dataframes.assert_called_once()
        mock_spark_engine.show_differences.assert_not_called()

def test_spark_test_step_failure(mock_spark_engine: MagicMock):
    mock_spark_engine.config.test.source_data.path = "/tmp/test_source.csv"
    mock_spark_engine.config.test.expected_results_data.table = "expected_table"
    mock_spark_engine.compare_dataframes.return_value = False

    mock_read_step = MagicMock(spec=SparkReadStep)
    mock_transform_step = MagicMock(spec=SparkTransformStep)
    mock_validate_step = MagicMock(spec=SparkValidateStep)

    mock_read_step.execute.return_value = MagicMock(spec=DataFrame)
    mock_transform_step.execute.return_value = MagicMock(spec=DataFrame)
    mock_validate_step.execute.return_value = (MagicMock(spec=DataFrame), MagicMock(spec=DataFrame))

    with patch('declarative_data_framework.engines.spark.steps.tester.ReadStep', return_value=mock_read_step),
         patch('declarative_data_framework.engines.spark.steps.tester.TransformStep', return_value=mock_transform_step),
         patch('declarative_data_framework.engines.spark.steps.tester.ValidateStep', return_value=mock_validate_step),
         patch.object(mock_spark_engine.spark.read, 'table', return_value=MagicMock(spec=DataFrame)):
        step = SparkTestStep(mock_spark_engine)
        with pytest.raises(AssertionError, match="The test result does not match the expected result."):
            step.execute()
        mock_spark_engine.compare_dataframes.assert_called_once()
        mock_spark_engine.show_differences.assert_called_once()

def test_spark_test_step_missing_test_config(mock_spark_engine: MagicMock):
    mock_spark_engine.config.test = None
    step = SparkTestStep(mock_spark_engine)
    with pytest.raises(ConfigurationError, match="Test configuration is missing."):
        step.execute()

# Spark WriterStep
def test_spark_writer_step_standard_write(mock_spark_engine: MagicMock, dummy_spark_df: Any):
    mock_spark_engine.config.sink.mode = "overwrite"
    mock_spark_engine.config.sink.catalog = "test_catalog"
    mock_spark_engine.config.sink.schema_name = "test_schema"
    mock_spark_engine.config.sink.table = "test_table"

    with patch.object(dummy_spark_df.write, 'mode', return_value=MagicMock(saveAsTable=MagicMock())) as mock_write_mode:
        step = SparkWriterStep(mock_spark_engine)
        step.execute(dummy_spark_df)
        mock_write_mode.assert_called_once_with("overwrite")
        mock_write_mode.return_value.saveAsTable.assert_called_once_with("test_catalog.test_schema.test_table")

def test_spark_writer_step_validation_log_write(mock_spark_engine: MagicMock, dummy_spark_df: Any):
    mock_spark_engine.config.sink.mode = "overwrite"
    mock_spark_engine.config.validation_log_table = "test_log_table"
    mock_validation_log_df = MagicMock(spec=DataFrame)

    with patch.object(dummy_spark_df.write, 'mode', return_value=MagicMock(saveAsTable=MagicMock())),
         patch.object(mock_validation_log_df.write, 'mode', return_value=MagicMock(saveAsTable=MagicMock())) as mock_log_write_mode:
        step = SparkWriterStep(mock_spark_engine)
        step.execute(dummy_spark_df, validation_log_df=mock_validation_log_df)
        mock_log_write_mode.assert_called_once_with("append")
        mock_log_write_mode.return_value.saveAsTable.assert_called_once_with("test_log_table")
