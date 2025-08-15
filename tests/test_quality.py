import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from drune.core.quality import BaseValidation, BaseTableValidation, register_rule, get_validationrule

# Pandas Rules
from drune.engines.pandas.quality.rules import NotNullValidation as PandasNotNullValidation
from drune.engines.pandas.quality.rules import IsInValidation as PandasIsInValidation

# Spark Rules
from drune.engines.spark.quality.rules import NotNullValidation as SparkNotNullValidation
from drune.engines.spark.quality.rules import PatternValidation as SparkPatternValidation
from drune.engines.spark.quality.rules import IsInValidation as SparkIsInValidation
from drune.engines.spark.quality.rules import GreaterThanOrEqualToValidation as SparkGreaterThanOrEqualToValidation
from drune.engines.spark.quality.rules import IsBetweenValidation as SparkIsBetweenValidation
from drune.engines.spark.quality.rules import DuplicateCheckValidation as SparkDuplicateCheckValidation


# Fixtures
@pytest.fixture
def dummy_pandas_df():
    return pd.DataFrame({"col1": [1, 2, None, 4], "col2": ["A", "B", "C", "A"]})

@pytest.fixture
def dummy_spark_df(spark):
    schema = StructType([
        StructField("col1", IntegerType(), True),
        StructField("col2", StringType(), True)
    ])
    return spark.createDataFrame([(1, "A"), (2, "B"), (None, "C"), (4, "A")], schema)

@pytest.fixture
def dummy_spark_numeric_df(spark):
    schema = StructType([
        StructField("value", FloatType(), True)
    ])
    return spark.createDataFrame([(10.0,), (20.0,), (30.0,)], schema)


# Test register_rule and get_validationrule
def test_rule_registration():
    class MyRule(BaseValidation):
        def apply(self, df, column_name): pass

    register_rule("my_test_rule", MyRule)
    retrieved_rule = get_validationrule("my_test_rule")
    assert retrieved_rule == MyRule

    @register_rule("another_test_rule")
    class AnotherRule(BaseValidation):
        def apply(self, df, column_name): pass

    retrieved_another_rule = get_validationrule("another_test_rule")
    assert retrieved_another_rule == AnotherRule

    # Test overwriting
    class OverwriteRule(BaseValidation):
        def apply(self, df, column_name): pass

    register_rule("my_test_rule", OverwriteRule)
    retrieved_overwritten_rule = get_validationrule("my_test_rule")
    assert retrieved_overwritten_rule == OverwriteRule

def test_get_non_existent_rule():
    assert get_validationrule("non_existent_rule") is None

# Test BaseValidation and BaseTableValidation abstract methods
def test_base_validation_instantiation_fails():
    with pytest.raises(TypeError):
        BaseValidation()

def test_base_table_validation_instantiation_fails():
    with pytest.raises(TypeError):
        BaseTableValidation()


# --- Pandas Quality Rules Tests ---

def test_pandas_not_null_validation(dummy_pandas_df):
    validator = PandasNotNullValidation()
    failures, successes = validator.apply(dummy_pandas_df, "col1")
    assert len(failures) == 1
    assert failures.iloc[0]["col1"] is None
    assert len(successes) == 3
    assert None not in successes["col1"].tolist()

def test_pandas_isin_validation(dummy_pandas_df):
    validator = PandasIsInValidation({'allowed_values_str': "['A', 'B']"})
    failures, successes = validator.apply(dummy_pandas_df, "col2")
    assert len(failures) == 1
    assert failures.iloc[0]["col2"] == "C"
    assert len(successes) == 3
    assert "C" not in successes["col2"].tolist()


# --- Spark Quality Rules Tests ---

def test_spark_not_null_validation(dummy_spark_df):
    validator = SparkNotNullValidation()
    failures, successes = validator.apply(dummy_spark_df, "col1")
    assert failures.count() == 1
    assert successes.count() == 3

def test_spark_pattern_validation(dummy_spark_df):
    validator = SparkPatternValidation({'pattern': "A"})
    failures, successes = validator.apply(dummy_spark_df, "col2")
    assert failures.count() == 2 # B, C
    assert successes.count() == 2 # A, A

def test_spark_isin_validation(dummy_spark_df):
    validator = SparkIsInValidation({'allowed_values_str': "['A', 'B']"})
    failures, successes = validator.apply(dummy_spark_df, "col2")
    assert failures.count() == 1 # C
    assert successes.count() == 3 # A, B, A

def test_spark_isin_validation_invalid_param():
    with pytest.raises(ValueError, match="Invalid parameter for 'isin' rule."):
        SparkIsInValidation({'allowed_values_str': "not_a_list"})

def test_spark_greater_than_or_equal_to_validation(dummy_spark_numeric_df):
    validator = SparkGreaterThanOrEqualToValidation({'value_str': "20.0"})
    failures, successes = validator.apply(dummy_spark_numeric_df, "value")
    assert failures.count() == 1 # 10.0
    assert successes.count() == 2 # 20.0, 30.0

def test_spark_greater_than_or_equal_to_validation_invalid_param():
    with pytest.raises(ValueError, match="Invalid parameter for 'greater_than_or_equal_to'."):
        SparkGreaterThanOrEqualToValidation({'value_str': "not_a_number"})

def test_spark_is_between_validation(dummy_spark_numeric_df):
    validator = SparkIsBetweenValidation({'bounds_str': "[15.0, 25.0]"})
    failures, successes = validator.apply(dummy_spark_numeric_df, "value")
    assert failures.count() == 2 # 10.0, 30.0
    assert successes.count() == 1 # 20.0

def test_spark_is_between_validation_invalid_param():
    with pytest.raises(ValueError, match="Invalid parameter for 'isbetween'."):
        SparkIsBetweenValidation({'bounds_str': "[10]"})

def test_spark_duplicate_check_validation_no_duplicates(spark):
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "col"])
    validator = SparkDuplicateCheckValidation({'columns': ["id"]})
    validator.apply(df) # Should not raise error

def test_spark_duplicate_check_validation_with_duplicates(spark):
    df = spark.createDataFrame([(1, "A"), (1, "B")], ["id", "col"])
    validator = SparkDuplicateCheckValidation({'columns': ["id"]})
    with pytest.raises(ValueError, match="Duplicate check failed for columns: \['id'\]. 1 duplicates found."):
        validator.apply(df)
