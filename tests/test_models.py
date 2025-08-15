import pytest
from pydantic import ValidationError
from drune.models.pydantic_models import (
    ValidationRule,
    ColumnSpec,
    ForeignKey,
    TableValidation,
    Defaults,
    SourceConfig,
    SCDConfig,
    SinkConfig,
    TestConfig,
    TransformationConfig,
    StepConfig,
    PipelineConfig,
    RESERVED_COLUMN_NAMES
)

# Test ValidationRule
def test_validation_rule_basic_instantiation():
    rule = ValidationRule(rule="not_null")
    assert rule.rule == "not_null"
    assert rule.on_fail == "fail"

def test_validation_rule_on_fail_values():
    rule = ValidationRule(rule="unique", on_fail="drop")
    assert rule.on_fail == "drop"
    rule = ValidationRule(rule="pattern", on_fail="warn")
    assert rule.on_fail == "warn"
    with pytest.raises(ValidationError):
        ValidationRule(rule="invalid", on_fail="invalid_action")

# Test ColumnSpec
def test_column_spec_name_or_rename_required():
    with pytest.raises(ValidationError, match="Each column must have either a 'name' or 'rename' field."):
        ColumnSpec(type="string")

def test_column_spec_name_and_rename_set_correctly():
    spec1 = ColumnSpec(name="original", type="string")
    assert spec1.name == "original"
    assert spec1.rename == "original"

    spec2 = ColumnSpec(rename="new", type="int")
    assert spec2.name == "new"
    assert spec2.rename == "new"

    spec3 = ColumnSpec(name="original", rename="new", type="float")
    assert spec3.name == "original"
    assert spec3.rename == "new"

def test_column_spec_reserved_names_rejected():
    for reserved_name in RESERVED_COLUMN_NAMES:
        with pytest.raises(ValidationError, match=f"The column name '{reserved_name}' is reserved by the framework."):
            ColumnSpec(name=reserved_name, type="string")
        with pytest.raises(ValidationError, match=f"The column name '{reserved_name}' is reserved by the framework."):
            ColumnSpec(rename=reserved_name, type="string")

def test_column_spec_basic_instantiation():
    spec = ColumnSpec(name="id", type="long", pk=True, validation_rules=[{"rule": "not_null"}])
    assert spec.name == "id"
    assert spec.type == "long"
    assert spec.pk is True
    assert len(spec.validation_rules) == 1
    assert spec.validation_rules[0].rule == "not_null"

# Test ForeignKey
def test_foreign_key_basic_instantiation():
    fk = ForeignKey(name="fk_order_customer", local_columns=["customer_id"], references_table="customers", references_columns=["id"])
    assert fk.name == "fk_order_customer"
    assert fk.local_columns == ["customer_id"]

# Test TableValidation
def test_table_validation_basic_instantiation():
    tv = TableValidation(type="duplicate_check", columns=["id", "name"])
    assert tv.type == "duplicate_check"
    assert tv.columns == ["id", "name"]
    assert tv.on_fail == "fail"

def test_table_validation_on_fail_values():
    tv = TableValidation(type="duplicate_check", columns=["id"], on_fail="warn")
    assert tv.on_fail == "warn"
    with pytest.raises(ValidationError):
        TableValidation(type="duplicate_check", columns=["id"], on_fail="invalid_action")

# Test Defaults
def test_defaults_basic_instantiation():
    defaults = Defaults()
    assert defaults.date_format is None
    assert defaults.column_rename_pattern == "none"

# Test SourceConfig
def test_source_config_basic_instantiation():
    source = SourceConfig(format="csv", path="data.csv", options={"header": True})
    assert source.format == "csv"
    assert source.path == "data.csv"
    assert source.options == {"header": True}

# Test SCDConfig
def test_scd_config_basic_instantiation():
    scd = SCDConfig()
    assert scd.type == "2"
    assert scd.track_columns is None

# Test SinkConfig
def test_sink_config_basic_instantiation():
    sink = SinkConfig(format="delta", mode="append", table="my_table")
    assert sink.format == "delta"
    assert sink.mode == "append"
    assert sink.table == "my_table"

def test_sink_config_mode_values():
    SinkConfig(format="delta", mode="overwrite", table="my_table")
    SinkConfig(format="delta", mode="merge", table="my_table")
    SinkConfig(format="delta", mode="overwrite_partition", table="my_table")
    SinkConfig(format="delta", mode="overwrite_where", table="my_table")
    with pytest.raises(ValidationError):
        SinkConfig(format="delta", mode="invalid_mode", table="my_table")

# Test TestConfig
def test_test_config_basic_instantiation():
    test_config = TestConfig(
        source_data=SourceConfig(format="csv", path="test_input.csv"),
        expected_results_data=SourceConfig(format="table", table="expected_results")
    )
    assert test_config.source_data.path == "test_input.csv"
    assert test_config.expected_results_data.table == "expected_results"

# Test TransformationConfig
def test_transformation_config_basic_instantiation():
    transform = TransformationConfig(name="my_transform", type="sql", sql="SELECT * FROM my_view")
    assert transform.name == "my_transform"
    assert transform.type == "sql"
    assert transform.sql == "SELECT * FROM my_view"

def test_transformation_config_type_values():
    TransformationConfig(name="my_transform", type="sql", sql="SELECT 1")
    with pytest.raises(ValidationError):
        TransformationConfig(name="my_transform", type="invalid_type", sql="SELECT 1")

# Test StepConfig
def test_step_config_basic_instantiation():
    step = StepConfig(name="read_data", type="read", params={"option": "value"})
    assert step.name == "read_data"
    assert step.type == "read"
    assert step.params == {"option": "value"}

# Test PipelineConfig
def test_pipeline_config_silver_validation():
    # Missing source
    with pytest.raises(ValidationError, match="'source' is required for silver pipelines."):
        PipelineConfig(
            engine="spark",
            pipeline_type="silver",
            pipeline_name="test_silver",
            sink=SinkConfig(format="delta", mode="append", table="output_table"),
            columns=[ColumnSpec(name="id", type="int")]
        )
    # Missing columns
    with pytest.raises(ValidationError, match="'columns' are required for silver pipelines."):
        PipelineConfig(
            engine="spark",
            pipeline_type="silver",
            pipeline_name="test_silver",
            source=SourceConfig(format="csv", path="input.csv"),
            sink=SinkConfig(format="delta", mode="append", table="output_table")
        )

def test_pipeline_config_gold_validation():
    # Missing transformation
    with pytest.raises(ValidationError, match="'transformation' is required for gold pipelines."):
        PipelineConfig(
            engine="spark",
            pipeline_type="gold",
            pipeline_name="test_gold",
            dependencies=["table1"],
            sink=SinkConfig(format="delta", mode="append", table="output_table")
        )
    # Missing dependencies
    with pytest.raises(ValidationError, match="'dependencies' are required for gold pipelines."):
        PipelineConfig(
            engine="spark",
            pipeline_type="gold",
            pipeline_name="test_gold",
            transformation=[TransformationConfig(name="t1", type="sql", sql="SELECT 1")],
            sink=SinkConfig(format="delta", mode="append", table="output_table")
        )

def test_pipeline_config_basic_silver_instantiation():
    config = PipelineConfig(
        engine="pandas",
        pipeline_type="silver",
        pipeline_name="my_silver_pipeline",
        source=SourceConfig(format="csv", path="input.csv"),
        sink=SinkConfig(format="csv", mode="overwrite", path="output.csv"),
        columns=[
            ColumnSpec(name="col1", type="string"),
            ColumnSpec(name="col2", type="int")
        ]
    )
    assert config.pipeline_name == "my_silver_pipeline"
    assert config.engine == "pandas"
    assert config.pipeline_type == "silver"
    assert config.source.path == "input.csv"
    assert config.sink.path == "output.csv"
    assert len(config.columns) == 2

def test_pipeline_config_basic_gold_instantiation():
    config = PipelineConfig(
        engine="spark",
        pipeline_type="gold",
        pipeline_name="my_gold_pipeline",
        dependencies=["silver.table1", "silver.table2"],
        transformation=[
            TransformationConfig(name="agg_step", type="sql", sql="SELECT COUNT(*) FROM silver.table1")
        ],
        sink=SinkConfig(format="delta", mode="overwrite", table="gold_output")
    )
    assert config.pipeline_name == "my_gold_pipeline"
    assert config.engine == "spark"
    assert config.pipeline_type == "gold"
    assert len(config.dependencies) == 2
    assert len(config.transformation) == 1
    assert config.sink.table == "gold_output"
