from drune.core.pipeline import Pipeline
from drune.models.pydantic_models import PipelineConfig
from drune.engines.spark.spark_engine import SparkEngine

def test_create_pipeline(spark):
    """
    Testa a criação de um objeto Pipeline.
    """
    # Assuming PipelineConfig is the correct model for pipeline definition
    pipeline_config = PipelineConfig(
        pipeline_name="test_pipeline",
        engine="spark", # Added engine as it's a required field in PipelineConfig
        pipeline_type="silver", # Added pipeline_type as it's a required field
        source={"type": "csv", "path": "dummy.csv"}, # Dummy source
        sink={"type": "csv", "path": "dummy_output.csv"}, # Dummy sink
        columns=[] # Empty columns for this basic test
    )
    spark_engine = SparkEngine(spark)
    pipeline = Pipeline(pipeline_config, spark_engine)
    assert pipeline.pipeline_config.pipeline_name == "test_pipeline"
    assert pipeline.engine == spark_engine
