import re
from typing import Type
from drune.core.engine import BaseEngine, register_engine
from drune.core.step import get_step
from drune.models import PipelineModel
from drune.utils.logger import get_logger

@register_engine('spark')
class SparkEngine(BaseEngine):

    def __init__(self, config: PipelineModel):
        from pyspark.sql import SparkSession, DataFrame
        from pyspark.sql.types import StructType, StructField
        from pyspark.sql.functions import col, lit, current_timestamp, expr, sha2, concat_ws
        import pyspark.sql.functions as F
        from pyspark.sql.utils import AnalysisException
        from delta.tables import DeltaTable

        self.spark = SparkSession.builder.appName(config.pipeline_name).getOrCreate()
        self.config = config

    def run(self):
        """Executes the pipeline by dynamically running the steps defined in the config."""
        df = None
        for step_config in self.config.steps:
            step_class = get_step(step_config.type)
            if not step_class:
                raise ValueError(f"Step type '{step_config.type}' not found in registry.")
            
            step_instance = step_class(self)
            df = step_instance.execute(df, **step_config.params)
        return df
    
    def create_table(self, config: PipelineModel):
        """Creates a table in Unity Catalog with unified logic for Silver and Gold."""
        target_table_name = f"{config.sink.catalog}.{config.sink.schema_name}.{config.sink.table}"

        if self._table_exists(target_table_name):
            logger.info(f"Table '{target_table_name}' already exists. No action will be taken.")
        else:
            self._create_unified_table(config)

        if config.validation_log_table and not self._table_exists(config.validation_log_table):
            self._create_validation_log_table(config)
    
    def update_table(self, config: PipelineModel):
        """Applies schema and metadata changes to an existing table."""
        target_table_name = f"{config.sink.catalog}.{config.sink.schema_name}.{config.sink.table}"

        if not self._table_exists(target_table_name):
            raise Exception(f"Table '{target_table_name}' does not exist. Use the 'create' command first.")

        logger.info(f"Updating schema and metadata for table '{target_table_name}'...")

        existing_schema = self.spark.read.table(target_table_name).schema
        existing_cols = {field.name: field for field in existing_schema.fields}

        for spec in config.columns:
            final_name = self._get_final_column_name(spec, config.defaults)

            if final_name not in existing_cols:
                comment = spec.description or ''
                self.spark.sql(f"ALTER TABLE {target_table_name} ADD COLUMN `{final_name}` {spec.type} COMMENT '{comment}'")
                logger.info(f"  -> Column '{final_name}' added.")
            else:
                existing_comment = existing_cols[final_name].metadata.get('comment', '')
                new_comment = spec.description or ''
                if new_comment and new_comment != existing_comment:
                    self.spark.sql(f"ALTER TABLE {target_table_name} ALTER COLUMN `{final_name}` COMMENT '{new_comment}'")
                    logger.info(f"  -> Column comment for '{final_name}' updated.")

        logger.info("Checking if hash_key needs to be reprocessed...")
        try:
            tbl_properties_str = self.spark.sql(f"DESCRIBE TABLE EXTENDED {target_table_name}").filter("col_name = 'Table Properties'").collect()[0]['data_type']
            match = re.search(r"framework\\.primary_keys=([a-zA-Z0-9_,]+)", tbl_properties_str)
            existing_pks_str = match.group(1) if match else ""
            existing_pks = set(existing_pks_str.split(',')) if existing_pks_str else set()
        except (IndexError, AttributeError):
            existing_pks = set()

        new_pks = {self._get_final_column_name(spec, config.defaults) for spec in config.columns if spec.pk}

        if new_pks != existing_pks:
            logger.warning(f"[WARNING] Primary keys changed from {existing_pks} to {new_pks}. Reprocessing 'hash_key' column...")

            full_table_df = self.spark.read.table(target_table_name)

            reprocessed_df = full_table_df.drop("hash_key").withColumn(
                "hash_key", sha2(concat_ws("||", *sorted(list(new_pks))), 256)
            )

            target_table = DeltaTable.forName(self.spark, target_table_name)
            (target_table.alias("target")
                .merge(
                    reprocessed_df.alias("source"),
                    "target.id = source.id"
                )
                .whenMatchedUpdate(set={
                    "hash_key": "source.hash_key"
                })
                .execute())

            self.spark.sql(f"ALTER TABLE {target_table_name} SET TBLPROPERTIES ('framework.primary_keys' = '{','.join(sorted(list(new_pks)))}')")
            logger.info("  -> hash_key reprocessing completed.")
        else:
            logger.info("  -> Primary keys have not changed. No action required for hash_key.")

    def test(self, config: PipelineModel):
        pass

    def _table_exists(self, table_name: str) -> bool:
        """Checks if a table exists in Unity Catalog in a serverless-compatible way."""
        try:
            self.spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}")
            return True
        except AnalysisException as e:
            if 'TABLE_OR_VIEW_NOT_FOUND' in str(e).upper():
                return False
            raise e
    
    def _get_final_column_name(self, spec, defaults):
        """Helper to get the final column name based on rules."""
        from .steps.transformer import _apply_rename_pattern
        if spec.rename:
            return spec.rename
        if defaults.column_rename_pattern == 'snake_case':
            return _apply_rename_pattern(spec.name)
        return spec.name

    def _create_unified_table(self, config: PipelineModel):
        """Builds and executes the DDL for any table (Silver or Gold)."""
        sink_config = config.sink
        target_table_name = f"{sink_config.catalog}.{sink_config.schema_name}.{sink_config.table}"

        logger.info(f"Building DDL for table '{target_table_name}' (Type: {config.pipeline_type})...")

        column_definitions = ["id BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate primary key.'"]
        primary_keys = []

        for spec in config.columns:
            final_name = self._get_final_column_name(spec, config.defaults)
            if spec.pk:
                primary_keys.append(final_name)
            is_not_null = any(v.rule == "not_null" for v in spec.validation_rules)
            not_null_str = " NOT NULL" if is_not_null else ""
            comment_str = f" COMMENT '{spec.description}'" if spec.description else ""
            column_definitions.append(f"{final_name} {spec.type}{not_null_str}{comment_str}")

        if sink_config.scd and sink_config.scd.type == '2':
            column_definitions.extend([
                "data_hash STRING COMMENT 'Hash for change detection.'",
                "is_current BOOLEAN COMMENT 'Active record flag.'",
                "start_date TIMESTAMP COMMENT 'Validity start date.'",
                "end_date TIMESTAMP COMMENT 'Validity end date.'"
            ])
        else:
            column_definitions.append("created_at TIMESTAMP COMMENT 'Creation timestamp.'")

        column_definitions.extend([
            "hash_key STRING COMMENT 'Hash of primary keys.'",
            "updated_at TIMESTAMP COMMENT 'Last update timestamp.'"
        ])

        fk_definitions = []
        if sink_config.foreign_keys:
            for fk in sink_config.foreign_keys:
                fk_definitions.append(f"CONSTRAINT {fk.name} FOREIGN KEY ({', '.join(fk.local_columns)}) REFERENCES {fk.references_table}({', '.join(fk.references_columns)})")

        pk_constraint = f", CONSTRAINT pk_{sink_config.table} PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else f", CONSTRAINT pk_{sink_config.table} PRIMARY KEY (id)"

        all_definitions = column_definitions + fk_definitions
        ddl = f"CREATE TABLE {target_table_name} ({', '.join(all_definitions)}{pk_constraint})"

        if config.description:
            ddl += f" COMMENT '{config.description}'"
        if sink_config.partition_by:
            ddl += f" PARTITIONED BY ({', '.join(sink_config.partition_by)})"

        logger.info("Executing table creation DDL...")
        self.spark.sql(ddl)
        if primary_keys:
            self.spark.sql(f"ALTER TABLE {target_table_name} SET TBLPROPERTIES ('framework.primary_keys' = '{','.join(sorted(list(primary_keys)))}')")
        logger.info("Table created successfully.")

    def _create_validation_log_table(self, config: PipelineModel):
        """Creates the validation log table with a fixed schema."""
        logger.info(f"Creating validation log table: {config.validation_log_table}")
        log_ddl = f"""
        CREATE TABLE {config.validation_log_table} (
            pipeline_name STRING,
            validation_rule STRING,
            failed_column STRING,
            failed_value STRING,
            log_timestamp TIMESTAMP,
            hash_key STRING COMMENT 'Hash of the primary keys of the failed record.'
        )
        """
        self.spark.sql(log_ddl)
        logger.info("Validation log table created successfully.")


    def read_table(self, table_name: str) -> DataFrame:
        """Reads a Delta table and returns it as a DataFrame."""
        return self.spark.read.table(table_name)

    def compare_dataframes(self, df_actual: DataFrame, df_expected: DataFrame) -> bool:
        """Compares the schema and data of two DataFrames."""
        if df_actual.schema != df_expected.schema:
            logger.error("Error: Schemas are different.")
            return False

        if df_actual.count() != df_expected.count():
            logger.error(f"Error: Row count is different. Actual: {df_actual.count()}, Expected: {df_expected.count()}")
            return False

        # exceptAll returns rows that are in one DF but not in the other.
        # If both results are empty, the DFs are identical.
        diff1 = df_actual.exceptAll(df_expected)
        diff2 = df_expected.exceptAll(df_actual)

        return diff1.count() == 0 and diff2.count() == 0

    def show_differences(self, df_actual: DataFrame, df_expected: DataFrame):
        """Shows the rows that differ between the two DataFrames."""
        logger.info("--- Difference Details ---")
        logger.info("\nRows in ACTUAL result that are NOT in EXPECTED:")
        df_actual.exceptAll(df_expected).show()

        logger.info("\nRows in EXPECTED result that are NOT in ACTUAL:")
        df_expected.exceptAll(df_actual).show()
    
    def execute_gold_transformation(self, config: PipelineModel) -> DataFrame:
        """Executes a sequence of SQL transformations for a Gold pipeline."""
        logger.info("  -> Reading dependencies...")
        for dep in config.dependencies:
            table_name = dep.split('.')[-1]
            self.read_table(dep).createOrReplaceTempView(table_name)
            logger.info(f"  -> Dependency registered: Table '{dep}' as SQL view '{table_name}'")

        df_result = None
        for i, transform_step in enumerate(config.transformation):
            step_name = transform_step.name.replace(' ', '_').lower()
            logger.info(f"  -> Executing Gold transformation step {i+1}/{len(config.transformation)}: '{step_name}'")
            if transform_step.type == 'sql':
                df_result = self.spark.sql(transform_step.sql)
                # The result of each step becomes a view for the next
                df_result.createOrReplaceTempView(step_name)
                logger.info(f"    -> Intermediate view '{step_name}' created.")
            else:
                raise NotImplementedError(f"Transformation type '{transform_step.type}' not supported for Gold.")

        if df_result is None:
            raise ValueError("No transformation step was executed for the Gold pipeline.")

        return df_result
