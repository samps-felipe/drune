from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from ....core.step import BaseStep, register_step

@register_step('write')
class WriterStep(BaseStep):
    """Step responsible for writing data to the destination."""
    def execute(self, df: DataFrame, validation_log_df: DataFrame = None, **kwargs) -> DataFrame:
        self.logger.info("--- Step: Write (Spark) ---")
        
        sink_config = self.config.sink

        if sink_config.mode == 'merge':
            if sink_config.scd and sink_config.scd.type == '2':
                self._merge_scd2(df)
            else:
                self._merge_standard(df)
        else:
            self._write_standard(df)

        if validation_log_df and self.config.validation_log_table:
            validation_log_df.write.mode("append").saveAsTable(self.config.validation_log_table)

        self.logger.info("Write to destination completed.")
        return df

    def _write_standard(self, df: DataFrame):
        sink_config = self.config.sink
        target_table = f"{sink_config.catalog}.{sink_config.schema_name}.{sink_config.table}"
        writer = df.write.mode(sink_config.mode)
        if sink_config.partition_by:
            writer = writer.partitionBy(*sink_config.partition_by)
        if sink_config.mode in ['overwrite_partition', 'overwrite_where'] and sink_config.overwrite_condition:
            writer = writer.option("replaceWhere", sink_config.overwrite_condition)
        writer.option("mergeSchema", "true").saveAsTable(target_table)

    def _merge_standard(self, df: DataFrame):
        """Performs a standard merge (upsert) operation, ignoring the 'id' column."""
        target_table_name = f"{self.config.sink.catalog}.{self.config.sink.schema_name}.{self.config.sink.table}"
        delta_table = DeltaTable.forName(self.engine.spark, target_table_name)
        
        update_map = {c: f"source.{c}" for c in df.columns if c not in ["id", "hash_key", "created_at"]}
        update_map["updated_at"] = "source.updated_at"
        
        insert_map = {c: f"source.{c}" for c in df.columns if c != "id"}
        if "created_at" in df.columns:
            insert_map["created_at"] = "source.updated_at"

        (delta_table.alias("target")
            .merge(df.alias("source"), "target.hash_key = source.hash_key")
            .whenMatchedUpdate(set=update_map)
            .whenNotMatchedInsert(values=insert_map)
            .execute())

    def _merge_scd2(self, df: DataFrame):
        """Executes robust and idempotent merge logic for SCD Type 2."""
        target_table_name = f"{self.config.sink.catalog}.{self.config.sink.schema_name}.{self.config.sink.table}"
        
        source_df = df.alias("source")
        target_table = DeltaTable.forName(self.engine.spark, target_table_name)
        target_df = target_table.toDF().alias("target")
        join_condition = "source.hash_key = target.hash_key"

        changed_records = source_df.join(
            target_df.filter(col("target.is_current") == True),
            expr(join_condition), "inner"
        ).where(col("source.data_hash") != col("target.data_hash")).select("source.*")

        records_to_expire = changed_records.select(col("hash_key"), lit(False).alias("is_current"), current_timestamp().alias("end_date"))

        new_records_to_insert = changed_records.withColumn("is_current", lit(True)).withColumn("start_date", current_timestamp()).withColumn("end_date", lit(None).cast("timestamp"))
        brand_new_records = source_df.join(target_df.filter("is_current = true"), "hash_key", "left_anti").withColumn("is_current", lit(True)).withColumn("start_date", current_timestamp()).withColumn("end_date", lit(None).cast("timestamp"))
        final_inserts = new_records_to_insert.unionByName(brand_new_records, allowMissingColumns=True)

        self._scd2_expire_old_records(target_table, records_to_expire, join_condition)
        self._scd2_insert_new_records(target_table, final_inserts, target_table_name)
        
        self.logger.info("SCD Type 2 Merge operation completed.")

    def _scd2_expire_old_records(self, target_table: DeltaTable, records_to_expire: DataFrame, join_condition: str):
        """Expires old records in an SCD2 operation."""
        if records_to_expire.head(1):
            self.logger.info(f"Expiring old records...")
            (target_table.alias("target")
                .merge(records_to_expire.alias("source"), f"{join_condition} AND target.is_current = true")
                .whenMatchedUpdate(set={"is_current": "source.is_current", "end_date": "source.end_date"})
                .execute())

    def _scd2_insert_new_records(self, target_table: DeltaTable, final_inserts: DataFrame, target_table_name: str):
        """Inserts new records in an SCD2 operation."""
        if final_inserts.head(1):
            self.logger.info(f"Inserting new/updated records...")
            existing_versions = target_table.toDF().select("hash_key", "data_hash")
            records_to_actually_insert = final_inserts.join(
                existing_versions,
                (final_inserts.hash_key == existing_versions.hash_key) & (final_inserts.data_hash == existing_versions.data_hash),
                "left_anti"
            )
            if records_to_actually_insert.head(1):
                records_to_actually_insert.write.format("delta").mode("append").saveAsTable(target_table_name)
