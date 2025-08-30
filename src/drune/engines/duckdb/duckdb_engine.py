from ast import Dict
import os
from typing import Any, Type, List
import duckdb
from drune.core.engine import BaseEngine
from drune.core.models import ColumnSpec
from drune.utils.logger import get_logger
from drune.utils.exceptions import ConfigurationError
from drune.utils.parsers import parse_function_string

from drune.core.engine import EngineManager

@EngineManager.register("duckdb")
class DuckDBEngine(BaseEngine):
    def __init__(self, database: str = ':memory:', read_only: bool = False):
        self.name = 'duckdb'
        self.logger = get_logger("engine:duckdb")

        self.con = duckdb.connect(database=database, read_only=read_only)

    def read_file(self, source) -> Any:
        self.logger.info(f"Reading source: {source.name}")
        if source.format == 'csv':
            return self.con.read_csv(source.path, **source.options)
        elif source.format == 'json':
            return self.con.read_json(source.path, **source.options)
        elif source.format == 'parquet':
            return self.con.read_parquet(source.path, **source.options)
        else:
            raise NotImplementedError(f"Source format '{source.format}' is not supported by the duckdb engine.")

    def read_table(self, table_name: str) -> Any:
        self.logger.info(f"Reading table: {table_name}")
        return self.con.table(table_name)

    def execute_query(self, query: str) -> Any:
        self.logger.info(f"Executing query: {query}")
        return self.con.sql(query)

    def write(self, relation, target_config):
        self.logger.info("--- Step: Write (DuckDB) ---")
        if not target_config:
            raise ConfigurationError("'Target' configuration not found.")

        filename = f"{target_config.name}.{target_config.format}"
        output_file_path = os.path.join(target_config.path, filename)

        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        if target_config.format == 'csv':
            relation.to_csv(output_file_path)
        elif target_config.format == 'json':
            relation.to_json(output_file_path)
        elif target_config.format == 'parquet':
            relation.to_parquet(output_file_path)
        else:
            raise NotImplementedError(f"Sink format '{target_config.format}' is not supported by the duckdb engine.")

    def rollback(self, target):
        return super().rollback(target)

    def apply_schema(self, relation, schema) -> Any:
        if not schema or not schema.columns:
            self.logger.info("No schema or columns to apply.")
            return relation

        self.logger.info("Applying schema to the DataFrame.")
        
        temp_view_name = "temp_view_for_schema"
        relation.create_view(temp_view_name, replace=True)

        select_clauses = []
        for col_spec in schema.columns:
            source_col_name = col_spec.name
            final_col_name = col_spec.rename or source_col_name

            # Type casting
            cast_expression = self._get_cast_expression(source_col_name, col_spec)

            # Transformations
            transforms = parse_function_string(col_spec.transform) if col_spec.transform else []

            transformed_expression = self._apply_transformations(cast_expression, transforms)

            select_clauses.append(f"{transformed_expression} AS {final_col_name}")

        select_statement = ", ".join(select_clauses)
        query = f"SELECT {select_statement} FROM {temp_view_name}"
        
        self.logger.info(f"Applying schema with query: {query}")
        return self.con.sql(query)

    def _get_cast_expression(self, column_name: str, col_spec: ColumnSpec) -> str:
        target_type = col_spec.type.lower()
        type_map = {
            'str': 'VARCHAR',
            'int': 'BIGINT',
            'long': 'BIGINT',
            'float': 'DOUBLE',
            'double': 'DOUBLE',
            'bool': 'BOOLEAN',
            'date': 'DATE',
            'timestamp': 'TIMESTAMP',
        }
        duckdb_type = type_map.get(target_type)

        if not duckdb_type:
            self.logger.warning(f"Type '{col_spec.type}' not supported for casting column '{column_name}'. Skipping.")
            return column_name

        if col_spec.try_cast:
            return f"TRY_CAST(\"{column_name}\" AS {duckdb_type})"
        else:
            return f"CAST(\"{column_name}\" AS {duckdb_type})"

    def _apply_transformations(self, column_expression: str, transfoms: List[str]) -> str:
        if not transfoms:
            return column_expression

        expression = column_expression
        for transform in transfoms:
            if transform['function'] == 'trim':
                expression = f"trim({expression})"
            elif transform['function'] == 'upper':
                expression = f"upper({expression})"
            elif transform['function'] == 'lower':
                expression = f"lower({expression})"
            else:
                self.logger.warning(f"Transformation '{transform}' not supported. Skipping.")
        return expression

    def write_list(self, fails_list: List[Any], path: str):
        if not fails_list:
            self.logger.info("No failed records to write.")
            return

        output_file_path = os.path.join(path, "failed_records.csv")
        
        # Create a single relation from the list of relations
        all_fails_relation = fails_list[0]
        for i in range(1, len(fails_list)):
            all_fails_relation = all_fails_relation.union(fails_list[i])

        all_fails_relation.to_csv(output_file_path)
        self.logger.info(f"Wrote {len(all_fails_relation)} failed records to {output_file_path}")
