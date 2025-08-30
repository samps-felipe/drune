from typing import Any, Dict
import duckdb
from drune.core.steps import BaseStep
from drune.core.steps import StepManager

@StepManager.register('duckdb', 'transform')
class TransformStep(BaseStep):
    def execute(self, sources, target, columns: Dict[str, any]) -> Any:
        self.logger.info("--- Step: Transform (DuckDB) ---")
        
        if len(sources) != 1:
            raise ValueError("Transform step requires exactly one source.")

        source_name = list(sources.keys())[0]
        relation = sources[source_name]

        select_expressions = []
        for spec in columns:
            final_name = spec.rename or spec.name
            if spec.transform:
                select_expressions.append(f"{spec.transform} AS \"{final_name}\"")
            else:
                select_expressions.append(f"\"{spec.name}\" AS \"{final_name}\"")

        query = f"SELECT {', '.join(select_expressions)} FROM {source_name}"
        
        target = relation.query(source_name, query)
        self.logger.info("Transform step completed successfully.")
        return target

@StepManager.register('duckdb', 'filter')
class FilterStep(BaseStep):
    def execute(self, sources, target, where: str):
        self.logger.info("--- Step: Filter (DuckDB) ---")

        relation = target
        relation = relation.filter(where)
        target = relation
        self.logger.info("Filter step completed successfully.")
        return target

@StepManager.register('duckdb', 'join')
class JoinStep(BaseStep):
    def execute(self,sources, target):
        self.logger.info("--- Step: Join (DuckDB) ---")
        
        if not len(self.config.sources) < 2:
            raise ValueError("Join step requires at least two sources.")

        if not hasattr(self.config, "on"):
            raise ValueError("Join step requires 'on' in the configuration.")

        join_type = getattr(self.config, "join_type", "inner").upper()

        # Start with the first source as the base relation
        base_source_name = self.config.sources[0]
        base_relation = pipeline_state.sources[base_source_name]

        # Build the join query
        join_query = f'FROM "{base_source_name}"'
        for i in range(1, len(self.config.sources)):
            other_source_name = self.config.sources[i]
            join_on = self.config.on[i-1] if isinstance(self.config.on, list) else self.config.on
            join_query += f" {join_type} JOIN \"{other_source_name}\" ON {join_on}"

        query = f"SELECT * {join_query}"

        # Create views for all sources to be joined
        for source_name in self.config.sources:
            pipeline_state.sources[source_name].create_view(source_name, replace=True)

        target = base_relation.connection.execute(query)
        self.logger.info("Join step completed successfully.")
        return target

@StepManager.register('duckdb', 'sql')
class SQLStep(BaseStep):
    def execute(self, sources, target, query: str):
        self.logger.info("--- Step: SQL (DuckDB) ---")

        # Create views for all sources
        for name, relation in sources.items():
            relation.create_view(name, replace=True)

        result_relation = sources[list(sources.keys())[0]].connection.execute(query)
        target = result_relation
        self.logger.info("SQL step completed successfully.")
        return target

# @StepManager.register('duckdb', 'pivot')
# class PivotStep(BaseStep):
#     def execute(self, pipeline_state: PipelineState, index: str, columns: str, values: str, agg_func: str = 'first') -> PipelineState:
#         self.logger.info("--- Step: Pivot (DuckDB) ---")

#         relation = pipeline_state.target
#         temp_view_name = "temp_view_for_pivot"
#         relation.create_view(temp_view_name, replace=True)
        
#         query = f"PIVOT {temp_view_name} ON \"{columns}\" USING {agg_func}(\"{values}\") GROUP BY \"{index}\""

#         pipeline_state.target = relation.connection.execute(f"SELECT * FROM ({query})")
#         self.logger.info("Pivot step completed successfully.")
#         return pipeline_state
