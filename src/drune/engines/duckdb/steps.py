from typing import Dict
import duckdb
from drune.core.state import PipelineState
from drune.core.step import BaseStep, register_step

@register_step('transform')
class TransformStep(BaseStep):
    def execute(self, pipeline_state: PipelineState, params: Dict[str, any] = None) -> PipelineState:
        self.logger.info("--- Step: Transform (DuckDB) ---")
        
        if len(pipeline_state.sources) != 1:
            raise ValueError("Transform step requires exactly one source.")

        source_name = list(pipeline_state.sources.keys())[0]
        relation = pipeline_state.sources[source_name]

        select_expressions = []
        for spec in self.config.columns:
            final_name = spec.rename or spec.name
            if spec.transform:
                select_expressions.append(f"{spec.transform} AS \"{final_name}\"")
            else:
                select_expressions.append(f"\"{spec.name}\" AS \"{final_name}\"")

        query = f"SELECT {', '.join(select_expressions)} FROM {source_name}"
        
        pipeline_state.target = relation.query(source_name, query)
        self.logger.info("Transform step completed successfully.")
        return pipeline_state

@register_step('filter')
class FilterStep(BaseStep):
    def execute(self, pipeline_state: PipelineState, params: Dict[str, any] = None) -> PipelineState:
        self.logger.info("--- Step: Filter (DuckDB) ---")
        if not hasattr(self.config, "where"):
            raise ValueError("Filter step requires a 'where' clause in the configuration.")

        relation = pipeline_state.target
        relation = relation.filter(self.config.where)
        pipeline_state.target = relation
        self.logger.info("Filter step completed successfully.")
        return pipeline_state

@register_step('join')
class JoinStep(BaseStep):
    def execute(self, pipeline_state: PipelineState, params: Dict[str, any] = None) -> PipelineState:
        self.logger.info("--- Step: Join (DuckDB) ---")
        
        if not hasattr(self.config, "sources") or len(self.config.sources) < 2:
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

        pipeline_state.target = base_relation.connection.execute(query)
        self.logger.info("Join step completed successfully.")
        return pipeline_state

@register_step('sql')
class SQLStep(BaseStep):
    def execute(self, pipeline_state: PipelineState, params: Dict[str, any] = None) -> PipelineState:
        self.logger.info("--- Step: SQL (DuckDB) ---")
        if not hasattr(self.config, "query"):
            raise ValueError("SQL step requires a 'query' in the configuration.")

        # Create views for all sources
        for name, relation in pipeline_state.sources.items():
            relation.create_view(name, replace=True)

        result_relation = pipeline_state.sources[list(pipeline_state.sources.keys())[0]].connection.execute(self.config.query)
        pipeline_state.target = result_relation
        self.logger.info("SQL step completed successfully.")
        return pipeline_state

@register_step('pivot')
class PivotStep(BaseStep):
    def execute(self, pipeline_state: PipelineState, params: Dict[str, any] = None) -> PipelineState:
        self.logger.info("--- Step: Pivot (DuckDB) ---")
        
        if not hasattr(self.config, "index"):
            raise ValueError("Pivot step requires 'index' in the configuration.")
        if not hasattr(self.config, "columns"):
            raise ValueError("Pivot step requires 'columns' in the configuration.")
        if not hasattr(self.config, "values"):
            raise ValueError("Pivot step requires 'values' in the configuration.")

        relation = pipeline_state.target
        temp_view_name = "temp_view_for_pivot"
        relation.create_view(temp_view_name, replace=True)

        agg_func = getattr(self.config, "aggfunc", "first")
        
        query = f"PIVOT {temp_view_name} ON \"{self.config.columns}\" USING {agg_func}(\"{self.config.values}\") GROUP BY \"{self.config.index}\""

        pipeline_state.target = relation.connection.execute(f"SELECT * FROM ({query})")
        self.logger.info("Pivot step completed successfully.")
        return pipeline_state
