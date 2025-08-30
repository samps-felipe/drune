from typing import Dict, List
import pandas as pd
from drune.core.pipeline_state import PipelineState
from drune.core.steps.step import BaseStep, register_step

try:
    import duckdb
except ImportError:
    duckdb = None


@register_step("transform")
class TransformStep(BaseStep):
    """Step responsible for applying transformations to a pandas DataFrame."""

    def execute(
        self, pipeline_state: PipelineState, params: Dict[str, any] = None
    ) -> Dict[str, pd.DataFrame]:
        self.logger.info("--- Step: Transform (Pandas) ---")

        sources = pipeline_state.sources
        target = pipeline_state.target

        if len(sources) != 1:
            raise ValueError("Transform step requires exactly one source DataFrame.")

        # TODO: deal with multiple sources if needed
        # Assuming sources is a dictionary with one DataFrame
        df = sources[list(sources.keys())[0]]

        # Apply column renaming
        rename_map = {
            spec.name: spec.rename
            for spec in self.config.columns
            if spec.name and spec.rename and spec.name in df.columns
        }
        if rename_map:
            df = df.rename(columns=rename_map)

        # Apply transformations
        for spec in self.config.columns:
            final_name = spec.rename or spec.name
            if spec.transform and final_name in df.columns:
                # This is a simplified transformation using eval.
                # For more complex scenarios, a more robust solution is needed.
                try:
                    df[final_name] = df.eval(spec.transform)
                except Exception as e:
                    self.logger.error(
                        f"Could not apply transform '{spec.transform}' on column '{final_name}': {e}"
                    )

        # Select and order columns
        final_columns = [spec.rename or spec.name for spec in self.config.columns]
        df = df[[col for col in final_columns if col in df.columns]]

        pipeline_state.target = df
        self.logger.info("Transform step completed successfully.")

        return pipeline_state


@register_step("filter")
class FilterStep(BaseStep):
    def execute(
        self, pipeline_state: PipelineState, params: Dict[str, any] = None
    ) -> PipelineState:
        self.logger.info("--- Step: Filter (Pandas) ---")
        if not hasattr(self.config, "where"):
            raise ValueError("Filter step requires a 'where' clause in the configuration.")

        df = pipeline_state.target
        df = df.query(self.config.where)
        pipeline_state.target = df
        self.logger.info("Filter step completed successfully.")
        return pipeline_state


@register_step("join")
class JoinStep(BaseStep):
    def execute(
        self, pipeline_state: PipelineState, params: Dict[str, any] = None
    ) -> PipelineState:
        self.logger.info("--- Step: Join (Pandas) ---")
        
        if not hasattr(self.config, "sources"):
            raise ValueError("Join step requires 'sources' in the configuration.")

        if not hasattr(self.config, "on"):
            raise ValueError("Join step requires 'on' in the configuration.")

        join_type = getattr(self.config, "join_type", "inner")
        
        sources_to_join = self.config.sources
        if len(sources_to_join) < 2:
            raise ValueError("Join step requires at least two sources.")

        # Get the dataframes from the pipeline state
        dataframes = [pipeline_state.sources[src] for src in sources_to_join]
        
        # Start with the first dataframe
        df = dataframes[0]

        # Iteratively join the rest of the dataframes
        for i in range(1, len(dataframes)):
            df = pd.merge(
                df,
                dataframes[i],
                on=self.config.on,
                how=join_type,
                suffixes=("", f"_right_{i}")
            )

        pipeline_state.target = df
        self.logger.info("Join step completed successfully.")
        return pipeline_state


@register_step("sql")
class SQLStep(BaseStep):
    def execute(
        self, pipeline_state: PipelineState, params: Dict[str, any] = None
    ) -> PipelineState:
        self.logger.info("--- Step: SQL (Pandas) ---")
        if duckdb is None:
            raise ImportError("duckdb is required for the SQL step with pandas engine. Please install it with 'pip install duckdb'")

        if not hasattr(self.config, "query"):
            raise ValueError("SQL step requires a 'query' in the configuration.")

        # Register all source dataframes with duckdb
        con = duckdb.connect(database=':memory:', read_only=False)
        for name, df in pipeline_state.sources.items():
            con.register(name, df)

        # Execute the query
        result_df = con.execute(self.config.query).fetchdf()
        
        pipeline_state.target = result_df
        self.logger.info("SQL step completed successfully.")
        return pipeline_state


@register_step("pivot")
class PivotStep(BaseStep):
    def execute(
        self, pipeline_state: PipelineState, params: Dict[str, any] = None
    ) -> PipelineState:
        self.logger.info("--- Step: Pivot (Pandas) ---")
        
        if not hasattr(self.config, "index"):
            raise ValueError("Pivot step requires 'index' in the configuration.")
        if not hasattr(self.config, "columns"):
            raise ValueError("Pivot step requires 'columns' in the configuration.")
        if not hasattr(self.config, "values"):
            raise ValueError("Pivot step requires 'values' in the configuration.")

        df = pipeline_state.target
        
        aggfunc = getattr(self.config, "aggfunc", "mean")

        df_pivot = df.pivot_table(
            index=self.config.index,
            columns=self.config.columns,
            values=self.config.values,
            aggfunc=aggfunc
        )
        
        pipeline_state.target = df_pivot.reset_index()
        self.logger.info("Pivot step completed successfully.")
        return pipeline_state
