from drune.core.engine import BaseEngine, register_engine
from drune.core.step import get_step
from drune.models import PipelineModel
from .steps import ReadStep, WriteStep, TransformStep, ValidateStep

@register_engine('pandas')
class PandasEngine(BaseEngine):
    def __init__(self, config: PipelineModel):
        import pandas as pd
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
        # Pandas engine does not create tables in a persistent catalog.
        # This could be implemented to create a schema file or similar.
        pass

    def update_table(self, config: PipelineModel):
        # Pandas engine does not update tables in a persistent catalog.
        pass