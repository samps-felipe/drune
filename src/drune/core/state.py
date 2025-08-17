from typing import Any, Dict

class PipelineState():
    """Class to hold the state of a pipeline execution."""
    def __init__(self, sources: Dict[str, Any] = {}, target: Any = None, ):
        self.sources = sources
        self.target = target
