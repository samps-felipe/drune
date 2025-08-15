"""Custom exceptions for the framework."""

class DruneError(Exception):
    """Base class for exceptions in this framework."""
    pass

class PipelineError(DruneError):
    """Raised for errors in the pipeline execution."""
    pass

class StepExecutionError(DruneError):
    """Raised for errors during a step execution."""
    pass

class ValidationError(StepExecutionError):
    """Raised for data validation errors."""
    pass

class ConfigurationError(DruneError):
    """Raised for configuration errors."""
    pass
