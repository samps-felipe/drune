from pyspark.sql import DataFrame, functions as F
from typing import Tuple
import ast
from ..quality import BaseValidation, BaseTableValidation, register_rule

# --- Column Validation Implementations ---

@register_rule("not_null")
class NotNullValidation(BaseValidation):
    def apply(self, df: DataFrame, column_name: str) -> Tuple[DataFrame, DataFrame]:
        condition = F.col(column_name).isNotNull()
        failures_df = df.filter(~condition)
        success_df = df.filter(condition)
        return failures_df, success_df

@register_rule("pattern")
class PatternValidation(BaseValidation):
    def __init__(self, params: dict = None):
        super().__init__(params)
        self.pattern = self.params.get('pattern')

    def apply(self, df: DataFrame, column_name: str) -> Tuple[DataFrame, DataFrame]:
        condition = F.col(column_name).rlike(self.pattern)
        failures_df = df.filter(~condition)
        success_df = df.filter(condition)
        return failures_df, success_df

@register_rule("isin")
class IsInValidation(BaseValidation):
    """Validates if the column value is in a list of allowed values."""
    def __init__(self, params: dict = None):
        super().__init__(params)
        allowed_values_str = self.params.get('allowed_values_str')
        try:
            self.allowed_values = ast.literal_eval(allowed_values_str)
            if not isinstance(self.allowed_values, list):
                raise TypeError
        except (ValueError, SyntaxError, TypeError):
            raise ValueError(f"Invalid parameter for 'isin' rule. Expected a list format, e.g., ['A', 'B']. Received: {allowed_values_str}")

    def apply(self, df: DataFrame, column_name: str) -> Tuple[DataFrame, DataFrame]:
        condition = F.col(column_name).isin(self.allowed_values)
        failures_df = df.filter(~condition)
        success_df = df.filter(condition)
        return failures_df, success_df

@register_rule("greater_than_or_equal_to")
class GreaterThanOrEqualToValidation(BaseValidation):
    """Validates if the column value is greater than or equal to a numeric value."""
    def __init__(self, params: dict = None):
        super().__init__(params)
        value_str = self.params.get('value_str')
        try:
            self.value = float(value_str)
        except ValueError:
            raise ValueError(f"Invalid parameter for 'greater_than_or_equal_to'. Expected a number. Received: {value_str}")

    def apply(self, df: DataFrame, column_name: str) -> Tuple[DataFrame, DataFrame]:
        condition = F.col(column_name) >= self.value
        failures_df = df.filter(~condition)
        success_df = df.filter(condition)
        return failures_df, success_df

@register_rule("isbetween")
class IsBetweenValidation(BaseValidation):
    """Validates if the column value is between a lower and upper bound."""
    def __init__(self, params: dict = None):
        super().__init__(params)
        bounds_str = self.params.get('bounds_str')
        try:
            bounds = ast.literal_eval(bounds_str)
            if not isinstance(bounds, list) or len(bounds) != 2:
                raise TypeError
            self.lower_bound = float(bounds[0])
            self.upper_bound = float(bounds[1])
        except (ValueError, SyntaxError, TypeError):
            raise ValueError(f"Invalid parameter for 'isbetween'. Expected a list of two numbers, e.g., [0, 100]. Received: {bounds_str}")

    def apply(self, df: DataFrame, column_name: str) -> Tuple[DataFrame, DataFrame]:
        condition = F.col(column_name).between(self.lower_bound, self.upper_bound)
        failures_df = df.filter(~condition)
        success_df = df.filter(condition)
        return failures_df, success_df

# --- Table Validation Implementations ---

@register_rule("duplicate_check")
class DuplicateCheckValidation(BaseTableValidation):
    def __init__(self, params: dict = None):
        self.columns = (params or {}).get('columns')

    def apply(self, df: DataFrame):
        duplicate_count = df.groupBy(*self.columns).count().filter("count > 1").count()
        if duplicate_count > 0:
            raise ValueError(f"Duplicate check failed for columns: {self.columns}. {duplicate_count} duplicates found.")
