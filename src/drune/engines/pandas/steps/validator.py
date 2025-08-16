from typing import Dict
import pandas as pd
import ast
from drune.core.step import BaseStep, register_step
from drune.utils.exceptions import ValidationError
from drune.core.quality import BaseValidation, get_validation_rule, register_rule

@register_rule('not_null')
class NotNullValidation(BaseValidation):
    def apply(self, df: pd.DataFrame, column_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        success_df = df[df[column_name].notna()]
        failures_df = df[df[column_name].isna()]
        return failures_df, success_df

@register_rule('isin')
class IsInValidation(BaseValidation):
    def __init__(self, params: dict):
        super().__init__(params)
        # Assuming params contains a string representation of a list of allowed values
        self.allowed_values = ast.literal_eval(self.params)

    def apply(self, df: pd.DataFrame, column_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        success_df = df[df[column_name].isin(self.allowed_values)]
        failures_df = df[~df[column_name].isin(self.allowed_values)]
        return failures_df, success_df

@register_step('validate')
class ValidateStep(BaseStep):
    """Step responsible for applying data quality validations to a pandas DataFrame."""
    def execute(self, sources: Dict[str, pd.DataFrame] = None, **kwargs) -> Dict[str, pd.DataFrame]:
        self.logger.info("--- Step: Validate (Pandas) ---")

        df = sources.get('_output')

        for column_spec in self.config.columns:
            for rule in column_spec.validation_rules:
                rule_name, params = self._parse_rule(rule.rule)
                validator = self._create_validator(rule_name, params)

                if validator:
                    failures_df, success_df = validator.apply(df, column_spec.rename or column_spec.name)
                    if not failures_df.empty:
                        if rule.on_fail == 'fail':
                            raise ValidationError(f"Validation failed for column '{column_spec.name}' with rule '{rule.rule}'.")
                        elif rule.on_fail == 'drop':
                            df = success_df
                            self.logger.info(f"Dropped {len(failures_df)} rows from column '{column_spec.name}' due to validation rule '{rule.rule}'.")
                        elif rule.on_fail == 'warn':
                            self.logger.warning(f"Validation failed for column '{column_spec.name}' with rule '{rule.rule}'.")
        
        sources['_output'] = df

        return sources

    def _parse_rule(self, rule_string: str) -> tuple:
        parts = rule_string.split(':', 1)
        return parts[0], parts[1] if len(parts) > 1 else None

    def _create_validator(self, rule_name: str, param: str):

        rule_class = get_validation_rule(rule_name)

        if not rule_class:
            self.logger.warning(f"Validation rule '{rule_name}' is not registered. Skipping.")
            return None
        else:
            return rule_class(param)



        # if rule_name == 'not_null':
        #     return NotNullValidation()
        # elif rule_name == 'isin':
        #     return IsInValidation({'allowed_values_str': param})
        # else:
        #     self.logger.warning(f"Unknown validation rule '{rule_name}'. Skipping.")
        #     return None
