import pandas as pd
from ....core.step import BaseStep, register_step
from ..quality.rules import NotNullValidation, IsInValidation
from ....utils.exceptions import ValidationError

@register_step('validate')
class ValidateStep(BaseStep):
    """Step responsible for applying data quality validations to a pandas DataFrame."""
    def execute(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        self.logger.info("--- Step: Validate (Pandas) ---")

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
        return df

    def _parse_rule(self, rule_string: str) -> tuple:
        parts = rule_string.split(':', 1)
        return parts[0], parts[1] if len(parts) > 1 else None

    def _create_validator(self, rule_name: str, param: str):
        if rule_name == 'not_null':
            return NotNullValidation()
        elif rule_name == 'isin':
            return IsInValidation({'allowed_values_str': param})
        else:
            self.logger.warning(f"Unknown validation rule '{rule_name}'. Skipping.")
            return None
