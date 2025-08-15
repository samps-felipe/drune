from pyspark.sql import DataFrame, functions as F
from ....core.step import BaseStep, register_step
from ....core.quality import get_validationrule
from ....utils.exceptions import ValidationError, ConfigurationError

@register_step('validate')
class ValidateStep(BaseStep):
    """
    Orchestrates the execution of column and table validations.
    """
    def _parse_rule(self, rule_string: str) -> tuple:
        """Extracts rule name and parameter. Ex: 'pattern:regex' -> ('pattern', 'regex')"""
        parts = rule_string.split(':', 1)
        return parts[0], parts[1] if len(parts) > 1 else None

    def _create_validator(self, rule_name: str, param: str):
        """Creates a validator instance using the registry, passing all parameters as a dict."""
        rule_class = get_validationrule(rule_name)
        if not rule_class:
            self.logger.warning(f"Unknown validation rule '{rule_name}'. Skipping.")
            return None
        try:
            params = {}
            if param:
                params = {f"{rule_name}_param": param}
                params["pattern"] = param
                params["allowed_values_str"] = param
                params["value_str"] = param
                params["bounds_str"] = param
            return rule_class(params)
        except (TypeError, ValueError) as e:
            raise ConfigurationError(f"Failed to instantiate rule '{rule_name}' with param '{param}'. Error: {e}") from e

    def execute(self, df: DataFrame, **kwargs) -> tuple[DataFrame, DataFrame]:
        self.logger.info("--- Step: Validation (Spark) ---")
        df_to_validate = df
        all_failures_list = []

        # --- 1. PROCESS 'WARN' VALIDATIONS ---
        self.logger.info("Executing 'warn' validations...")
        for spec in self.config.columns:
            final_column_name = self.engine._get_final_column_name(spec, self.config.defaults)
            for validation in [v for v in spec.validation_rules if v.on_fail == 'warn']:
                rule_name, param = self._parse_rule(validation.rule)
                validator = self._create_validator(rule_name, param)
                if validator:
                    failures_df, _ = validator.apply(df_to_validate, final_column_name)
                    if failures_df.count() > 0:
                        self.logger.warning(f"{failures_df.count()} records failed rule '{validation.rule}' for column '{final_column_name}'.")
                        
                        log_select_exprs = [
                            F.lit(self.config.pipeline_name).alias("pipeline_name"),
                            F.lit(validation.rule).alias("validation_rule"),
                            F.lit(final_column_name).alias("failed_column"),
                            F.col(final_column_name).cast("string").alias("failed_value"),
                            F.current_timestamp().alias("log_timestamp")
                        ]

                        if "hash_key" in failures_df.columns:
                            log_select_exprs.append(F.col("hash_key"))
                        else:
                            log_select_exprs.append(F.lit(None).cast("string").alias("hash_key"))
                        
                        log_info_df = failures_df.select(*log_select_exprs)
                        all_failures_list.append(log_info_df)

        # --- 2. PROCESS 'DROP' VALIDATIONS ---
        self.logger.info("Executing 'drop' validations...")
        for spec in self.config.columns:
            final_column_name = self.engine._get_final_column_name(spec, self.config.defaults)
            for validation in [v for v in spec.validation_rules if v.on_fail == 'drop']:
                rule_name, param = self._parse_rule(validation.rule)
                validator = self._create_validator(rule_name, param)
                if validator:
                    failures_df, success_df = validator.apply(df_to_validate, final_column_name)
                    if failures_df.count() > 0:
                        self.logger.warning(f"{failures_df.count()} records dropped by rule '{validation.rule}' on column '{final_column_name}'.")
                        df_to_validate = success_df

        # --- 3. PROCESS 'FAIL' VALIDATIONS ---
        self.logger.info("Executing 'fail' validations...")
        for spec in self.config.columns:
            final_column_name = self.engine._get_final_column_name(spec, self.config.defaults)
            for validation in [v for v in spec.validation_rules if v.on_fail == 'fail']:
                rule_name, param = self._parse_rule(validation.rule)
                validator = self._create_validator(rule_name, param)
                if validator:
                    failures_df, _ = validator.apply(df_to_validate, final_column_name)
                    if failures_df.count() > 0:
                        msg = f"{failures_df.count()} records failed critical rule '{validation.rule}' for column '{final_column_name}'."
                        self.logger.error(msg)
                        failures_df.show()
                        raise ValidationError(msg)

        # --- 4. TABLE VALIDATIONS ---
        self.logger.info("Executing table validations...")
        for table_val in self.config.table_validations:
            rule_class = get_validationrule(table_val.type)
            if rule_class:
                validator = rule_class(columns=table_val.columns)
                try:
                    validator.apply(df_to_validate)
                except ValueError as e:
                    self.logger.error(f"Table validation '{table_val.type}' failed. Action: {table_val.on_fail}", exc_info=True)
                    if table_val.on_fail == 'fail':
                        raise ValidationError(f"Table validation '{table_val.type}' failed.") from e
                    else:
                        self.logger.warning(f"Table validation '{table_val.type}' failed: {e}")

        # --- 5. CONSOLIDATE FAILURE LOGS ---
        final_log_df = None
        if all_failures_list:
            self.logger.info("Consolidating validation logs...")
            from functools import reduce
            
            base_log_schema = ["pipeline_name", "validation_rule", "failed_column", "failed_value", "log_timestamp", "hash_key"]
            
            def standardize_df(df_to_std):
                for col_name in base_log_schema:
                    if col_name not in df_to_std.columns:
                        df_to_std = df_to_std.withColumn(col_name, F.lit(None))
                return df_to_std.select(*base_log_schema)

            standardized_dfs = [standardize_df(df) for df in all_failures_list]
            final_log_df = reduce(lambda df1, df2: df1.unionByName(df2), standardized_dfs)

        self.logger.info("Validations completed.")
        return df_to_validate, final_log_df
