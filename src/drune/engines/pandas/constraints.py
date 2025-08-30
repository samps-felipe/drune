from typing import Any, Dict, Tuple
import pandas as pd
from drune.core.quality.constraint import BaseConstraintRule, register_constraint
import datetime


class PandasConstraintRule(BaseConstraintRule):
    """Base class for all column constraints in Pandas."""
    def _format_dataframe_fail(self, df: pd.DataFrame):
        df = df.reset_index()

        key_column_name = '_hash_key' if '_hash_key' in df.columns else 'index'
        df['key_column_name'] = key_column_name
        df['key_column_value'] = df[key_column_name]
        df['constraint_name'] = self.name
        df['constraint_action'] = 'warn'
        df['column_name'] = self.column
        df['message'] = f"Warning for value in column '{self.column}'"
        df['created_at'] = datetime.datetime.now()

        df = df.drop(columns=[self.uuid])

        print(df.columns)

        return df
        


    def fail(self, df: pd.DataFrame) -> pd.DataFrame:
        """Returns the DataFrame rows that failed the constraint."""
        mask = df[self.uuid]
        fail_df = df.loc[~mask].copy()
        fail_df = self._format_dataframe_fail(fail_df)

        df.drop(columns=[self.uuid], inplace=True)

        self.logger.error(
            f"{len(fail_df)} failed values in column '{self.column}'"
        )

        return df, fail_df

    def drop(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Returns the DataFrame with failed rows dropped."""
        mask = df[self.uuid]
        fail_df = df.loc[~mask].copy()
        fail_df = self._format_dataframe_fail(fail_df)

        df.drop(columns=[self.uuid], inplace=True)

        new_df = df[mask]

        self.logger.warning(
            f"{len(fail_df)} dropped values in column '{self.column}'"
        )

        return new_df, fail_df

    def warn(self, df: pd.DataFrame) -> None:
        """Logs a warning for failed rows."""
        mask = df[self.uuid]
        fail_df = df.loc[~mask].copy()
        fail_df = self._format_dataframe_fail(fail_df)

        df.drop(columns=[self.uuid], inplace=True)

        if not fail_df.empty:
            self.logger.warning(
                f"{len(fail_df)} warning values in column '{self.column}'"
            )
        
        return df, fail_df

@register_constraint('pandas', 'not_null')
class NotNullConstraint(PandasConstraintRule):
    def apply(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        self.column = params['column_name']
        mask = df[self.column].notna()
        df[self.uuid] = mask
        return df

@register_constraint('pandas', 'unique')
class UniqueConstraint(PandasConstraintRule):
    def apply(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        self.column = params['column_name']
        mask = ~df[self.column].duplicated(keep=False)
        df[self.uuid] = mask
        return df

@register_constraint('pandas', 'isin')
class IsInConstraint(PandasConstraintRule):
    def apply(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        self.column = params['column_name']
        allowed_values = params[0]
        mask = df[self.column].isin(allowed_values)
        df[self.uuid] = mask
        return df

@register_constraint('pandas', 'pattern')
class PatternConstraint(PandasConstraintRule):
    def apply(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        self.column = params['column_name']
        pattern = params.get('pattern') or params.get(0)

        if not pattern:
            raise ValueError("Pattern not provided for pattern constraint.")
        mask = df[self.column].str.match(pattern)
        df[self.uuid] = mask
        return df
