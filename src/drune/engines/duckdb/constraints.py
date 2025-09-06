from typing import Any, Dict, Tuple
from duckdb import DuckDBPyRelation
from drune.core.quality import BaseConstraint
from drune.core.quality import DataQualityManager
import datetime


class DuckDBConstraintRule(BaseConstraint):
    """Base class for all column constraints in DuckDB."""

    def _get_fail_relation(self, relation: DuckDBPyRelation) -> DuckDBPyRelation:
        return relation.filter(f'not "{self.name}"')

    def fail(
        self,
        relation: DuckDBPyRelation,
    ) -> Tuple[DuckDBPyRelation, DuckDBPyRelation]:
        fail_relation = self._get_fail_relation(relation)
        if len(fail_relation) > 0:
            self.logger.error(f"{len(fail_relation)} failed values in column '{self.column}'")
        return relation.select(f'* , "{self.name}" as "_drune_constraint_status"'), fail_relation

    def drop(
        self,
        relation: DuckDBPyRelation,
    ) -> Tuple[DuckDBPyRelation, DuckDBPyRelation]:
        fail_relation = self._get_fail_relation(relation)
        if len(fail_relation) > 0:
            self.logger.warning(f"{len(fail_relation)} dropped values in column '{self.column}'")
        return relation.filter(f'"{self.name}"' ), fail_relation

    def warn(
        self,
        relation: DuckDBPyRelation,
    ) -> Tuple[DuckDBPyRelation, DuckDBPyRelation]:
        fail_relation = self._get_fail_relation(relation)
        if len(fail_relation) > 0:
            self.logger.warning(
                f"{len(fail_relation)} warning values in column '{self.column}'"
            )
        return relation, fail_relation

    def _apply_constraint(
        self,
        relation: DuckDBPyRelation,
        condition: str,
    ) -> DuckDBPyRelation:
        return relation.select(f'*, ({condition}) AS "{self.name}"')


@DataQualityManager.register("duckdb", "not_null")
class NotNullConstraint(DuckDBConstraintRule):
    def apply(
        self,
        relation: DuckDBPyRelation,
        params: Dict[str, Any],
    ) -> DuckDBPyRelation:
        self.column = params["column_name"]
        self.name = f"_{self.name}_{self.column}_"
        condition = f'"{self.column}" IS NOT NULL'
        return self._apply_constraint(relation, condition)


@DataQualityManager.register("duckdb", "unique")
class UniqueConstraint(DuckDBConstraintRule):
    def apply(
        self,
        relation: DuckDBPyRelation,
        params: Dict[str, Any],
    ) -> DuckDBPyRelation:
        self.column = params["column_name"]
        self.name = f"_{self.name}_{self.column}_"
        condition = f'COUNT(*) OVER (PARTITION BY "{self.column}") = 1'
        return self._apply_constraint(relation, condition)


@DataQualityManager.register("duckdb", "isin")
class IsInConstraint(DuckDBConstraintRule):
    def apply(
        self,
        relation: DuckDBPyRelation,
        params: Dict[str, Any],
    ) -> DuckDBPyRelation:
        self.column = params["column_name"]
        self.name = f"_{self.name}_{self.column}_"
        allowed_values = params[0]
        values_str = ", ".join([f"'{v}'" for v in allowed_values])
        condition = f'"{self.column}" IN ({values_str})'
        return self._apply_constraint(relation, condition)


@DataQualityManager.register("duckdb", "pattern")
class PatternConstraint(DuckDBConstraintRule):
    def apply(
        self,
        relation: DuckDBPyRelation,
        params: Dict[str, Any],
    ) -> DuckDBPyRelation:
        self.column = params["column_name"]
        self.name = f"_{self.name}_{self.column}_"

        pattern = params.get("pattern") or params.get(0)

        if not pattern:
            raise ValueError("Pattern not provided for pattern constraint.")
        
        condition = f"regexp_full_match(\"{self.column}\", '{pattern}')"
        return self._apply_constraint(relation, condition)