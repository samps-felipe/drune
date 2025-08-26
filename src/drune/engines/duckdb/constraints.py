from typing import Any, Dict, Tuple
import duckdb
from drune.core.quality import BaseConstraintRule, register_constraint
import datetime


class DuckDBConstraintRule(BaseConstraintRule):
    """Base class for all column constraints in DuckDB."""

    def _get_fail_relation(self, relation: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
        return relation.filter(f'not "{self.uuid}"')

    def fail(
        self,
        relation: duckdb.DuckDBPyRelation,
    ) -> Tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation]:
        fail_relation = self._get_fail_relation(relation)
        self.logger.error(f"{len(fail_relation)} failed values in column '{self.column}'")
        return relation.select(f'* , "{self.uuid}" as "_drune_constraint_status"'), fail_relation

    def drop(
        self,
        relation: duckdb.DuckDBPyRelation,
    ) -> Tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation]:
        fail_relation = self._get_fail_relation(relation)
        self.logger.warning(f"{len(fail_relation)} dropped values in column '{self.column}'")
        return relation.filter(f'"{self.uuid}"' ), fail_relation

    def warn(
        self,
        relation: duckdb.DuckDBPyRelation,
    ) -> Tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation]:
        fail_relation = self._get_fail_relation(relation)
        if not fail_relation.fetchone() is None:
            self.logger.warning(
                f"{len(fail_relation)} warning values in column '{self.column}'"
            )
        return relation, fail_relation

    def _apply_constraint(
        self,
        relation: duckdb.DuckDBPyRelation,
        condition: str,
    ) -> duckdb.DuckDBPyRelation:
        return relation.select(f'*, ({condition}) AS "{self.uuid}"')


@register_constraint("duckdb", "not_null")
class NotNullConstraint(DuckDBConstraintRule):
    def apply(
        self,
        relation: duckdb.DuckDBPyRelation,
        params: Dict[str, Any],
    ) -> duckdb.DuckDBPyRelation:
        self.column = params["column_name"]
        condition = f'"{self.column}" IS NOT NULL'
        return self._apply_constraint(relation, condition)


@register_constraint("duckdb", "unique")
class UniqueConstraint(DuckDBConstraintRule):
    def apply(
        self,
        relation: duckdb.DuckDBPyRelation,
        params: Dict[str, Any],
    ) -> duckdb.DuckDBPyRelation:
        self.column = params["column_name"]
        condition = f'COUNT(*) OVER (PARTITION BY "{self.column}") = 1'
        return self._apply_constraint(relation, condition)


@register_constraint("duckdb", "isin")
class IsInConstraint(DuckDBConstraintRule):
    def apply(
        self,
        relation: duckdb.DuckDBPyRelation,
        params: Dict[str, Any],
    ) -> duckdb.DuckDBPyRelation:
        self.column = params["column_name"]
        allowed_values = params[0]
        values_str = ", ".join([f"'{v}'" for v in allowed_values])
        condition = f'"{self.column}" IN ({values_str})'
        return self._apply_constraint(relation, condition)


@register_constraint("duckdb", "pattern")
class PatternConstraint(DuckDBConstraintRule):
    def apply(
        self,
        relation: duckdb.DuckDBPyRelation,
        params: Dict[str, Any],
    ) -> duckdb.DuckDBPyRelation:
        self.column = params["column_name"]
        pattern = params.get("pattern") or params.get(0)

        if not pattern:
            raise ValueError("Pattern not provided for pattern constraint.")
        condition = f"regexp_matches(\"{self.column}\", '{pattern}')"
        return self._apply_constraint(relation, condition)