import ast
import logging

import sqlparse
from sqlparse.sql import IdentifierList, Identifier, TokenList
from sqlparse.tokens import Keyword

from spark_ast_lineage.analyzer.safe_evaluator import SafeEvaluator
from spark_ast_lineage.analyzer.extractors.registry import register_extractor
from spark_ast_lineage.analyzer.extractors.base import BaseExtractor


logger = logging.getLogger(__name__)


@register_extractor("sql")
class SQLExtractor(BaseExtractor):
    """Extractor for `spark.sql("SELECT * FROM ...")` calls."""

    def extract(self, node: ast.Call, variables: dict[str, set]) -> set[str]:
        """
        Extracts table names from `spark.sql(...)` calls.

        Args:
            node (ast.Call): The AST node representing a function call.
            variables (dict): Dictionary of pre-extracted variable values.

        Returns:
            set[str]: A set of extracted table names used in the SQL query.
        """
        tables = set()

        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            return tables

        if node.func.attr != "sql":
            return tables

        if not node.args:
            logger.debug("No arguments found in .sql() call")
            return tables

        sql_expr = node.args[0]

        logger.debug(f"Attempting to evaluate SQL query argument: {ast.dump(sql_expr)}")
        evaluated = SafeEvaluator.evaluate(sql_expr, variables)

        if not isinstance(evaluated, str):
            logger.debug("Failed to resolve SQL query statically.")
            return tables

        logger.debug(f"Evaluated SQL string: {evaluated}")
        tables.update(self._extract_tables_from_sql(evaluated))

        return tables

    @staticmethod
    def _extract_tables_from_sql(sql_query: str) -> set[str]:
        """
        Extracts real source table names from a SQL query.
        - Skips CTE aliases
        - Recursively traverses nested queries and tokens

        Args:
            sql_query (str): SQL code to parse.

        Returns:
            set[str]: Table names used in the query.
        """

        tables = set()
        cte_aliases = set()

        parsed = sqlparse.parse(sql_query)
        if not parsed:
            return set()

        def extract_cte_aliases(token_list: TokenList):
            """
            Collects aliases from WITH clause (e.g., WITH alias AS (...))
            """
            seen_with = False

            for token in token_list.tokens:
                if token.ttype is Keyword.CTE and token.normalized == "WITH":
                    seen_with = True
                    continue

                if seen_with:
                    if isinstance(token, IdentifierList):
                        for ident in token.get_identifiers():
                            name = ident.get_real_name()
                            if name:
                                cte_aliases.add(name)

                    elif isinstance(token, Identifier):
                        name = token.get_real_name()
                        if name:
                            cte_aliases.add(name)

                    # stop collecting after first statement
                    if token.normalized == "SELECT":
                        break

        def extract_table_names(token_list: TokenList):
            """
            Recursively searches for real table names in FROM / JOIN / INTO / UPDATE clauses
            """
            for token in token_list.tokens:
                if token.is_group:
                    extract_table_names(token)

                if token.ttype is Keyword and token.normalized in {
                    "FROM",
                    "JOIN",
                    "UPDATE",
                    "INTO",
                }:

                    idx = token_list.token_index(token)
                    next_token = token_list.token_next(idx, skip_ws=True, skip_cm=True)

                    if next_token:
                        next_token = next_token[
                            1
                        ]  # token_next returns tuple (idx, token)

                        if isinstance(next_token, IdentifierList):

                            for identifier in next_token.get_identifiers():
                                name = identifier.get_real_name()
                                if name and name not in cte_aliases:
                                    tables.add(name)

                        elif isinstance(next_token, Identifier):
                            name = next_token.get_real_name()
                            if name and name not in cte_aliases:
                                tables.add(name)

        for statement in parsed:
            extract_cte_aliases(statement)
            extract_table_names(statement)

        return tables
