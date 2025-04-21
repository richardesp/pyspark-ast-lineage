import ast
import logging

from pyspark_ast_lineage.analyzer.utils.safe_evaluator import SafeEvaluator
from pyspark_ast_lineage.analyzer.extractors.registry import register_extractor
from pyspark_ast_lineage.analyzer.extractors.base import BaseExtractor

logger = logging.getLogger(__name__)


@register_extractor("table")
class TableExtractor(BaseExtractor):
    """Extractor for `spark.read.table("table_name")` calls."""

    def extract(self, node: ast.Call, variables: dict[str, set]) -> set[str]:
        """
        Extracts table names from `spark.read.table("table_name")` calls.

        Args:
            node (ast.Call): The AST node representing a function call.
            variables (dict): Dictionary of pre-extracted variable values.

        Returns:
            set[str]: A set of extracted table names (usually just one).
        """
        tables = set()

        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            return tables

        if node.func.attr != "table":
            return tables

        if not node.args:
            logger.debug("No arguments found in .table() call")
            return tables

        table_arg = node.args[0]

        logger.debug(f"Attempting to evaluate table argument: {ast.dump(table_arg)}")
        evaluated = SafeEvaluator.evaluate(table_arg, variables)

        if evaluated is None:
            logger.debug("Failed to resolve table name statically.")
            return tables

        if isinstance(evaluated, str):
            tables.add(evaluated)
        elif isinstance(evaluated, set):
            tables.update(str(val) for val in evaluated if isinstance(val, str))
        else:
            logger.debug(f"Unexpected evaluation result type: {type(evaluated)}")

        return tables
