import ast
import logging

from spark_ast_lineage.analyzer.utils.safe_evaluator import SafeEvaluator
from spark_ast_lineage.analyzer.extractors.base import BaseExtractor
from spark_ast_lineage.analyzer.extractors.registry import register_extractor

logger = logging.getLogger(__name__)


@register_extractor("csv")
@register_extractor("parquet")
@register_extractor("json")
@register_extractor("orc")
@register_extractor("load")
class ReadExtractor(BaseExtractor):
    """
    Extractor for PySpark read methods such as:
        - spark.read.csv("...")
        - spark.read.parquet("...")
        - spark.read.json("...")
        - spark.read.orc("...")
        - spark.read.load("...")
    """

    def extract(self, node: ast.Call, variables: dict[str, set]) -> set[str]:
        """
        Extracts input paths from PySpark read method calls.

        Args:
            node (ast.Call): The AST node representing a function call.
            variables (dict): Dictionary of variable values.

        Returns:
            Set[str]: A set of extracted source paths.
        """
        sources = set()

        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            return sources

        if not node.args:
            logger.debug("No arguments passed to read method.")
            return sources

        arg = node.args[0]
        logger.debug(f"Attempting to evaluate read source argument: {ast.dump(arg)}")

        evaluated = SafeEvaluator.evaluate(arg, variables)

        logger.debug(f"Retrieved evaluated read path: {evaluated}")

        if isinstance(evaluated, str):
            sources.add(evaluated)
        elif isinstance(evaluated, set):
            sources.update(str(val) for val in evaluated if isinstance(val, str))
        else:
            logger.debug(
                f"Read method returned unsupported value type: {type(evaluated)}"
            )

        return sources
