import ast
import logging

from spark_ast_lineage.analyzer.safe_evaluator import SafeEvaluator
from spark_ast_lineage.analyzer.extractors.base import BaseExtractor
from spark_ast_lineage.analyzer.extractors.registry import register_extractor

logger = logging.getLogger(__name__)


@register_extractor("saveAsTable")
@register_extractor("insertInto")
@register_extractor("save")
@register_extractor("parquet")
@register_extractor("json")
@register_extractor("csv")
class WriteExtractor(BaseExtractor):
    """
    Extractor for PySpark write methods such as:
        - df.write.saveAsTable("...")
        - df.write.insertInto("...")
        - df.write.parquet("...")
        - df.write.save("/path")

    These methods are responsible for writing data either to tables or file paths.
    """

    def extract(self, node: ast.Call, variables: dict[str, set]) -> set[str]:
        """
        Extracts write destinations from write method calls in PySpark.

        Args:
            node (ast.Call): The AST node representing the method call.
            variables (dict): Dictionary of variable values already extracted.

        Returns:
            Set[str]: A set of extracted target destinations (tables or paths).
        """
        targets = set()

        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            return targets

        if not node.args:
            logger.debug("No arguments passed to write method.")
            return targets

        arg = node.args[0]
        logger.debug(f"Attempting to evaluate write target argument: {ast.dump(arg)}")

        evaluated = SafeEvaluator.evaluate(arg, variables)

        logger.debug(f"Retrieved evaluated: {evaluated}")

        if isinstance(evaluated, str):
            targets.add(evaluated)
        elif isinstance(evaluated, set):
            targets.update(str(val) for val in evaluated if isinstance(val, str))
        else:
            logger.debug(
                f"Write method returned unsupported value type: {type(evaluated)}"
            )

        return targets
