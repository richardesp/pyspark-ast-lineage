import ast
import logging
from typing import Any

from spark_ast_lineage.analyzer.utils.evaluators.registry import EVALUATOR_REGISTRY

logger = logging.getLogger(__name__)


class SafeEvaluator:
    @staticmethod
    def evaluate(expr_node: ast.AST, variables: dict[str, set]) -> Any:
        try:
            if isinstance(expr_node, (int, float, str, bool, list, tuple, dict)):
                return expr_node

            logger.debug(f"Trying to apply ast.literal_eval to {expr_node}")
            return ast.literal_eval(expr_node)

        except Exception:
            logger.debug(f"Failed evaluating {expr_node}")
            pass

        logger.debug(f"Evaluating type: {type(expr_node).__name__}")
        logger.debug(f"Current variables: {variables}")

        node_type = type(expr_node)
        evaluators = EVALUATOR_REGISTRY.get(node_type, [])

        for evaluator in evaluators:
            try:
                result = evaluator(
                    expr_node, variables, evaluate=SafeEvaluator.evaluate
                )
                if result is not None:
                    return result
            except Exception as e:
                logger.warning(f"Evaluator {evaluator.__name__} failed: {e}")

        return None
