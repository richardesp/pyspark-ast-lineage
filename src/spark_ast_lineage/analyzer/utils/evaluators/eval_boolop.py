import ast
import logging

from spark_ast_lineage.analyzer.utils.evaluators.registry import register_evaluator

logger = logging.getLogger(__name__)


@register_evaluator(ast.BoolOp)
def eval_boolop(node: ast.BoolOp, variables: dict[str, set], evaluate=None):
    """
    Evaluates boolean operations like `or` and `and` between evaluated expressions.

    Args:
        node (ast.BoolOp): Boolean operation node.
        variables (dict): Variable context.

    Returns:
        Any or None: First truthy value for `or`, or final result for `and`.
    """
    if isinstance(node.op, ast.Or):
        for value in node.values:
            evaluated = evaluate(value, variables)
            if evaluated:  # Return first truthy value
                return evaluated
        return evaluated  # Last one, even if falsy

    elif isinstance(node.op, ast.And):
        result = None
        for value in node.values:
            evaluated = evaluate(value, variables)
            if not evaluated:
                return evaluated
            result = evaluated
        return result

    return None


__all__ = ["eval_boolop"]
