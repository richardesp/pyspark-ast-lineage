import ast
import logging

from pyspark_ast_lineage.analyzer.utils.evaluators.registry import register_evaluator

logger = logging.getLogger(__name__)


@register_evaluator(ast.BinOp)
def eval_binop(node: ast.BinOp, variables: dict[str, set], evaluate=None):
    """
    Safely evaluates binary operations in AST nodes.

    Handles string concatenation, string multiplication, and string formatting
    using %, supporting combinations of constants and variable sets.

    Args:
        node (ast.BinOp): The binary operation AST node.
        variables (dict): Extracted variable values for evaluation.

    Returns:
        set | str | None: The evaluated result as a string, set of strings,
        or None if evaluation fails.
    """
    left = evaluate(node.left, variables)
    right = evaluate(node.right, variables)

    left_set = left if isinstance(left, set) else {left}
    right_set = right if isinstance(right, set) else {right}

    result = set()

    if isinstance(node.op, ast.Add):
        if isinstance(left, set) and isinstance(right, set):
            return {
                str(left_val) + str(right_val)
                for left_val in left
                for right_val in right
            }
        elif isinstance(left, set):
            return {str(left_val) + str(right) for left_val in left}
        elif isinstance(right, set):
            return {str(left) + str(right_val) for right_val in right}
        elif left is not None and right is not None:
            return str(left) + str(right)

    elif isinstance(node.op, ast.Mult):
        for left_val in left_set:
            for right_val in right_set:
                if isinstance(left_val, str) and isinstance(right_val, int):
                    result.add(left_val * right_val)
                elif isinstance(right_val, str) and isinstance(left_val, int):
                    result.add(right_val * left_val)

    elif isinstance(node.op, ast.Mod):
        for left_val in left_set:
            for right_val in right_set:
                if isinstance(left_val, str):
                    try:
                        result.add(left_val % right_val)
                    except Exception as e:
                        logger.warning(f"Failed to format string using % operator: {e}")

    return result or None


__all__ = ["eval_binop"]
