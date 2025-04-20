import ast
import logging

from spark_ast_lineage.analyzer.utils.evaluators.registry import register_evaluator

logger = logging.getLogger(__name__)


@register_evaluator(ast.Call)
def eval_call_string_methods(node: ast.Call, variables: dict[str, set], evaluate=None):
    """
    Evaluates basic string methods like .upper(), .lower(), etc.

    Args:
        node (ast.Call): The method call node.
        variables (dict): Variable context.

    Returns:
        str or None: The resulting string or None if not applicable.
    """
    if not isinstance(node.func, ast.Attribute):
        return None

    attr = node.func.attr
    base_val = evaluate(node.func.value, variables)

    if base_val is None:
        return None

    try:
        base_str = str(base_val)
        match attr:
            case "upper":
                return base_str.upper()
            case "lower":
                return base_str.lower()
            case "strip":
                return base_str.strip()
            case "capitalize":
                return base_str.capitalize()
            case "title":
                return base_str.title()

    except Exception as e:
        logger.warning(f"String method call failed: {e}")

    return None


__all__ = ["eval_call_string_methods"]
