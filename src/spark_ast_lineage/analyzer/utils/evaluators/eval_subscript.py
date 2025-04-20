import ast
import logging

from spark_ast_lineage.analyzer.utils.evaluators.registry import register_evaluator

logger = logging.getLogger(__name__)


@register_evaluator(ast.Subscript)
def eval_subscript(node: ast.Subscript, variables: dict[str, set], evaluate=None):
    """
    Evaluates subscript/index operations (e.g., x[0], d['key'], lst[:2]).

    Supports subscripting into values stored as stringified dictionaries or lists.

    Args:
        node (ast.Subscript): Subscript node.
        variables (dict): Variable context.

    Returns:
        Any or None: Result of indexing or slicing.
    """
    container = evaluate(node.value, variables)
    if container is None:
        return None

    slice_node = node.slice

    try:
        if isinstance(slice_node, ast.Slice):
            lower = evaluate(slice_node.lower, variables) if slice_node.lower else None
            upper = evaluate(slice_node.upper, variables) if slice_node.upper else None
            return container[lower:upper]

        if hasattr(slice_node, "value"):
            index = evaluate(slice_node.value, variables)
        elif isinstance(slice_node, ast.Constant):
            index = slice_node.value
        else:
            index = evaluate(slice_node, variables)

        # Handle set of stringified containers
        if isinstance(container, set):
            result = set()
            for value in container:
                try:
                    literal = ast.literal_eval(value)
                    if isinstance(literal, (dict, list, tuple)):
                        result.add(literal[index])
                except Exception:
                    continue
            return result or None

        # Handle single stringified container
        if isinstance(container, str):
            try:
                literal = ast.literal_eval(container)
                if isinstance(literal, (dict, list, tuple)):
                    return literal[index]
            except Exception:
                return None

        # Fallback for real containers
        return container[index]

    except Exception as e:
        logger.warning(f"Subscript access failed: {e}")
        return None


__all__ = ["eval_subscript"]
