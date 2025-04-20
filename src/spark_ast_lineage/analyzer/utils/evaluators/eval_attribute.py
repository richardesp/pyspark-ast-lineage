import ast
import logging

from spark_ast_lineage.analyzer.utils.evaluators.registry import register_evaluator

logger = logging.getLogger(__name__)


@register_evaluator(ast.Attribute)
def eval_attribute(node: ast.AST, variables: dict[str, set], evaluate=None):
    """
    Evaluates attribute access expressions (e.g., cfg.table_name).

    Args:
        node (ast.Attribute): The attribute node.
        variables (dict): Variable context.

    Returns:
        Any or None: The resolved value or None if not found.
    """
    if isinstance(node, ast.Attribute):
        name = _get_attribute_full_doted_name(node)

        if name and name in variables:
            values = variables[name]
            return next(iter(values)) if values else None

    return None


def _get_attribute_full_doted_name(node: ast.Attribute) -> str | None:
    """
    Recursively constructs the full dotted attribute name from an AST node.

    Example: for `self.source; cfg = Config()`, returns "cfg.source".

    Args:
        node (ast.Attribute): The attribute node.

    Returns:
        str | None: Fully qualified name or None if invalid.
    """
    parts = []

    while isinstance(node, ast.Attribute):
        parts.insert(0, node.attr)
        node = node.value

    if isinstance(node, ast.Name):
        parts.insert(0, node.id)
        return ".".join(parts)

    return None


__all__ = ["eval_attribute"]
