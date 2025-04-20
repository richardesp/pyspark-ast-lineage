import ast
import logging

from spark_ast_lineage.analyzer.utils.evaluators.registry import register_evaluator

logger = logging.getLogger(__name__)


@register_evaluator(ast.JoinedStr)
def eval_joinedstr(node: ast.JoinedStr, variables: dict[str, set], evaluate=None):
    """
    Evaluates f-strings (`JoinedStr`).

    Args:
        node (ast.JoinedStr): F-string node.
        variables (dict): Variable context.

    Returns:
        str or None: The resulting string or None.
    """
    parts = []

    for val in node.values:
        if isinstance(val, ast.FormattedValue):
            subval = evaluate(val.value, variables)

            if isinstance(subval, set) and len(subval) == 1:
                subval = next(iter(subval))

            if isinstance(subval, str):
                parts.append(subval)
            elif subval is not None:
                parts.append(str(subval))
            else:
                return None

        elif isinstance(val, ast.Constant):
            parts.append(str(val.value))

    return "".join(parts)


__all__ = ["eval_joinedstr"]
