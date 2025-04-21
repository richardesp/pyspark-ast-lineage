import ast
import logging

from pyspark_ast_lineage.analyzer.utils.evaluators.registry import register_evaluator

logger = logging.getLogger(__name__)


@register_evaluator(ast.Name)
def eval_name(node: ast.Name, variables: dict[str, set], evaluate=None):
    """
    Resolves variable names to their values.

    Args:
        node (ast.Name): The name node.
        variables (dict): Variable context.

    Returns:
        Any or None: The variable's value(s) or None.
    """
    logger.debug(f"Inside eval_name at the node: {ast.dump(node)}")

    var_name = node.id

    if var_name in variables:
        values = variables[var_name]

        resolved = set()
        for val in values:
            if isinstance(val, str):
                try:
                    logger.debug(f"Trying to evaluate: {val}")
                    parsed = ast.literal_eval(val)
                    logger.debug(f"Parsed value: {parsed}")
                    # Only allow parsing if it's a container to avoid converting to int/float/etc
                    if isinstance(parsed, (dict, list, tuple)):
                        evaluated = parsed
                    else:
                        evaluated = val
                except Exception:
                    evaluated = val
            else:
                evaluated = val

            resolved.add(str(evaluated))

        return next(iter(resolved)) if len(resolved) == 1 else resolved

    return None


__all__ = ["eval_name"]
