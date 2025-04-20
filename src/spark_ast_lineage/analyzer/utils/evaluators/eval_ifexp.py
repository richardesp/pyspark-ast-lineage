import ast
import logging

from spark_ast_lineage.analyzer.utils.evaluators.registry import register_evaluator

logger = logging.getLogger(__name__)


@register_evaluator(ast.IfExp)
def eval_ifexp(node: ast.IfExp, variables: dict[str, set], evaluate=None):
    """
    Evaluates ternary expressions (x if cond else y).

    Args:
        node (ast.IfExp): The ternary node.
        variables (dict): Variable context.

    Returns:
        set: A set with both possible outcomes (body, orelse).
    """
    test = evaluate(node.test, variables)
    body = evaluate(node.body, variables)
    orelse = evaluate(node.orelse, variables)

    logger.debug(f"Evaluating ternary: {test=}, {body=}, {orelse=}")
    return {body, orelse}


__all__ = ["eval_ifexp"]
