import ast
import logging

from spark_ast_lineage.analyzer.utils.evaluators.registry import register_evaluator

logger = logging.getLogger(__name__)


@register_evaluator(ast.Call)
def eval_call_dict_methods(node: ast.Call, variables: dict[str, set], evaluate=None):
    """
    Evaluates common string/dict methods like .get(), .format(), .join().

    Args:
        node (ast.Call): Call node with method call.
        variables (dict): Variable context.

    Returns:
        Any or None: Result of method call or None.
    """
    if not isinstance(node.func, ast.Attribute):
        return None

    attr = node.func.attr
    base = evaluate(node.func.value, variables)

    if attr == "get":
        if isinstance(base, set):
            for item in base:
                try:
                    parsed = ast.literal_eval(item)
                    if isinstance(parsed, dict):
                        key = evaluate(node.args[0], variables)
                        if isinstance(key, set):
                            results = set()
                            for k in key:
                                val = parsed.get(k)
                                if val is not None:
                                    results.add(val)
                            return results or None
                        else:
                            default = (
                                evaluate(node.args[1], variables)
                                if len(node.args) > 1
                                else None
                            )
                            return parsed.get(key, default)
                except Exception as e:
                    logger.warning(f"Failed parsing string to dict: {e}")

        elif isinstance(base, str):
            try:
                parsed = ast.literal_eval(base)
                if isinstance(parsed, dict):
                    key = evaluate(node.args[0], variables)
                    default = (
                        evaluate(node.args[1], variables)
                        if len(node.args) > 1
                        else None
                    )
                    return parsed.get(key, default)
            except Exception as e:
                logger.warning(f"Failed parsing base as dict: {e}")

        elif isinstance(base, dict):
            key = evaluate(node.args[0], variables)
            default = evaluate(node.args[1], variables) if len(node.args) > 1 else None
            return base.get(key, default)

    elif attr == "format":
        try:
            args = [evaluate(arg, variables) for arg in node.args]
            if None not in args:
                return base.format(*args)
        except Exception as e:
            logger.warning(f"format() evaluation failed: {e}")

    elif attr == "join":
        if node.args:
            iterable = evaluate(node.args[0], variables)
            result = set()

            if isinstance(iterable, set):
                for val in iterable:
                    try:
                        parsed = ast.literal_eval(val) if isinstance(val, str) else val
                        if isinstance(parsed, list) and all(
                            isinstance(x, str) for x in parsed
                        ):
                            result.add(base.join(parsed))
                    except Exception as e:
                        logger.warning(f"Join evaluation failed: {e}")
                return result if result else None

            elif isinstance(iterable, str):
                try:
                    parsed = ast.literal_eval(iterable)
                    if isinstance(parsed, list) and all(
                        isinstance(x, str) for x in parsed
                    ):
                        return base.join(parsed)
                except Exception as e:
                    logger.warning(f"Join evaluation failed on str: {e}")

            elif isinstance(iterable, list) and all(
                isinstance(x, str) for x in iterable
            ):
                return base.join(iterable)

    return None


__all__ = ["eval_call_dict_methods"]
