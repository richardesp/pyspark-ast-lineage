import ast
import logging

logger = logging.getLogger(__name__)


class SafeEvaluator:
    """
    Evaluates AST nodes safely with support for variable lookups and static analysis
    for literals, binops, method calls, indexing, slicing, etc.
    """

    @staticmethod
    def evaluate(expr_node, variables):
        try:
            # Skip literal_eval if already a native constant
            if isinstance(expr_node, (int, float, str, bool, list, tuple, dict)):
                return expr_node

            logger.debug(f"Trying to apply ast.literal_eval to {expr_node}")
            return ast.literal_eval(expr_node)
        except Exception:
            logger.debug(f"Failed evaluating {expr_node}")
            pass  # fallback below

        logger.debug(f"expr_node type: {type(expr_node)}")
        logger.debug(f"Current variables lookup: {variables}")

        if isinstance(expr_node, ast.Attribute):
            name = SafeEvaluator._get_attribute_name(expr_node)
            if name and name in variables:
                values = variables[name]
                if len(values) == 1:
                    return next(iter(values))
                elif values:
                    return next(iter(values))
            return None

        if isinstance(expr_node, ast.IfExp):

            test = SafeEvaluator.evaluate(expr_node.test, variables)
            body = SafeEvaluator.evaluate(expr_node.body, variables)
            orelse = SafeEvaluator.evaluate(expr_node.orelse, variables)

            logger.debug(f"Inside ifelse one line expression: {test, body, orelse}")
            logger.debug(f"Returning: {set((body, orelse))}")

            return {body, orelse}  # set of both possible values

        if isinstance(expr_node, ast.Name):
            var_name = expr_node.id
            if var_name in variables:
                values = variables[var_name]
                logger.debug(f"Detected expr_node with a ast.Name type: {values}")

                if values:
                    return values

            return None

        # Handle string methods on constants or variables
        if isinstance(expr_node, ast.Call) and isinstance(
            expr_node.func, ast.Attribute
        ):
            attr = expr_node.func.attr
            base_expr = expr_node.func.value
            base_val = SafeEvaluator.evaluate(base_expr, variables)

            if base_val is not None:
                base_str = str(base_val)
                logger.debug(f"Evaluating string method: {base_str}.{attr}()")
                try:
                    if attr == "upper":
                        return base_str.upper()
                    elif attr == "lower":
                        return base_str.lower()
                    elif attr == "strip":
                        return base_str.strip()
                    elif attr == "capitalize":
                        return base_str.capitalize()
                    elif attr == "title":
                        return base_str.title()
                except Exception as e:
                    logger.warning(f"String method call failed: {e}")
                    return None

        # Handle BinOp: string concat, multiplication, percent-formatting
        if isinstance(expr_node, ast.BinOp):
            left = SafeEvaluator.evaluate(expr_node.left, variables)
            right = SafeEvaluator.evaluate(expr_node.right, variables)

            if isinstance(expr_node.op, ast.Add):
                logger.debug("Handling string concatenation with +")
                if left is not None and right is not None:
                    return str(left) + str(right)

            elif isinstance(expr_node.op, ast.Mult):
                logger.debug("Handling string multiplication with *")
                if isinstance(left, str) and isinstance(right, int):
                    return left * right
                elif isinstance(right, str) and isinstance(left, int):
                    return right * left

            elif isinstance(expr_node.op, ast.Mod):
                logger.debug("Handling percent string formatting with % operator")
                if isinstance(left, str):
                    try:
                        return left % right
                    except Exception as e:
                        logger.warning(f"Failed to format string using % operator: {e}")

        # Handle f-strings
        if isinstance(expr_node, ast.JoinedStr):
            parts = []
            for val in expr_node.values:
                if isinstance(val, ast.FormattedValue):
                    subval = SafeEvaluator.evaluate(val.value, variables)
                    if subval is None:
                        return None
                    parts.append(str(subval))
                elif isinstance(val, ast.Constant):
                    parts.append(str(val.value))
            return "".join(parts)

        # Handle subscript access like x[0], d["key"], list[:2]
        if isinstance(expr_node, ast.Subscript):
            container_node = expr_node.value
            slice_node = expr_node.slice

            # Try to evaluate the container if it's a constant
            if isinstance(container_node, ast.Constant):
                try:
                    container = ast.literal_eval(container_node)
                except Exception as e:
                    logger.warning(f"Failed to literal_eval container: {e}")
                    return None
            else:
                container = SafeEvaluator.evaluate(container_node, variables)

            # Slicing case (e.g., val[:10])
            if isinstance(slice_node, ast.Slice):
                logger.debug("Trying to evaluate slicing")
                lower = (
                    SafeEvaluator.evaluate(slice_node.lower, variables)
                    if slice_node.lower
                    else None
                )
                upper = (
                    SafeEvaluator.evaluate(slice_node.upper, variables)
                    if slice_node.upper
                    else None
                )
                try:
                    return container[lower:upper]
                except Exception as e:
                    logger.warning(f"Failed to slice: {e}")
                    return None

            # Indexing case (e.g., val[0], val['key'])
            try:
                if hasattr(slice_node, "value"):
                    index = SafeEvaluator.evaluate(slice_node.value, variables)
                elif isinstance(slice_node, ast.Constant):
                    index = slice_node.value
                else:
                    index = SafeEvaluator.evaluate(slice_node, variables)

                return container[index]
            except Exception as e:
                logger.warning(f"Subscript access failed: {e}")
                return None

        # Handle .format(), .join() and .get()
        if isinstance(expr_node, ast.Call) and isinstance(
            expr_node.func, ast.Attribute
        ):
            attr = expr_node.func.attr

            if attr == "get":
                logger.debug("Handling dict.get() method")
                dict_obj = SafeEvaluator.evaluate(expr_node.func.value, variables)

                if isinstance(dict_obj, dict) and len(expr_node.args) >= 1:
                    key = SafeEvaluator.evaluate(expr_node.args[0], variables)
                    default = (
                        SafeEvaluator.evaluate(expr_node.args[1], variables)
                        if len(expr_node.args) > 1
                        else None
                    )

                    result = dict_obj.get(key, default)
                    logger.debug(
                        f"dict.get() result: {result} (key={key}, default={default})"
                    )
                    return result
                else:
                    logger.debug("Skipped dict.get(): Not a dict or missing args")

            elif attr == "format":
                logger.debug("Handling str.format() method")

                base = SafeEvaluator.evaluate(expr_node.func.value, variables)
                if isinstance(base, str):
                    try:
                        args = [
                            SafeEvaluator.evaluate(arg, variables)
                            for arg in expr_node.args
                        ]
                        if None not in args:
                            result = base.format(*args)
                            logger.debug(f"format() result: {result}")
                            return result
                    except Exception as e:
                        logger.warning(f"format() evaluation failed: {e}")
                else:
                    logger.debug("Skipped format(): Base is not a string")

            elif attr == "join":
                logger.debug("Handling str.join() method")

                separator = SafeEvaluator.evaluate(expr_node.func.value, variables)
                if isinstance(separator, str) and expr_node.args:
                    iterable = SafeEvaluator.evaluate(expr_node.args[0], variables)
                    if isinstance(iterable, list) and all(
                        isinstance(x, str) for x in iterable
                    ):
                        result = separator.join(iterable)
                        logger.debug(f"join() result: {result}")
                        return result
                    else:
                        logger.debug("Skipped join(): Iterable is not list of strings")
                else:
                    logger.debug(
                        "Skipped join(): Separator is not a string or missing args"
                    )

        return None  # fallback

    @staticmethod
    def _get_attribute_name(node):
        """
        Extracts attribute names from AST nodes, handling nested attributes.

        Args:
            node (ast.Attribute): The attribute node.

        Returns:
            str: The fully qualified attribute name.
        """
        parts = []
        while isinstance(node, ast.Attribute):
            parts.insert(0, node.attr)
            node = node.value
        if isinstance(node, ast.Name):
            parts.insert(0, node.id)
            return ".".join(parts)
        return None
