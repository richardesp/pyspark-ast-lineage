import ast
import logging
from typing import Any

logger = logging.getLogger(__name__)


class SafeEvaluator:
    """
    Evaluates AST nodes statically and safely using a provided variable context recursive solving.
    Supports basic operations, attribute access, method calls, and string formatting.
    """

    @staticmethod
    def evaluate(expr_node: ast.AST, variables: dict[str, set]) -> Any:
        """
        Attempts to statically evaluate a Python AST node given a dictionary of variable values.

        Args:
            expr_node (ast.AST): The node to evaluate.
            variables (dict[str, set]): A map of variable names to their possible values.

        Returns:
            Any: The evaluated result, or None if evaluation failed or is unsupported.
        """
        try:
            if isinstance(expr_node, (int, float, str, bool, list, tuple, dict)):
                return expr_node

            logger.debug(f"Trying to apply ast.literal_eval to {expr_node}")
            return ast.literal_eval(expr_node)

        except Exception:
            logger.debug(f"Failed evaluating {expr_node}")
            pass

        logger.debug(f"Evaluating type: {type(expr_node).__name__}")
        logger.debug(f"Current variables: {variables}")

        # Delegated evaluations
        return (
            SafeEvaluator._eval_attribute(expr_node, variables)
            or SafeEvaluator._eval_ifexp(expr_node, variables)
            or SafeEvaluator._eval_name(expr_node, variables)
            or SafeEvaluator._eval_call_string_methods(expr_node, variables)
            or SafeEvaluator._eval_binop(expr_node, variables)
            or SafeEvaluator._eval_boolop(expr_node, variables)
            or SafeEvaluator._eval_joinedstr(expr_node, variables)
            or SafeEvaluator._eval_subscript(expr_node, variables)
            or SafeEvaluator._eval_call_dict_methods(expr_node, variables)
        )

    @staticmethod
    def _eval_attribute(node: ast.AST, variables: dict[str, set]):
        """
        Evaluates attribute access expressions (e.g., cfg.table_name).

        Args:
            node (ast.Attribute): The attribute node.
            variables (dict): Variable context.

        Returns:
            Any or None: The resolved value or None if not found.
        """
        if isinstance(node, ast.Attribute):
            name = SafeEvaluator._get_attribute_name(node)

            if name and name in variables:
                values = variables[name]
                return next(iter(values)) if values else None

        return None

    @staticmethod
    def _eval_ifexp(node, variables):
        """
        Evaluates ternary expressions (x if cond else y).

        Args:
            node (ast.IfExp): The ternary node.
            variables (dict): Variable context.

        Returns:
            set: A set with both possible outcomes (body, orelse).
        """
        if isinstance(node, ast.IfExp):
            test = SafeEvaluator.evaluate(node.test, variables)
            body = SafeEvaluator.evaluate(node.body, variables)
            orelse = SafeEvaluator.evaluate(node.orelse, variables)

            logger.debug(f"Evaluating ternary: {test=}, {body=}, {orelse=}")
            return {body, orelse}

        return None

    @staticmethod
    def _eval_name(node, variables):
        """
        Resolves variable names to their values.

        Args:
            node (ast.Name): The name node.
            variables (dict): Variable context.

        Returns:
            Any or None: The variable's value(s) or None.
        """
        logger.debug(f"Inside eval_name at the node: {ast.dump(node)}")

        if isinstance(node, ast.Name):
            var_name = node.id

            if var_name in variables:
                values = variables[var_name]

                resolved = set()
                for val in values:
                    if isinstance(val, str):
                        try:
                            logger.debug(f"Trying to evaluate: {val}")
                            parsed = ast.literal_eval(val)
                            logger.debug(f"Parsed value: {node}")
                            # Only allow parsing if the result is a container, not a primitive
                            if isinstance(parsed, (dict, list, tuple)):
                                evaluated = parsed
                            else:
                                evaluated = val  # Keep original string form to avoid numeric interpretation
                        except Exception:
                            evaluated = val

                    else:
                        evaluated = val

                    # Always store string representation (even if it was a dict originally to avoid hashing problems)
                    resolved.add(str(evaluated))

                return next(iter(resolved)) if len(resolved) == 1 else resolved

        return None

    @staticmethod
    def _eval_call_string_methods(node, variables):
        """
        Evaluates basic string methods like .upper(), .lower(), etc.

        Args:
            node (ast.Call): The method call node.
            variables (dict): Variable context.

        Returns:
            str or None: The resulting string or None if not applicable.
        """
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            base_val = SafeEvaluator.evaluate(node.func.value, variables)

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

    @staticmethod
    def _eval_boolop(node, variables):
        """
        Evaluates boolean operations like `or` and `and` between evaluated expressions.

        Args:
            node (ast.BoolOp): Boolean operation node.
            variables (dict): Variable context.

        Returns:
            Any or None: First truthy value for `or`, or final result for `and`.
        """
        if not isinstance(node, ast.BoolOp):
            return None

        if isinstance(node.op, ast.Or):
            for value in node.values:
                evaluated = SafeEvaluator.evaluate(value, variables)
                if evaluated:  # Return first truthy value
                    return evaluated
            return evaluated  # Return last one even if falsy

        elif isinstance(node.op, ast.And):
            result = None
            for value in node.values:
                evaluated = SafeEvaluator.evaluate(value, variables)
                if not evaluated:
                    return evaluated
                result = evaluated
            return result

        return None

    @staticmethod
    def _eval_binop(node, variables):
        """
        Safely evaluates binary operations in AST nodes.

        Handles string concatenation, string multiplication, and string formatting
        using `%`, supporting combinations of constants and variable sets.

        Args:
            node (ast.BinOp): The binary operation AST node.
            variables (dict[str, set]): Extracted variable values for evaluation.

        Returns:
            set | str | None: The evaluated result as a string, set of strings,
            or None if evaluation fails.
        """
        if not isinstance(node, ast.BinOp):
            return None

        # Recursively evaluate both sides
        left = SafeEvaluator.evaluate(node.left, variables)
        right = SafeEvaluator.evaluate(node.right, variables)

        # Normalize to sets
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
                            logger.warning(
                                f"Failed to format string using % operator: {e}"
                            )

        return result or None

    @staticmethod
    def _eval_joinedstr(node, variables):
        """
        Evaluates f-strings (`JoinedStr`).

        Args:
            node (ast.JoinedStr): F-string node.
            variables (dict): Variable context.

        Returns:
            str or None: The resulting string or None.
        """
        if not isinstance(node, ast.JoinedStr):
            return None

        parts = []

        for val in node.values:
            if isinstance(val, ast.FormattedValue):
                subval = SafeEvaluator.evaluate(val.value, variables)

                # Extract safely from sets
                if isinstance(subval, set) and len(subval) == 1:
                    subval = next(iter(subval))

                # Ensure it's a string if it was originally quoted
                if isinstance(subval, str):
                    parts.append(subval)
                elif subval is not None:
                    parts.append(str(subval))
                else:
                    return None

            elif isinstance(val, ast.Constant):
                parts.append(str(val.value))

        return "".join(parts)

    @staticmethod
    def _eval_subscript(node, variables):
        """
        Evaluates subscript/index operations (e.g., x[0], d['key'], lst[:2]).

        Supports subscripting into values stored as stringified dictionaries or lists.

        Args:
            node (ast.Subscript): Subscript node.
            variables (dict): Variable context.

        Returns:
            Any or None: Result of indexing or slicing.
        """
        if not isinstance(node, ast.Subscript):
            return None

        container = SafeEvaluator.evaluate(node.value, variables)
        if container is None:
            return None

        slice_node = node.slice

        try:
            if isinstance(slice_node, ast.Slice):
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
                return container[lower:upper]

            if hasattr(slice_node, "value"):
                index = SafeEvaluator.evaluate(slice_node.value, variables)
            elif isinstance(slice_node, ast.Constant):
                index = slice_node.value
            else:
                index = SafeEvaluator.evaluate(slice_node, variables)

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

    @staticmethod
    def _eval_call_dict_methods(node, variables):
        """
        Evaluates common string/dict methods like .get(), .format(), .join().

        Args:
            node (ast.Call): Call node with method call.
            variables (dict): Variable context.

        Returns:
            Any or None: Result of method call or None.
        """
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            base = SafeEvaluator.evaluate(node.func.value, variables)

            if attr == "get":

                # Case 1: base is a set of stringified dicts
                if isinstance(base, set):

                    for item in base:
                        try:
                            parsed = ast.literal_eval(item)

                            if isinstance(parsed, dict):
                                key = SafeEvaluator.evaluate(node.args[0], variables)

                                if isinstance(key, set):

                                    # multiple keys (e.g., ternary)
                                    results = set()

                                    for k in key:
                                        val = parsed.get(k)
                                        if val is not None:
                                            results.add(val)

                                    return results or None

                                else:
                                    default = (
                                        SafeEvaluator.evaluate(node.args[1], variables)
                                        if len(node.args) > 1
                                        else None
                                    )
                                    return parsed.get(key, default)

                        except Exception as e:
                            logger.warning(f"Failed parsing string to dict: {e}")

                # Case 2: base is a stringified dict
                elif isinstance(base, str):
                    try:
                        parsed = ast.literal_eval(base)

                        if isinstance(parsed, dict):
                            key = SafeEvaluator.evaluate(node.args[0], variables)
                            default = (
                                SafeEvaluator.evaluate(node.args[1], variables)
                                if len(node.args) > 1
                                else None
                            )

                            return parsed.get(key, default)

                    except Exception as e:
                        logger.warning(f"Failed parsing base as dict: {e}")

                # Case 3: base is already a dict
                elif isinstance(base, dict):

                    key = SafeEvaluator.evaluate(node.args[0], variables)
                    default = (
                        SafeEvaluator.evaluate(node.args[1], variables)
                        if len(node.args) > 1
                        else None
                    )

                    return base.get(key, default)

            elif attr == "format":

                try:
                    args = [SafeEvaluator.evaluate(arg, variables) for arg in node.args]
                    if None not in args:
                        return base.format(*args)

                except Exception as e:
                    logger.warning(f"format() evaluation failed: {e}")

            elif attr == "join":
                if node.args:
                    iterable = SafeEvaluator.evaluate(node.args[0], variables)

                    result = set()

                    if isinstance(iterable, set):
                        for val in iterable:
                            try:
                                parsed = (
                                    ast.literal_eval(val)
                                    if isinstance(val, str)
                                    else val
                                )
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

        return None

    @staticmethod
    def _get_attribute_name(node: ast.Attribute) -> str | None:
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
