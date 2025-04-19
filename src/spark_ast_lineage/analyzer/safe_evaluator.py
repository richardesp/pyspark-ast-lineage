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
        if isinstance(node, ast.Name):
            var_name = node.id

            if var_name in variables:
                values = variables[var_name]
                if values:
                    return values

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
    def _eval_binop(node, variables):
        """
        Evaluates binary operations like +, *, %, mostly for strings.

        Args:
            node (ast.BinOp): Binary operation node.
            variables (dict): Variable context.

        Returns:
            str or None: Result or None if unsupported.
        """
        if isinstance(node, ast.BinOp):
            left = SafeEvaluator.evaluate(node.left, variables)
            right = SafeEvaluator.evaluate(node.right, variables)

            if isinstance(node.op, ast.Add):
                if left is not None and right is not None:
                    return str(left) + str(right)

            elif isinstance(node.op, ast.Mult):
                if isinstance(left, str) and isinstance(right, int):
                    return left * right

                elif isinstance(right, str) and isinstance(left, int):
                    return right * left

            elif isinstance(node.op, ast.Mod):
                if isinstance(left, str):
                    try:
                        return left % right

                    except Exception as e:
                        logger.warning(f"String formatting failed: {e}")
        return None

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
        if isinstance(node, ast.JoinedStr):
            parts = []

            for val in node.values:
                if isinstance(val, ast.FormattedValue):
                    subval = SafeEvaluator.evaluate(val.value, variables)
                    if subval is None:
                        return None
                    parts.append(str(subval))

                elif isinstance(val, ast.Constant):
                    parts.append(str(val.value))

            return "".join(parts)
        return None

    @staticmethod
    def _eval_subscript(node, variables):
        """
        Evaluates subscript/index operations (e.g., x[0], d['key'], lst[:2]).

        Args:
            node (ast.Subscript): Subscript node.
            variables (dict): Variable context.

        Returns:
            Any or None: Result of indexing or slicing.
        """
        if isinstance(node, ast.Subscript):
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

            if attr == "get" and isinstance(base, dict):

                if len(node.args) >= 1:
                    key = SafeEvaluator.evaluate(node.args[0], variables)
                    default = (
                        SafeEvaluator.evaluate(node.args[1], variables)
                        if len(node.args) > 1
                        else None
                    )

                    return base.get(key, default)

            elif attr == "format" and isinstance(base, str):

                try:
                    args = [SafeEvaluator.evaluate(arg, variables) for arg in node.args]
                    if None not in args:
                        return base.format(*args)

                except Exception as e:
                    logger.warning(f"format() evaluation failed: {e}")

            elif attr == "join" and isinstance(base, str):
                if node.args:
                    iterable = SafeEvaluator.evaluate(node.args[0], variables)

                    if isinstance(iterable, list) and all(
                        isinstance(x, str) for x in iterable
                    ):
                        return base.join(iterable)
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
