import ast
import logging

from spark_ast_lineage.analyzer.extractors.registry import EXTRACTOR_REGISTRY
from spark_ast_lineage.analyzer.variable_tracker import extract_variables

logger = logging.getLogger(__name__)


class PysparkTablesExtractor:
    """Factory to return appropriate extractors for AST nodes"""

    @staticmethod
    def extract_tables_from_code(code: str):
        """
        Parses Python code and extracts table names using the correct extractor.

        Args:
            code (str): The Python code to analyze.

        Returns:
            set: A set of unique table names found in the code.
        """

        logger.debug("Extracting tables from code")

        tree = ast.parse(code)
        tables = set()

        # Step 1: Extract all variables first
        variables = extract_variables(tree, code)
        logger.debug(f"Variables extracted from source code: {variables}")

        # Step 2: Process each AST node
        for node in ast.walk(tree):
            extractor = PysparkTablesExtractor._get_extractor(node)
            if extractor:
                logger.debug(f"Current extractor: {extractor}")
                logger.debug(f"Variables to process: {variables}")

                tables.update(extractor.extract(node, variables))  # Pass variables

        return tables

    @staticmethod
    def _get_extractor(node):
        """
        Factory method that returns the appropriate extractor instance for a given AST node.

        This method inspects a function call in the AST (e.g., `spark.read.table("...")`
        or `df.write.saveAsTable("...")`) and looks up a matching extractor class
        from the global EXTRACTOR_REGISTRY based on the method name (e.g., "table", "sql", "saveAsTable").

        The registry is populated using the `@register_extractor` decorator to allow
        for modular and collaborative extension of supported PySpark methods.

        Args:
            node (ast.AST): An AST node representing a function call.

        Returns:
            Optional[Extractor]: An instance of the corresponding extractor class if matched,
            or `None` if the method is unsupported.
        """
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            extractor_cls = EXTRACTOR_REGISTRY.get(method_name)

            if extractor_cls:
                logger.debug(
                    f"Using {extractor_cls.__name__} for method: {method_name}"
                )
                return extractor_cls()

        return None

    @staticmethod
    def _evaluate_expression(expr, variables):
        try:
            if isinstance(expr, ast.Dict):  # Handle dictionary literals
                keys = [
                    PysparkTablesExtractor._evaluate_expression(k, variables)
                    for k in expr.keys
                ]
                values = [
                    PysparkTablesExtractor._evaluate_expression(v, variables)
                    for v in expr.values
                ]
                return dict(zip(keys, values))

            elif isinstance(expr, ast.List):  # Handle list literals
                return [
                    PysparkTablesExtractor._evaluate_expression(e, variables)
                    for e in expr.elts
                ]

            elif isinstance(expr, ast.Tuple):  # Handle tuple literals
                return tuple(
                    PysparkTablesExtractor._evaluate_expression(e, variables)
                    for e in expr.elts
                )

            elif isinstance(expr, ast.JoinedStr):  # Handle f-strings
                evaluated_parts = []
                for value in expr.values:
                    if isinstance(value, ast.FormattedValue):
                        if (
                            isinstance(value.value, ast.Name)
                            and value.value.id in variables
                        ):
                            evaluated_parts.append(str(variables[value.value.id]))
                        else:
                            return None
                    elif isinstance(value, ast.Constant):
                        evaluated_parts.append(value.value)
                    elif isinstance(value, ast.Str):
                        evaluated_parts.append(value.s)
                return "".join(evaluated_parts)

            elif isinstance(expr, ast.BinOp) and isinstance(
                expr.op, ast.Add
            ):  # Handle concatenation
                left = PysparkTablesExtractor._evaluate_expression(expr.left, variables)
                right = PysparkTablesExtractor._evaluate_expression(
                    expr.right, variables
                )
                if isinstance(left, str) and isinstance(right, str):
                    return left + right

            elif isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.Mod):
                left = PysparkTablesExtractor._evaluate_expression(expr.left, variables)
                right = PysparkTablesExtractor._evaluate_expression(
                    expr.right, variables
                )
                if isinstance(left, str) and isinstance(right, (tuple, list)):
                    return left % tuple(right)

            elif isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.Mult):
                left = PysparkTablesExtractor._evaluate_expression(expr.left, variables)
                right = PysparkTablesExtractor._evaluate_expression(
                    expr.right, variables
                )
                if isinstance(left, str) and isinstance(right, int):
                    return left * right
                if isinstance(right, str) and isinstance(left, int):
                    return right * left

            elif isinstance(expr, ast.Subscript):
                value = PysparkTablesExtractor._evaluate_expression(
                    expr.value, variables
                )
                if isinstance(expr.slice, ast.Constant):  # e.g. [0]
                    index = expr.slice.value
                elif isinstance(expr.slice, ast.Index):  # older Python versions
                    index = PysparkTablesExtractor._evaluate_expression(
                        expr.slice.value, variables
                    )
                elif isinstance(expr.slice, ast.Slice):
                    lower = PysparkTablesExtractor._evaluate_expression(
                        expr.slice.lower, variables
                    )
                    upper = PysparkTablesExtractor._evaluate_expression(
                        expr.slice.upper, variables
                    )
                    if isinstance(value, str):
                        return value[lower:upper]
                    return None
                else:
                    index = None
                if isinstance(value, (list, tuple, dict)) and index is not None:
                    return value[index]

            elif isinstance(expr, ast.Constant):
                return expr.value

            elif isinstance(expr, ast.Name) and expr.id in variables:
                return variables[expr.id]

            elif isinstance(expr, ast.Call) and isinstance(expr.func, ast.Attribute):
                method_name = expr.func.attr

                # Handle str.format()
                if (
                    method_name == "format"
                    and isinstance(expr.func.value, ast.Constant)
                    and isinstance(expr.func.value.value, str)
                ):
                    # Evaluate arguments
                    args = [
                        PysparkTablesExtractor._evaluate_expression(arg, variables)
                        for arg in expr.args
                    ]
                    if None not in args:  # Ensure all arguments are resolved
                        return expr.func.value.value.format(*args)

                # Handle str.join()
                if (
                    method_name == "join"
                    and isinstance(expr.func.value, ast.Constant)
                    and isinstance(expr.func.value.value, str)
                ):
                    iterable = PysparkTablesExtractor._evaluate_expression(
                        expr.args[0], variables
                    )
                    if isinstance(iterable, list):
                        return expr.func.value.value.join(map(str, iterable))

            else:
                return None

        except Exception as e:
            logger.warning(f"Failed to evaluate expression: {e}")
            return None
