import ast
import logging
import textwrap

from spark_ast_lineage.analyzer.extractors import SQLExtractor, TableExtractor

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
        variables = PysparkTablesExtractor._extract_variables(tree, code)

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
        """Returns the appropriate extractor based on the AST node type"""

        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "sql":
                logger.debug("Using SQLExtractor for node Call")
                return SQLExtractor()

            elif node.func.attr == "table":
                logger.debug("Using TableExtractor for node Call")
                return TableExtractor()  # Updated to support variable extraction

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

            else:
                return None

        except Exception as e:
            logger.warning(f"Failed to evaluate expression: {e}")
            return None

    @staticmethod
    def _extract_variables(tree, code):
        """
        Extracts primitive variable assignments (strings, numbers, lists, dicts) from AST safely.

        Args:
            tree (ast.AST): The parsed AST tree.
            code (str): The original source code.

        Returns:
            dict: A dictionary of variable names and their evaluated values.
        """
        variables = {}
        code = textwrap.dedent(str(code))  # Normalize indentation

        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                logger.debug(
                    f"Processing assignment: {ast.get_source_segment(code, node)}"
                )

                value = None
                try:
                    value = PysparkTablesExtractor._evaluate_expression(
                        node.value, variables
                    )  # Evaluate expression

                except Exception as e:
                    logger.warning(f"Failed to evaluate: {e}")
                    value = None  # If unsafe, assign None

                # Handle variable unpacking (tuples, lists)
                for target in node.targets:
                    if isinstance(target, ast.Name):  # Single variable assignments
                        logger.debug(f"Assigning {target.id} = {value}")
                        variables[target.id] = value

                    elif isinstance(target, (ast.Tuple, ast.List)):  # Unpacking
                        if isinstance(value, (tuple, list)) and len(target.elts) == len(
                            value
                        ):
                            for var, val in zip(target.elts, value):
                                if isinstance(var, ast.Name):
                                    logger.debug(f"Unpacking: {var.id} = {val}")
                                    variables[var.id] = val

        logger.debug(f"Extracted variables: {variables}")
        return variables

    @staticmethod
    def _get_attribute_name(node):
        """
        Extracts attribute names from AST nodes, handling nested attributes.

        Args:
            node (ast.Attribute): The attribute node.

        Returns:
            str: The fully qualified attribute name.
        """
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name):  # Simple case: self.attr
                return f"{node.value.id}.{node.attr}"
            elif isinstance(node.value, ast.Attribute):  # Nested attributes: self.a.b
                return f"{PysparkTablesExtractor._get_attribute_name(node.value)}.{node.attr}"
        return None
