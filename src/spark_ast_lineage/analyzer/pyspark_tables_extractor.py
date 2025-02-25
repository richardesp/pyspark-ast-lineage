import ast
import logging

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
        variables = PysparkTablesExtractor._extract_variables(tree)

        # Step 2: Process each AST node
        for node in ast.walk(tree):
            extractor = PysparkTablesExtractor._get_extractor(node)
            if extractor:
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
    def _extract_variables(tree):
        """
        Extracts variable assignments in the code, including expressions.

        Args:
            tree (ast.AST): The parsed AST tree.

        Returns:
            dict: A dictionary of variable names and their evaluated values.
        """
        variables = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
                var_name = node.targets[0].id  # Variable name
                value = PysparkTablesExtractor._evaluate_expression(
                    node.value, variables
                )
                if value:
                    variables[var_name] = value

        logger.debug(f"Extracted variables: {variables}")

        return variables

    @staticmethod
    def _evaluate_expression(node, variables):
        """
        Evaluates expressions such as:
        - Simple string assignments
        - String concatenation (`table_prefix + table_suffix`)
        - f-strings (`f"{table_base}_{table_suffix}"`)
        - .format() method
        - %-formatting
        """
        if isinstance(node, ast.Constant):  # Direct string
            return node.value

        if isinstance(node, ast.Name):  # Variable reference
            return variables.get(node.id, "")

        if isinstance(node, ast.BinOp) and isinstance(
            node.op, ast.Add
        ):  # String concatenation
            left = PysparkTablesExtractor._evaluate_expression(node.left, variables)
            right = PysparkTablesExtractor._evaluate_expression(node.right, variables)
            return (left or "") + (right or "")

        if isinstance(node, ast.JoinedStr):  # f-string evaluation
            result = []
            for part in node.values:
                if isinstance(part, ast.Constant):
                    result.append(part.value)
                elif isinstance(part, ast.FormattedValue) and isinstance(
                    part.value, ast.Name
                ):
                    var_name = part.value.id
                    result.append(
                        str(variables.get(var_name, ""))
                    )  # Convert variable to string
            return "".join(result)

        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "format":  # .format() method
                base_string = PysparkTablesExtractor._evaluate_expression(
                    node.func.value, variables
                )
                formatted_args = [
                    PysparkTablesExtractor._evaluate_expression(arg, variables)
                    for arg in node.args
                ]
                if base_string:
                    return base_string.format(*formatted_args)

        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):  # % formatting
            base_string = PysparkTablesExtractor._evaluate_expression(
                node.left, variables
            )
            formatted_args = PysparkTablesExtractor._evaluate_expression(
                node.right, variables
            )

            if base_string:
                try:
                    if isinstance(formatted_args, tuple):  # Handle tuple arguments
                        return base_string % tuple(formatted_args)
                    elif isinstance(formatted_args, list):  # Handle list arguments
                        return base_string % tuple(formatted_args)
                    else:
                        return base_string % formatted_args  # Single value substitution
                except TypeError:
                    return None  # Return None if formatting fails

        if isinstance(node, ast.Tuple):  # Handle tuple unpacking in %
            return tuple(
                PysparkTablesExtractor._evaluate_expression(elt, variables)
                for elt in node.elts
            )

        return None
