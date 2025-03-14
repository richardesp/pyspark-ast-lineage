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
        """
        Evaluates an AST expression safely.

        Args:
            expr (ast.expr): The right-hand side of an assignment.
            variables (dict): The current variables for lookup.

        Returns:
            The evaluated value or None if it cannot be evaluated.
        """
        try:
            # Convert AST expression back to source
            expr_code = ast.unparse(expr)  # Python 3.9+

            logger.debug(f"Evaluating expression: {expr_code}")

            # Evaluate expression safely with access only to known variables
            result = eval(expr_code, {"__builtins__": {}}, variables)

            logger.debug(f"Evaluated expression result: {result}")
            return result
        except Exception as e:
            logger.warning(f"Failed to evaluate expression: {e}")
            return None  # Return None if evaluation fails

    @staticmethod
    def _extract_variables(tree, code):
        """
        Extracts variable assignments in the code, including expressions.

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
                    f"Processing assignment node: {ast.get_source_segment(code, node)}"
                )

                # Evaluate the right-hand side expression
                value = PysparkTablesExtractor._evaluate_expression(
                    node.value, variables
                )

                # Process assignment targets
                for target in node.targets:
                    if isinstance(target, ast.Name):  # Standard variable assignment
                        logger.debug(f"Assigning {target.id} = {value}")
                        variables[target.id] = value

                    elif isinstance(
                        target, (ast.Tuple, ast.List)
                    ):  # Unpacking assignment
                        if isinstance(value, (tuple, list)) and len(target.elts) == len(
                            value
                        ):
                            for var, val in zip(target.elts, value):
                                if isinstance(var, ast.Name):
                                    logger.debug(f"Unpacking: {var.id} = {val}")
                                    variables[var.id] = val

                    elif isinstance(
                        target, ast.Attribute
                    ):  # Object attribute (self.var = value)
                        attr_name = PysparkTablesExtractor._get_attribute_name(target)
                        if attr_name:
                            logger.debug(f"Setting attribute {attr_name} = {value}")
                            variables[attr_name] = value

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
