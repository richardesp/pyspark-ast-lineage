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
