import ast
import logging

from spark_ast_lineage.analyzer.variable_tracker import extract_variables
from spark_ast_lineage.analyzer.extractor_factory import get_extractor

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
            extractor = get_extractor(node)
            if extractor:
                logger.debug(f"Current extractor: {extractor}")
                logger.debug(f"Variables to process: {variables}")

                tables.update(extractor.extract(node, variables))  # Pass variables

        return tables
