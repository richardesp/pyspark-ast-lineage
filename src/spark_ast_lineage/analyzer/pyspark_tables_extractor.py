import ast
import logging

from spark_ast_lineage.analyzer.utils.variable_tracker import extract_variables
from spark_ast_lineage.analyzer.utils.extractor_factory import get_extractor

logger = logging.getLogger(__name__)


class PysparkTablesExtractor:
    """Factory to return appropriate extractors for AST nodes"""

    @staticmethod
    def extract_tables_from_code(code: str, verbose=False):
        """
        Parses Python code and extracts table names using the correct extractor.

        Args:
            code (str): The Python code to analyze.
            verbose (bool): If True, returns verbose details, including resolved code fragments.

        Returns:
            set: A set of unique table names found in the code.
            list: A list of dictionaries with verbose information (if verbose=True).
        """

        logger.debug("Extracting tables from code")

        tree = ast.parse(code)
        tables = set()
        verbose_info = []

        # Step 1: Extract all variables first
        variables = extract_variables(tree, code)
        logger.debug(f"Variables extracted from source code: {variables}")

        # Step 2: Process each AST node
        for node in ast.walk(tree):
            extractor = get_extractor(node)
            if extractor:
                logger.debug(f"Current extractor: {extractor}")
                logger.debug(f"Variables to process: {variables}")

                table_names = extractor.extract(node, variables)  # Extract table names
                tables.update(table_names)

                # If verbose mode is enabled, add detailed information about the extraction
                if verbose:
                    for table in table_names:
                        solved_code = PysparkTablesExtractor.resolve_variables_in_code(
                            code, variables, node
                        )
                        verbose_info.append(
                            {
                                "table": table,
                                "extracted_by": extractor.__class__.__name__,
                                "line_number": getattr(node, "lineno", "N/A"),
                                "operator_applied": extractor.__class__.__name__,
                                "processed_variables": variables,
                                "code_fragment": ast.unparse(node),
                                "solved_code": solved_code,
                            }
                        )

        # Return the tables as a set and the detailed information (if verbose)
        if verbose:
            return tables, verbose_info

        return tables

    @staticmethod
    def resolve_variables_in_code(code: str, variables: dict, node: ast.AST) -> str:
        """
        Replaces variable references in the code with their corresponding values
        to generate the resolved code.

        Args:
            code (str): The source code to process.
            variables (dict): A dictionary of variable names and their values.
            node (ast.AST): The AST node being processed (to map to its position in the code).

        Returns:
            str: The resolved code with variables replaced by their values.
        """
        for var_name, values in variables.items():
            # If values is None, skip it
            if values is None:
                continue

            # If there are multiple possible values, we only use the first one
            if isinstance(values, (list, set)):
                value = next(iter(values), None)  # Get the first value or None if empty
            else:
                value = values  # If it's not iterable, just use the value

            if value is not None:
                code = code.replace(var_name, str(value))
            else:
                # If value is None, you can choose to replace None with an empty string or leave it as is.
                code = code.replace(var_name, "")

        return code
