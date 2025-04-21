import ast
import logging
from typing import Optional

from pyspark_ast_lineage.analyzer.utils.variable_tracker import extract_variables
from pyspark_ast_lineage.analyzer.utils.extractor_factory import get_extractor

logger = logging.getLogger(__name__)


class PysparkTablesExtractor:
    """Factory to return appropriate extractors for AST nodes"""

    @staticmethod
    def extract_tables_from_code(
        code: str, verbose: bool = False
    ) -> tuple[dict[set], Optional[list[dict]]]:
        """
        Parses Python code and extracts table names using the correct extractor.

        Args:
            code (str): The Python code to analyze.
            verbose (bool): If True, returns verbose details, including code fragments.

        Returns:
            dict: A set of possible table names found in the code based on the logic.
            list: A list of dictionaries with verbose information (if verbose=True).
        """

        logger.debug("Extracting tables from code")

        tree = ast.parse(code)
        tables = set()
        verbose_info = []

        variables = extract_variables(tree, code)
        logger.debug(f"Variables extracted from source code: {variables}")

        for node in ast.walk(tree):
            extractor = get_extractor(node)
            if extractor:
                logger.debug(f"Current extractor: {extractor}")
                logger.debug(f"Variables to process: {variables}")

                table_names = extractor.extract(node, variables)
                tables.update(table_names)

                if verbose:
                    for table in table_names:

                        code_fragment = PysparkTablesExtractor.get_code_fragment(
                            code, node.lineno
                        )

                        relevant_variables = (
                            PysparkTablesExtractor.get_relevant_variables(
                                variables, table
                            )
                        )

                        verbose_info.append(
                            {
                                "table": table,
                                "extracted_by": extractor.__class__.__name__,
                                "line_number": getattr(node, "lineno", "N/A"),
                                "operator_applied": extractor.__class__.__name__,
                                "processed_variables": relevant_variables,
                                "code_fragment": code_fragment,
                            }
                        )

        if verbose:
            return tables, verbose_info

        return tables

    @DeprecationWarning
    @staticmethod
    def clean_multiple_values(variables: dict[set]) -> dict[set]:
        """
        Processes the dictionary of variables to handle values that are in the string set format
        and unfold them into actual sets, merging the values into the respective variable.

        Args:
            variables (dict): A dictionary where the values could be sets, lists, or string representations
                            of sets.

        Returns:
            dict: The updated dictionary with unfolded sets.
        """
        cleaned_variables = {}

        for var_name, values in variables.items():
            if isinstance(values, set):
                cleaned_variables[var_name] = values

            elif isinstance(values, str):
                try:
                    possible_set = ast.literal_eval(values)
                    if isinstance(possible_set, set):
                        cleaned_variables[var_name] = values

                    else:
                        cleaned_variables[var_name] = {values}

                except (ValueError, SyntaxError):
                    cleaned_variables[var_name] = {values}

            else:
                cleaned_variables[var_name] = {values}

        return cleaned_variables

    @DeprecationWarning
    @staticmethod
    def resolve_variables_in_code(code: str, variables: dict) -> str:
        """
        Replaces variable references in the code with their corresponding values
        to generate the resolved code.

        Args:
            code (str): The source code to process.
            variables (dict): A dictionary of variable names and their values.

        Returns:
            str: The resolved code with variables replaced by their values.
        """
        for var_name, values in variables.items():
            if values is None:
                continue

            if isinstance(values, (list, set)):
                value = next(iter(values), None)
            else:
                value = values

            if value is not None:
                code = code.replace(var_name, str(value))
            else:
                code = code.replace(var_name, "")

        return code

    @staticmethod
    def get_code_fragment(code: str, line_number: int) -> str:
        """
        Retrieves the line of code at `line_number`, as well as the line above and below it.

        Args:
            code (str): The full code as a string.
            line_number (int): The line number where the AST node is located.

        Returns:
            str: A string with the surrounding lines (above, current, and below).
        """
        lines = code.splitlines()

        # Ensure line number is within range
        start = max(line_number - 2, 0)
        next_line = min(line_number, len(lines) - 1) + 1

        fragment = "\n".join(lines[start:next_line])
        return fragment

    @staticmethod
    def get_relevant_variables(variables: dict, table: str) -> dict:
        """
        Filters the relevant variables that are directly related to the given table.

        Args:
            variables (dict): The dictionary of variables.
            table (str): The table name being processed.

        Returns:
            dict: A dictionary of relevant variables for the table.
        """
        relevant_variables = {}

        for var_name, values in variables.items():
            # Include variables whose values are used to resolve the table
            if table in str(values):
                relevant_variables[var_name] = values

        return relevant_variables
