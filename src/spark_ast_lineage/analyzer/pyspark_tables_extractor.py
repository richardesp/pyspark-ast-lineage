import ast
from spark_ast_lineage.analyzer.extractors import SQLExtractor, TableExtractor


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
        tree = ast.parse(code)
        tables = set()

        for node in ast.walk(tree):
            extractor = PysparkTablesExtractor._get_extractor(node)
            if extractor:
                tables.update(extractor.extract(node))

        return tables

    @staticmethod
    def _get_extractor(node):
        """Returns the appropriate extractor based on the AST node type"""
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "sql":
                return SQLExtractor()
            elif node.func.attr == "table":
                return TableExtractor()
        return None
