import ast


class TableExtractor:
    """Extractor for `spark.read.table("table_name")` calls"""

    def extract(self, node, variables=None):
        """
        Extracts table names from `spark.read.table("table_name")` calls.

        Args:
            node (ast.Call): The AST node representing a function call.
            variables (dict, optional): Dictionary of variable values extracted.

        Returns:
            set: A set of extracted table names.
        """
        if variables is None:
            variables = {}

        # Case 1: Direct string constant (spark.read.table("table_name"))
        if isinstance(node.args[0], ast.Constant):
            return {node.args[0].value}

        # Case 2: Variable assignment (spark.read.table(table_name))
        if isinstance(node.args[0], ast.Name):
            table_name = variables.get(
                node.args[0].id
            )  # Get value from extracted variables
            return {table_name} if table_name else set()

        # Case 3: String concatenation (spark.read.table(table_prefix + table_suffix))
        if isinstance(node.args[0], ast.BinOp) and isinstance(node.args[0].op, ast.Add):
            return {self._evaluate_binop(node.args[0], variables)}

        # Case 4: f-strings (spark.read.table(f"{table_base}_{table_suffix}"))
        if isinstance(node.args[0], ast.JoinedStr):
            return {self._evaluate_fstring(node.args[0], variables)}

        return set()

    def _evaluate_binop(self, node, variables):
        """Recursively evaluate string concatenations"""
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = self._evaluate_binop(node.left, variables)
            right = self._evaluate_binop(node.right, variables)
            return (left or "") + (right or "")

        if isinstance(node, ast.Name):  # Variable reference
            return variables.get(node.id, "")

        if isinstance(node, ast.Constant):  # Direct string
            return node.value

        return ""

    def _evaluate_fstring(self, node, variables):
        """Evaluate f-strings to extract table names"""
        result = []
        for part in node.values:
            if isinstance(part, ast.Constant):  # Regular string part
                result.append(part.value)
            elif isinstance(part, ast.FormattedValue) and isinstance(
                part.value, ast.Name
            ):
                var_name = part.value.id
                result.append(
                    str(variables.get(var_name, ""))
                )  # Convert variable value to string
        return "".join(result)
