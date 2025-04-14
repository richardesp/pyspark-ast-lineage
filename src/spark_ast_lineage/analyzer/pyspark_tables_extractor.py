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

    @staticmethod
    def _extract_variables(tree, code):
        """
        Extracts all *possible* variable assignments (including across branches)
        and evaluates their values using AST.

        Args:
            tree (ast.AST): Parsed AST tree.
            code (str): Source code.

        Returns:
            dict[str, set]: Variable names mapped to a set of possible values.
        """
        variables = {}
        code = textwrap.dedent(str(code))

        def custom_literal_eval(expr_node, variables):
            try:
                logger.debug(f"Trying to apply ast.literal_eval to {expr_node}")
                return ast.literal_eval(expr_node)
            except Exception:
                logger.debug(f"Failed evaluating {expr_node}")
                pass  # fallback below

            logger.debug(f"expr_node type: {type(expr_node)}")
            logger.debug(f"Current variables lookup: {variables}")

            # Handle string concatenation and multiplication
            if isinstance(expr_node, ast.BinOp):
                left = custom_literal_eval(expr_node.left, variables)
                right = custom_literal_eval(expr_node.right, variables)

                if isinstance(expr_node.op, ast.Add):
                    logger.debug("Handling string concatenation with +")
                    if left is not None and right is not None:
                        return str(left) + str(right)

                elif isinstance(expr_node.op, ast.Mult):
                    logger.debug("Handling string multiplication with *")
                    if isinstance(left, str) and isinstance(right, int):
                        return left * right
                    elif isinstance(right, str) and isinstance(left, int):
                        return right * left

                elif isinstance(expr_node.op, ast.Mod):
                    logger.debug("Handling percent string formatting with % operator")
                    left = custom_literal_eval(expr_node.left, variables)
                    right = custom_literal_eval(expr_node.right, variables)
                    if isinstance(left, str) and isinstance(right, (tuple, list)):
                        try:
                            return left % tuple(right)
                        except Exception as e:
                            logger.warning(
                                f"Failed to format string using % operator: {e}"
                            )
                    elif isinstance(left, str):
                        try:
                            return left % right
                        except Exception as e:
                            logger.warning(
                                f"Failed to format string using % operator: {e}"
                            )

            # Handle f-strings
            if isinstance(expr_node, ast.JoinedStr):
                parts = []
                for val in expr_node.values:
                    if isinstance(val, ast.FormattedValue):
                        subval = custom_literal_eval(val.value, variables)
                        if subval is None:
                            return None
                        parts.append(str(subval))
                    elif isinstance(val, ast.Constant):
                        parts.append(str(val.value))
                return "".join(parts)

            # Handle slicing
            if isinstance(expr_node, ast.Subscript) and isinstance(
                expr_node.slice, ast.Slice
            ):
                value = custom_literal_eval(expr_node.value, variables)
                if isinstance(value, str):
                    lower = (
                        custom_literal_eval(expr_node.slice.lower, variables)
                        if expr_node.slice.lower
                        else None
                    )
                    upper = (
                        custom_literal_eval(expr_node.slice.upper, variables)
                        if expr_node.slice.upper
                        else None
                    )
                    return value[lower:upper]

            # Handle function calls (format, join)
            if isinstance(expr_node, ast.Call) and isinstance(
                expr_node.func, ast.Attribute
            ):
                attr = expr_node.func.attr

                if attr == "format":
                    logger.debug("Invoke for format() method detected")
                    logger.debug(
                        f"format() called on: {ast.unparse(expr_node.func.value)}, "
                        f"with the following arguments: {[ast.unparse(arg) for arg in expr_node.args]}"
                    )
                    try:
                        fmt = ast.literal_eval(expr_node.func.value)
                        if not isinstance(fmt, str):
                            logger.warning("format() base is not a string")
                            return None

                        args = [ast.literal_eval(arg) for arg in expr_node.args]
                        args = [str(a) for a in args]

                        return str.format(fmt, *args)

                    except Exception as e:
                        logger.warning(f"format() evaluation failed: {e}")
                        return None

                elif attr == "join":
                    logger.debug("Invoke for join() method detected")
                    logger.debug(
                        f"join() called on: {ast.unparse(expr_node.func.value)}, "
                        f"with the following argument: {ast.unparse(expr_node.args[0])}"
                    )
                    try:
                        separator = ast.literal_eval(expr_node.func.value)
                        if not isinstance(separator, str):
                            logger.warning("join() separator is not a string")
                            return None

                        iterable_objects = ast.literal_eval(expr_node.args[0])
                        if not isinstance(iterable_objects, (list, tuple)):
                            logger.warning("join() argument is not a list/tuple")
                            return None

                        if not all(isinstance(item, str) for item in iterable_objects):
                            logger.warning("join() items must all be strings")
                            return None

                        return str.join(separator, iterable_objects)

                    except Exception as e:
                        logger.warning(f"join() evaluation failed: {e}")
                        return None

            return None  # fallback: not resolvable

        def replace_names_with_constants(expr_node, variables):
            class NameReplacer(ast.NodeTransformer):
                def visit_Name(self, node):
                    if node.id in variables:
                        values = list(variables[node.id])
                        if len(values) == 1:
                            try:
                                # Try to literal_eval for numbers, dicts, etc.
                                value = ast.literal_eval(values[0])
                            except Exception:
                                value = values[0]
                            return ast.copy_location(ast.Constant(value=value), node)
                    return node

            return NameReplacer().visit(expr_node)

        def assign_variable(var_name, value):
            if var_name not in variables:
                variables[var_name] = set()
            if isinstance(value, set):
                variables[var_name].update(value)
            else:
                variables[var_name].add(value)

        def process_assign(node):
            try:
                # Replace variable names with literal values
                replaced_expr = replace_names_with_constants(node.value, variables)
                source_expr = ast.unparse(replaced_expr)  # this must be a string
                logger.debug(f"Evaluating expression string: {source_expr}")

                # Only now evaluate the string safely
                value_set = {str(custom_literal_eval(replaced_expr, variables))}

            except Exception as e:
                logger.warning(f"Failed to evaluate {ast.unparse(node)}: {e}")
                value_set = {str(None)}

            logger.debug(f"Processed assignment: {ast.unparse(node)} -> {value_set}")

            for target in node.targets:
                if isinstance(target, ast.Name):
                    assign_variable(target.id, value_set)

                elif isinstance(target, (ast.Tuple, ast.List)):
                    raw = next(iter(value_set))
                    try:
                        unpacked = ast.literal_eval(raw)
                        if isinstance(unpacked, (list, tuple)) and len(unpacked) == len(
                            target.elts
                        ):
                            for var_node, val in zip(target.elts, unpacked):
                                if isinstance(var_node, ast.Name):
                                    assign_variable(var_node.id, str(val))
                        else:
                            logger.warning(
                                f"Cannot unpack: {raw} into {len(target.elts)} vars"
                            )
                    except Exception as e:
                        logger.warning(f"Failed to unpack tuple/list: {raw} — {e}")

                elif isinstance(target, ast.Attribute):
                    name = PysparkTablesExtractor._get_attribute_name(target)
                    if name:
                        assign_variable(name, value_set)

        for node in tree.body:
            if isinstance(node, ast.Assign):
                process_assign(node)
            elif isinstance(node, ast.If):
                body_tree = ast.Module(body=node.body, type_ignores=[])
                orelse_tree = ast.Module(body=node.orelse, type_ignores=[])

                body_vars = PysparkTablesExtractor._extract_variables(body_tree, code)
                orelse_vars = PysparkTablesExtractor._extract_variables(
                    orelse_tree, code
                )

                all_keys = set(body_vars) | set(orelse_vars)
                for key in all_keys:
                    assign_variable(
                        key, body_vars.get(key, set()) | orelse_vars.get(key, set())
                    )

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
