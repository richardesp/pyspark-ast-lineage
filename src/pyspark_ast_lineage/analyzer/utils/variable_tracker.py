import ast
import textwrap
import logging

from pyspark_ast_lineage.analyzer.utils.safe_evaluator import SafeEvaluator

logger = logging.getLogger(__name__)


def extract_variables(tree: ast.AST, code: str, variables=None) -> dict[str, set]:
    """
    Extracts all *possible* variable assignments (including across branches)
    and evaluates their values using AST.

    Args:
        tree (ast.AST): Parsed AST tree.
        code (str): Source code.

    Returns:
        dict[str, set]: Variable names mapped to a set of possible values.
    """

    # We need to save the context when recursively we need to process the rest of the node.body
    variables = {} if not variables else variables
    code = textwrap.dedent(str(code))

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
            value_set = {str(SafeEvaluator.evaluate(replaced_expr, variables))}
            logger.debug(f"Retrieved value_set: {value_set}")

            # If assigning an instance like cfg = Config()
            if isinstance(replaced_expr, ast.Call) and isinstance(
                replaced_expr.func, ast.Name
            ):
                if isinstance(node.targets[0], ast.Name):
                    instance_name = node.targets[0].id
                    class_name = replaced_expr.func.id

                    # Step 1: Handle constructor arguments
                    tree_body = tree.body if isinstance(tree, ast.Module) else []
                    for class_node in tree_body:
                        if (
                            isinstance(class_node, ast.ClassDef)
                            and class_node.name == class_name
                        ):
                            for item in class_node.body:
                                if (
                                    isinstance(item, ast.FunctionDef)
                                    and item.name == "__init__"
                                ):
                                    param_names = [arg.arg for arg in item.args.args][
                                        1:
                                    ]  # skip 'self'
                                    arg_map = {}

                                    # Handle positional arguments
                                    for name, arg in zip(
                                        param_names, replaced_expr.args
                                    ):
                                        value = SafeEvaluator.evaluate(arg, variables)
                                        arg_map[name] = value

                                    # Handle keyword arguments
                                    for kw in replaced_expr.keywords:
                                        if kw.arg is not None:  # skip **kwargs
                                            value = SafeEvaluator.evaluate(
                                                kw.value, variables
                                            )
                                            arg_map[kw.arg] = value

                                    for stmt in item.body:
                                        if isinstance(stmt, ast.Assign):
                                            for target in stmt.targets:
                                                if isinstance(target, ast.Attribute):
                                                    full_attr = get_attribute_name(
                                                        target
                                                    )
                                                    if (
                                                        full_attr.startswith("self.")
                                                        and isinstance(
                                                            stmt.value, ast.Name
                                                        )
                                                        and stmt.value.id in arg_map
                                                    ):
                                                        instance_attr = f"{instance_name}.{full_attr[5:]}"
                                                        value = arg_map[stmt.value.id]
                                                        assign_variable(
                                                            full_attr, {value}
                                                        )
                                                        assign_variable(
                                                            instance_attr, {value}
                                                        )

                    # Step 2: Copy remaining self.* attributes to instance
                    self_attrs = {
                        k: list(v)[0]
                        for k, v in variables.items()
                        if k.startswith("self.") and len(v) == 1
                    }

                    for self_attr, val in self_attrs.items():
                        attr = self_attr.split("self.", 1)[1]
                        instance_attr = f"{instance_name}.{attr}"
                        assign_variable(instance_attr, {val})

                    # Step 3: Optionally store instance as dict
                    instance_dict_repr = (
                        "{"
                        + ", ".join(f"{k}: {v}" for k, v in self_attrs.items())
                        + "}"
                    )
                    assign_variable(instance_name, {instance_dict_repr})

            # Cleaning pre-processed sets value
            try:
                for value in value_set:
                    if type(ast.literal_eval(value)) is set:
                        for sub_value in ast.literal_eval(value):
                            value_set.add(sub_value)

                        value_set.discard(value)

                    elif ast.literal_eval(value) is None:
                        value_set.discard(value)
                        value_set.add(None)

            except Exception:
                pass

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
                name = get_attribute_name(target)
                if name:
                    assign_variable(name, value_set)

    for node in tree.body:
        if isinstance(node, ast.Assign):
            process_assign(node)

        elif isinstance(node, ast.If):
            body_tree = ast.Module(body=node.body, type_ignores=[])
            orelse_tree = ast.Module(body=node.orelse, type_ignores=[])

            body_vars = extract_variables(body_tree, code)
            orelse_vars = extract_variables(orelse_tree, code)

            all_keys = set(body_vars) | set(orelse_vars)
            for key in all_keys:
                assign_variable(
                    key, body_vars.get(key, set()) | orelse_vars.get(key, set())
                )

        elif isinstance(node, ast.For):
            iter_node = node.iter

            def extract_target_names(target):
                if isinstance(target, ast.Name):
                    return [target.id]
                elif isinstance(target, ast.Tuple):
                    return [elt.id for elt in target.elts if isinstance(elt, ast.Name)]
                return []

            target_variables = extract_target_names(node.target)
            logger.debug(f"Inside For loop (target variables: {target_variables})")

            values = set()

            if isinstance(iter_node, ast.List):
                # Direct list: for x in [(1, "a"), (2, "b")]
                try:
                    evaluated = ast.literal_eval(iter_node)
                    values = evaluated if isinstance(evaluated, list) else []
                except Exception as e:
                    logger.debug(f"Failed to literal_eval list: {e}")

            elif isinstance(iter_node, ast.Name):
                # Looping over a known variable
                known = variables.get(iter_node.id, set())
                try:
                    literal_value = ast.literal_eval(next(iter(known), "[]"))
                    values = literal_value if isinstance(literal_value, list) else []
                except Exception as e:
                    logger.debug(f"Failed to literal_eval variable {iter_node.id}: {e}")

            logger.debug(f"Resolved iterable values: {values}")

            # Assign values to target variables, one per item
            for item in values:
                if isinstance(item, (list, tuple)) and len(item) == len(
                    target_variables
                ):
                    for var_name, val in zip(target_variables, item):
                        assign_variable(var_name, str(val))
                elif len(target_variables) == 1:
                    assign_variable(target_variables[0], str(item))

            # Now extract variables from loop body
            body_tree = ast.Module(body=node.body, type_ignores=[])
            body_vars = extract_variables(body_tree, code, variables.copy())
            for key, new_values in body_vars.items():
                assign_variable(key, new_values)

        elif isinstance(node, ast.Try):
            logger.debug("Processing Try/Except block")

            # Extract from try body
            try_body_tree = ast.Module(body=node.body, type_ignores=[])
            try_vars = extract_variables(try_body_tree, code, variables.copy())

            # Extract from each except handler
            except_vars = {}
            for handler in node.handlers:
                handler_tree = ast.Module(body=handler.body, type_ignores=[])
                handler_vars = extract_variables(handler_tree, code, variables.copy())
                for key, value_set in handler_vars.items():
                    if key not in except_vars:
                        except_vars[key] = set()
                    except_vars[key].update(value_set)

            # Merge try and except variable sets
            all_keys = set(try_vars) | set(except_vars)
            for key in all_keys:
                combined_values = try_vars.get(key, set()) | except_vars.get(key, set())
                assign_variable(key, combined_values)

        elif isinstance(node, ast.With):
            logger.debug("Processing With block")

            body_tree = ast.Module(body=node.body, type_ignores=[])
            body_vars = extract_variables(body_tree, code, variables.copy())

            for key, value in body_vars.items():
                assign_variable(key, value)

        elif isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            logger.debug(
                f"Entering scope: {type(node).__name__} {getattr(node, 'name', '')}"
            )
            inner_tree = ast.Module(body=node.body, type_ignores=[])
            inner_vars = extract_variables(inner_tree, code, variables.copy())

            for key, val in inner_vars.items():
                assign_variable(key, val)

    return variables


def get_attribute_name(node):
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
            return f"{get_attribute_name(node.value)}.{node.attr}"
    return None


__all__ = ["extract_variables"]
