import ast
from spark_ast_lineage.analyzer.pyspark_tables_extractor import PysparkTablesExtractor


def unwrap(d):
    """Helper to convert single-item sets to scalar values for easy assertion comparison."""
    return {
        k: next(iter(v)) if isinstance(v, set) and len(v) == 1 else v
        for k, v in d.items()
    }


def test_simple_assignment():
    code = 'a = "hello"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert unwrap(variables) == {"a": "hello"}


def test_multiple_assignments():
    code = 'a = 10\nb = 20\nc = "test"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert unwrap(variables) == {"a": "10", "b": "20", "c": "test"}


def test_tuple_unpacking():
    code = 'a, b = 5, "world"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert unwrap(variables) == {"a": "5", "b": "world"}


def test_list_unpacking():
    code = '[a, b] = [42, "data"]'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert unwrap(variables) == {"a": "42", "b": "data"}


def test_dictionary_assignment():
    code = 'a = {"key": "value"}'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    expected = "{'key': 'value'}"
    assert unwrap(variables) == {"a": expected}


def test_function_call_assignment():
    code = "a = some_function()"
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert "a" in variables  # Function calls won't be statically resolved


def test_attribute_assignment():
    code = 'self.a = "class_var"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert unwrap(variables) == {"self.a": "class_var"}


def test_nested_assignment():
    code = "a = b = c = 99"
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert unwrap(variables) == {"a": "99", "b": "99", "c": "99"}


def test_ignore_expressions():
    code = 'print("Hello World")'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {}


def test_string_concatenation():
    code = """
prefix = "sales"
year = "2024"
table_name = prefix + "_" + year
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert unwrap(variables) == {
        "prefix": "sales",
        "year": "2024",
        "table_name": "sales_2024",
    }


def test_string_concatenation_3_times():
    code = """
source_schema = "silver"
prefix = "sales"
year = "2024"
table_name = source_schema + "." + prefix + "_" + year
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert unwrap(variables) == {
        "source_schema": "silver",
        "prefix": "sales",
        "year": "2024",
        "table_name": "silver.sales_2024",
    }


def test_string_format_method():
    code = """
prefix = "sales"
year = "2024"
table_name = "{}_{}".format(prefix, year)
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert unwrap(variables) == {
        "prefix": "sales",
        "year": "2024",
        "table_name": "sales_2024",
    }


def test_f_string():
    code = """
prefix = "sales"
year = "2024"
table_name = f"{prefix}_{year}"
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert unwrap(variables) == {
        "prefix": "sales",
        "year": "2024",
        "table_name": "sales_2024",
    }


def test_string_join_method():
    code = """
parts = ["sales", "2024"]
table_name = "_".join(parts)
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert unwrap(variables) == {
        "parts": "['sales', '2024']",
        "table_name": "sales_2024",
    }


def test_string_slicing():
    code = """
full_table_name = "sales_2024_backup"
table_name = full_table_name[:10]
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert unwrap(variables) == {
        "full_table_name": "sales_2024_backup",
        "table_name": "sales_2024",
    }


def test_string_multiplication():
    code = """
table_prefix = "sale" * 2
table_name = table_prefix + "_2024"
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert unwrap(variables) == {
        "table_prefix": "salesale",
        "table_name": "salesale_2024",
    }
