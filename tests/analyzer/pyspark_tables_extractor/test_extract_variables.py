import ast
from spark_ast_lineage.analyzer.pyspark_tables_extractor import PysparkTablesExtractor


def test_simple_assignment():
    code = 'a = "hello"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"a": {"hello"}}


def test_multiple_assignments():
    code = 'a = 10\nb = 20\nc = "test"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"a": {"10"}, "b": {"20"}, "c": {"test"}}


def test_tuple_unpacking():
    code = 'a, b = 5, "world"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"a": {"5"}, "b": {"world"}}


def test_list_unpacking():
    code = '[a, b] = [42, "data"]'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"a": {"42"}, "b": {"data"}}


def test_dictionary_assignment():
    code = 'a = {"key": "value"}'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    expected = "{'key': 'value'}"
    assert variables == {"a": {expected}}


def test_function_call_assignment():
    code = "a = some_function()"
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert "a" in variables  # Function calls won't be statically resolved


def test_attribute_assignment():
    code = 'self.a = "class_var"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"self.a": {"class_var"}}


def test_nested_assignment():
    code = "a = b = c = 99"
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"a": {"99"}, "b": {"99"}, "c": {"99"}}


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
    assert variables == {
        "prefix": {"sales"},
        "year": {"2024"},
        "table_name": {"sales_2024"},
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
    assert variables == {
        "source_schema": {"silver"},
        "prefix": {"sales"},
        "year": {"2024"},
        "table_name": {"silver.sales_2024"},
    }


def test_string_format_method():
    code = """
prefix = "sales"
year = "2024"
table_name = "{}_{}".format(prefix, year)
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {
        "prefix": {"sales"},
        "year": {"2024"},
        "table_name": {"sales_2024"},
    }


def test_f_string():
    code = """
prefix = "sales"
year = "2024"
table_name = f"{prefix}_{year}"
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {
        "prefix": {"sales"},
        "year": {"2024"},
        "table_name": {"sales_2024"},
    }


def test_string_join_method():
    code = """
parts = ["sales", "2024"]
table_name = "_".join(parts)
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {
        "parts": {"['sales', '2024']"},
        "table_name": {"sales_2024"},
    }


def test_string_slicing():
    code = """
full_table_name = "sales_2024_backup"
table_name = full_table_name[:10]
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {
        "full_table_name": {"sales_2024_backup"},
        "table_name": {"sales_2024"},
    }


def test_string_multiplication():
    code = """
table_prefix = "sale" * 2
table_name = table_prefix + "_2024"
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {
        "table_prefix": {"salesale"},
        "table_name": {"salesale_2024"},
    }


def test_percent_formatting():
    code = """
table_base = "departments"
month = "jan"
table_name = "%s_%s" % (table_base, month)
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {
        "table_base": {"departments"},
        "month": {"jan"},
        "table_name": {"departments_jan"},
    }


def test_dictionary_access():
    code = """
tables = {"primary": "orders", "backup": "orders_backup"}
table_name = tables["primary"]
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)

    assert variables == {
        "tables": {"{'primary': 'orders', 'backup': 'orders_backup'}"},
        "table_name": {"orders"},
    }


def test_nested_dictionary_access():
    code = """
tables = {"sales": {"q1": "sales_q1", "q2": "sales_q2"}}
table_name = tables["sales"]["q1"]
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {
        "tables": {"{'sales': {'q1': 'sales_q1', 'q2': 'sales_q2'}}"},
        "table_name": {"sales_q1"},
    }


def test_df_write_csv_nested_dict():
    code = """
paths = {"2024": {"csv": "/exports/2024/csv"}}
table = paths["2024"]["csv"]
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables["table"] == {"/exports/2024/csv"}


def test_list_indexing():
    code = """
table_names = ["customers", "orders"]
table_name = table_names[0]
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {
        "table_names": {"['customers', 'orders']"},
        "table_name": {"customers"},
    }


def test_tuple_indexing():
    code = """
tables = ("users", "orders")
table_name = tables[1]
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {
        "tables": {"('users', 'orders')"},
        "table_name": {"orders"},
    }


def test_for_loop_assignment():
    code = """
table_names = ["sales_q1", "sales_q2"]
for table in table_names:
    df = spark.read.table(table)
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)

    assert variables == {
        "table_names": {"['sales_q1', 'sales_q2']"},
        "table": {"sales_q1", "sales_q2"},
        "df": {None},
    }


def test_for_loop_tuple_unpacking():
    code = 'pairs = [(1, "a"), (2, "b")]\nfor x, y in pairs:\n    val = y'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {
        "pairs": {"[(1, 'a'), (2, 'b')]"},
        "x": {"1", "2"},
        "y": {"a", "b"},
        "val": {"a", "b"},
    }


def test_ternary_expression():
    code = 'env = "prod"\ntable = "prod_table" if env == "prod" else "dev_table"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {
        "env": {"prod"},
        "table": {"prod_table", "dev_table"},
    }


def test_try_except_assignment():
    code = """
try:
    table = "main"
except:
    table = "fallback"
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"table": {"main", "fallback"}}


def test_dict_comprehension_assignment():
    code = "a = {str(i): i for i in range(2)}"
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    # Can't resolve actual values without evaluating code
    assert "a" in variables


def test_function_scope_isolated():
    code = """
def my_func():
    x = "inside"
x = "outside"
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"x": {"inside", "outside"}}


def test_for_loop_over_range():
    code = """
for i in range(2):
    val = "table_" + str(i)
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert "val" in variables
    # Optionally: {"table_0", "table_1"} if you support limited constant range unrolling


def test_if_else_assignments():
    code = """
if True:
    table = "a"
else:
    table = "b"
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"table": {"a", "b"}}


def test_list_comprehension_variable_leak():
    code = '[x for x in range(3)]\nfinal = "done"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert "x" not in variables
    assert variables == {"final": {"done"}}


def test_dynamic_assignment_should_ignore():
    code = 'setattr(obj, "table", "dynamic")'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {}


def test_nested_if_blocks():
    code = """
if True:
    if False:
        table = "a"
    else:
        table = "b"
else:
    table = "c"
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"table": {"a", "b", "c"}}


def test_with_statement_assignment():
    code = """
with open("some_file") as f:
    table = "inside"
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"table": {"inside"}}


def test_function_scope_is_ignored():
    code = """
def my_func():
    table = "inside"
table = "outside"
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"table": {"inside", "outside"}}


def test_ternary_in_loop():
    code = """
flags = [True, False]
for flag in flags:
    table = "main" if flag else "fallback"
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables["table"] == {"main", "fallback"}


def test_dict_access_with_indirection():
    code = """
paths = {"main": "s3://bucket/main", "backup": "s3://bucket/backup"}
key = "main"
path = paths[key]
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables["path"] == {"s3://bucket/main"}


def test_list_comprehension_result():
    code = """
tables = ["a", "b", "c"]
prefixed = ["tbl_" + t for t in tables]
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert "prefixed" in variables  # Even if we can't resolve, it should be safe


def test_dict_get_method():
    code = """
tables = {"a": "t1", "b": "t2"}
table = tables.get("a", "default")
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables["table"] == {"t1"}


def test_deep_loop_unpacking():
    code = """
rows = [[("a", 1), ("b", 2)]]
for group in rows:
    for name, id in group:
        label = name
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables["label"] == {"a", "b"}


def test_method_call_assignment():
    code = """
def get_table():
    return "dynamic"

table = get_table()
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert "table" in variables  # Cannot resolve, but shouldn't error


def test_undeclared_variable_resolves_to_none():
    code = """
table_name = some_undefined_var
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"table_name": {None}}


def test_explicit_none_assignment():
    code = """
table_name = None
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"table_name": {None}}


def test_partial_resolution_with_undeclared():
    code = """
prefix = "sales"
table_name = prefix + "_" + unknown
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables["table_name"] == {None}


def test_self_attribute_assignment():
    code = """
class Manager:
    def __init__(self):
        self.table_name = "sales"

    def read(self):
        table = self.table_name
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"self.table_name": {"sales"}, "table": {"sales"}}


def test_function_local_variable_usage():
    code = """
def get_table():
    table = "sales"
    return table
"""
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"table": {"sales"}}


def test_chained_string_operations():
    code = """
prefix = "data"
suffix = "2025"
table = prefix.upper() + "_" + suffix.lower()
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables["table"] == {"DATA_2025"}


def test_nested_attribute_resolution():
    code = """
class Config:
    def __init__(self):
        self.env = "prod"

cfg = Config()
environment = cfg.env
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert "cfg.env" in variables
    assert variables["environment"] == {"prod"}


def test_nested_attribute_resolution_with_constructor_initilization_args():
    code = """
class Config:
    def __init__(self, environment):
        self.env = environment

cfg = Config("prod")
environment = cfg.env
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert "cfg.env" in variables
    assert variables["environment"] == {"prod"}


def test_nested_attribute_resolution_with_constructor_initilization_kwargs():
    code = """
class Config:
    def __init__(self, environment):
        self.env = environment

cfg = Config(environment="prod")
environment = cfg.env
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert "cfg.env" in variables
    assert variables["environment"] == {"prod"}


def test_ternary_with_function_call():
    code = """
def get_env():
    return "dev"

env = get_env()
table = "dev_table" if env == "dev" else "prod_table"
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert "env" in variables
    assert variables["table"] == {"dev_table", "prod_table"}


def test_complex_f_string():
    code = """
prefix = "sales"
region = "us"
year = 2025
table = f"{prefix}_{region}_{year}"
    """
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables["table"] == {"sales_us_2025"}
