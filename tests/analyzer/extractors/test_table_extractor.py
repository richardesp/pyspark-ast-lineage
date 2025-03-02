from spark_ast_lineage.analyzer.pyspark_tables_extractor import (
    PysparkTablesExtractor,
)


def test_spark_read_table():
    code = 'df = spark.read.table("sales_data")'
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"sales_data"}


def test_spark_read_table_with_variable():
    code = """
table_name = "sales_data"
df = spark.read.table(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"sales_data"}


def test_spark_read_table_with_variable_inside_variable():
    code = """
var = "sales_data"
table_name = var
df = spark.read.table(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"sales_data"}


def test_spark_read_table_with_concatenation():
    code = """
table_prefix = "sales"
table_suffix = "_data"
table_name = table_prefix + table_suffix
df = spark.read.table(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"sales_data"}


def test_spark_read_table_with_multiple_concatenations():
    code = """
table1 = "users"
table2 = "transactions"
table_name = table1 + "_" + table2
df = spark.read.table(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"users_transactions"}


def test_spark_read_table_with_f_string():
    code = """
table_base = "inventory"
table_suffix = "2024"
table_name = f"{table_base}_{table_suffix}"
df = spark.read.table(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"inventory_2024"}


def test_spark_read_table_with_format_method():
    code = """
table_base = "employees"
year = 2023
table_name = "{}_{}".format(table_base, year)
df = spark.read.table(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"employees_2023"}


def test_spark_read_table_with_percent_formatting():
    code = """
table_base = "departments"
month = "jan"
table_name = "%s_%s" % (table_base, month)
df = spark.read.table(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"departments_jan"}


def test_spark_read_table_multiline():
    code = """
table_name = (
    "product_catalog"
)
df = spark.read.table(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"product_catalog"}


def test_spark_read_table_with_3_concatenations():
    code = """
table1 = "sales"
table2 = "_data"
table3 = "_2024"
table_name = table1 + table2 + table3
df = spark.read.table(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"sales_data_2024"}


def test_spark_read_table_with_4_concatenations():
    code = """
table1 = "region"
table2 = "_sales"
table3 = "_q1"
table4 = "_2024"
table_name = table1 + table2 + table3 + table4
df = spark.read.table(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"region_sales_q1_2024"}


def test_spark_read_table_with_3_f_string_concatenations():
    code = """
region = "north"
suffix = "sales"
year = "2024"
table_name = f"{region}_{suffix}_{year}"
df = spark.read.table(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"north_sales_2024"}


def test_spark_read_table_with_4_f_string_concatenations():
    code = """
region = "west"
type = "retail"
quarter = "q2"
year = "2024"
table_name = f"{region}_{type}_{quarter}_{year}"
df = spark.read.table(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"west_retail_q2_2024"}


def test_spark_read_table_with_dictionary():
    code = """
tables = {"primary": "orders", "backup": "orders_backup"}
df = spark.read.table(tables["primary"])
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"orders"}


def test_spark_read_table_with_nested_dictionary():
    code = """
tables = {"sales": {"q1": "sales_q1", "q2": "sales_q2"}}
df = spark.read.table(tables["sales"]["q1"])
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"sales_q1"}


def test_spark_read_table_with_list_of_tables():
    code = """
table_names = ["customers", "orders"]
df = spark.read.table(table_names[0])
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"customers"}


def test_spark_read_table_with_loop():
    code = """
table_names = ["sales_q1", "sales_q2"]
for table in table_names:
    df = spark.read.table(table)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"sales_q1", "sales_q2"}


def test_spark_read_table_with_tuple():
    code = """
tables = ("users", "orders")
df = spark.read.table(tables[1])
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"orders"}


def test_spark_read_table_with_if_else():
    code = """
is_backup = True
if is_backup:
    table_name = "backup_data"
else:
    table_name = "live_data"
df = spark.read.table(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"backup_data", "live_data"}
