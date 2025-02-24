from src.spark_ast_lineage.analyzer.pyspark_tables_extractor import (
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
