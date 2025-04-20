from spark_ast_lineage.analyzer.pyspark_tables_extractor import PysparkTablesExtractor


def test_spark_sql_literal_query():
    code = 'df = spark.sql("SELECT * FROM customers")'
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"customers"}


def test_spark_sql_variable_query():
    code = """
query = "SELECT * FROM orders"
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"orders"}


def test_spark_sql_variable_chain():
    code = """
q = "SELECT * FROM employees"
query = q
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"employees"}


def test_spark_sql_with_f_string():
    code = """
table = "products"
query = f"SELECT * FROM {table}"
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"products"}


def test_spark_sql_with_multiline_f_string():
    code = '''
table = "sales_q1"
query = f"""
    SELECT *
    FROM {table}
    WHERE year = 2024
"""
df = spark.sql(query)
    '''
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"sales_q1"}


def test_spark_sql_with_format_method():
    code = """
table = "transactions"
query = "SELECT * FROM {}".format(table)
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"transactions"}


def test_spark_sql_with_percent_format():
    code = """
table = "inventory"
query = "SELECT * FROM %s" % table
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"inventory"}


def test_spark_sql_with_dict_access():
    code = """
tables = {"main": "users"}
query = f"SELECT * FROM {tables['main']}"
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"users"}


def test_spark_sql_with_dict_get():
    code = """
tables = {"active": "orders"}
query = f"SELECT * FROM {tables.get('active')}"
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"orders"}


def test_spark_sql_with_dict_get_default():
    code = """
tables = {}
query = f"SELECT * FROM {tables.get('archive', 'orders_archive')}"
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"orders_archive"}


def test_spark_sql_with_list_index():
    code = """
table_list = ["north_sales", "south_sales"]
query = f"SELECT * FROM {table_list[1]}"
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"south_sales"}


def test_spark_sql_with_joined_string():
    code = """
prefix = "retail"
suffix = "_summary"
query = "SELECT * FROM " + prefix + suffix
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"retail_summary"}


def test_spark_sql_with_multiline_query_and_variable():
    code = """
table = "products"
query = (
    "SELECT id, name "
    "FROM " + table
)
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"products"}


def test_spark_sql_with_join():
    code = """
query = "SELECT * FROM customers JOIN orders ON customers.id = orders.customer_id"
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"customers", "orders"}


def test_spark_sql_with_cte():
    code = '''
query = """
WITH active_customers AS (
    SELECT * FROM customers WHERE active = true
)
SELECT * FROM active_customers JOIN orders ON active_customers.id = orders.customer_id
"""
df = spark.sql(query)
    '''
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"customers", "orders"}  # not 'active_customers'


def test_spark_sql_insert_into():
    code = 'df = spark.sql("INSERT INTO sales SELECT * FROM transactions")'
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"sales", "transactions"}


def test_spark_sql_multiple_statements():
    code = 'df = spark.sql("SELECT * FROM products; SELECT * FROM suppliers")'
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"products", "suppliers"}


def test_spark_sql_f_string_join():
    code = """
customers_table = "customers"
orders_table = "orders"
query = f"SELECT * FROM {customers_table} JOIN {orders_table} ON customers.id = orders.customer_id"
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"customers", "orders"}


def test_spark_sql_multiline_concat():
    code = """
table = "products"
query = (
    "SELECT * FROM " +
    table +
    " WHERE price > 100"
)
df = spark.sql(query)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"products"}
