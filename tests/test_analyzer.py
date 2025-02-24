from src.spark_ast_lineage.analyzer.pyspark_tables_extractor import (
    PysparkTablesExtractor,
)


def test_spark_read_table():
    code = 'df = spark.read.table("sales_data")'
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"sales_data"}


def test_spark_sql():
    code = "df = spark.sql(\"SELECT * FROM customers WHERE country = 'US'\")"
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"customers"}
