from spark_ast_lineage.analyzer.pyspark_tables_extractor import (
    PysparkTablesExtractor,
)


def test_df_write_save_as_table():
    code = 'df.write.saveAsTable("processed_data")'
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"processed_data"}


def test_df_write_parquet_path():
    code = 'df.write.parquet("/tmp/output")'
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/tmp/output"}


def test_df_write_insert_into():
    code = 'df.write.insertInto("warehouse_table")'
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"warehouse_table"}


def test_df_write_save_with_path():
    code = 'df.write.save("/tmp/output_dir")'
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/tmp/output_dir"}


def test_df_write_parquet_path_variable():
    code = """
path = "/mnt/data/processed"
df.write.parquet(path)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/mnt/data/processed"}


def test_df_write_csv_f_string():
    code = """
filename = "2024_04_19"
df.write.csv(f"/output/csv_{filename}")
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/output/csv_2024_04_19"}


def test_df_write_save_concatenated_path():
    code = """
base = "/tmp"
subfolder = "/parquet"
df.write.save(base + subfolder)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/tmp/parquet"}


def test_df_write_json_with_dict_lookup():
    code = """
outputs = {"json": "/data/json_output"}
df.write.json(outputs["json"])
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/json_output"}


def test_df_write_insert_into_variable():
    code = """
table_name = "daily_summary"
df.write.insertInto(table_name)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"daily_summary"}


def test_df_write_csv_nested_dict():
    code = """
paths = {"2024": {"csv": "/exports/2024/csv"}}
df.write.csv(paths["2024"]["csv"])
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/exports/2024/csv"}
