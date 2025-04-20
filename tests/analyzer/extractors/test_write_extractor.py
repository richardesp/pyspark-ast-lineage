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


def test_df_write_save_multiplied_string():
    code = """
prefix = "/data/part"
df.write.save(prefix * 2)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/part/data/part"}  # if joined naively


def test_df_write_with_join():
    code = """
parts = ["2024", "04", "19"]
df.write.csv("/exports/" + "_".join(parts))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/exports/2024_04_19"}


def test_df_write_dict_get():
    code = """
paths = {"csv": "/data/fallback"}
df.write.csv(paths.get("csv", "/data/default"))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/fallback"}


def test_df_write_ternary_expr():
    code = """
condition = True
df.write.save("table_a" if condition else "table_b")
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"table_a", "table_b"}  # Both outcomes are tracked


def test_df_write_attribute_access():
    code = """
class Config:
    def __init__(self):
        self.path = "/data/config_output"

cfg = Config()
df.write.json(cfg.path)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/config_output"}


def test_df_write_with_variable_path():
    code = """
path = "/data/output"
df.write.csv(path)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/output"}


def test_df_write_with_nested_dict():
    code = """
paths = {"2024": {"csv": "/exports/2024/csv"}}
df.write.csv(paths["2024"]["csv"])
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/exports/2024/csv"}


def test_df_write_with_f_string():
    code = """
folder = "2024"
df.write.parquet(f"/data/{folder}/parquet")
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/2024/parquet"}


def test_df_write_with_percent_format():
    code = """
folder = "2024"
df.write.json("/data/%s/json" % folder)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/2024/json"}


def test_df_write_with_join_method():
    code = """
parts = ["2024", "04", "19"]
df.write.save("/exports/" + "_".join(parts))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/exports/2024_04_19"}


def test_df_write_with_dict_get_default():
    code = """
paths = {"csv": "/data/fallback"}
df.write.csv(paths.get("csv", "/data/default"))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/fallback"}


def test_df_write_with_dict_get_missing_key():
    code = """
paths = {}
df.write.csv(paths.get("csv", "/data/default"))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/default"}


def test_df_write_with_object_attribute_path():
    code = """
class Config:
    def __init__(self):
        self.output = "/tmp/output"

cfg = Config()
df.write.save(cfg.output)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/tmp/output"}


def test_df_write_with_multiple_concatenation():
    code = """
folder = "/data/"
subfolder = "sales"
year = "2024"
df.write.csv(folder + subfolder + "_" + year)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/sales_2024"}


def test_df_write_with_format_call():
    code = """
folder = "2024"
df.write.save("exports_{}.parquet".format(folder))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"exports_2024.parquet"}
