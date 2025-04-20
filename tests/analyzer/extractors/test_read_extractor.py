from spark_ast_lineage.analyzer.pyspark_tables_extractor import PysparkTablesExtractor


def test_read_csv_literal_path():
    code = 'df = spark.read.csv("/data/input.csv")'
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/input.csv"}


def test_read_parquet_variable_path():
    code = """
path = "/mnt/data/input.parquet"
df = spark.read.parquet(path)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/mnt/data/input.parquet"}


def test_read_json_with_f_string():
    code = """
date = "hope_2024_04_19"
df = spark.read.json(f"/data/json_{date}")
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/json_hope_2024_04_19"}


def test_read_csv_with_concatenation():
    code = """
prefix = "/data/"
file = "input.csv"
df = spark.read.csv(prefix + file)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/input.csv"}


def test_read_parquet_nested_dict():
    code = """
paths = {"2024": {"parquet": "/data/2024/input.parquet"}}
df = spark.read.parquet(paths["2024"]["parquet"])
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/2024/input.parquet"}


def test_read_with_percent_format():
    code = """
folder = "march"
df = spark.read.csv("/data/%s.csv" % folder)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/march.csv"}


def test_read_with_format_method():
    code = """
year = 2024
df = spark.read.json("events_{}.json".format(year))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"events_2024.json"}


def test_read_with_join_method():
    code = """
parts = ["2024", "04", "19"]
df = spark.read.csv("/input/" + "_".join(parts))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/input/2024_04_19"}


def test_read_with_dict_get():
    code = """
paths = {"csv": "/data/input.csv"}
df = spark.read.csv(paths.get("csv"))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/input.csv"}


def test_read_with_dict_get_default():
    code = """
paths = {}
df = spark.read.csv(paths.get("csv", "/data/default.csv"))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/default.csv"}


def test_read_with_ternary_expression():
    code = """
is_backup = False
df = spark.read.json("/data/backup.json" if is_backup else "/data/live.json")
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/backup.json", "/data/live.json"}


def test_read_with_object_attribute():
    code = """
class Config:
    def __init__(self):
        self.input_path = "/configs/events"

cfg = Config()
df = spark.read.load(cfg.input_path)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/configs/events"}


def test_read_with_variable_chain():
    code = """
base = "/data/"
mid = "archive"
final = "2024"
df = spark.read.parquet(base + mid + "_" + final)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/archive_2024"}


def test_read_load_with_literal():
    code = 'df = spark.read.load("/data/generic")'
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/generic"}


def test_read_with_multiplied_string():
    code = """
base = "/input"
df = spark.read.csv(base * 2)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/input/input"}


def test_read_from_class_attribute():
    code = """
class Loader:
    def __init__(self):
        self.source = "/datasets/source.csv"

loader = Loader()
df = spark.read.csv(loader.source)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/datasets/source.csv"}


def test_read_from_nested_assignment():
    code = """
raw = "/raw"
path = raw + "/data.csv"
df = spark.read.csv(path)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/raw/data.csv"}
