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


def test_read_csv_from_list_index():
    code = """
paths = ["/data/file1.csv", "/data/file2.csv"]
df = spark.read.csv(paths[1])
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/file2.csv"}


def test_read_parquet_from_slice_access():
    code = """
paths = ["/data/a", "/data/b", "/data/c"]
df = spark.read.parquet(paths[:2][0])
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/a"}


def test_read_json_with_chained_format():
    code = """
year = 2024
path_template = "events_{}.json"
df = spark.read.json(path_template.format(year))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"events_2024.json"}


def test_read_with_get_and_concatenation_default():
    code = """
paths = {}
default = "/default/data"
df = spark.read.csv(paths.get("file") or default)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/default/data"}


def test_read_with_str_method_chained():
    code = """
raw = "DATASET"
df = spark.read.load(("/data/" + raw.lower()))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/dataset"}


def test_read_with_nested_dict_get_and_format():
    code = """
cfg = {"base": {"template": "year_{}.json"}}
df = spark.read.json(cfg["base"]["template"].format("2023"))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"year_2023.json"}


def test_read_path_with_variable_from_class_init():
    code = """
class Cfg:
    def __init__(self, base):
        self.path = base

cfg = Cfg("/data")
df = spark.read.csv(cfg.path)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data"}


def test_read_path_with_multiple_join():
    code = """
parts = ["2023", "06", "01"]
df = spark.read.load("/data/" + "-".join(parts))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/2023-06-01"}


def test_read_load_from_tuple_index():
    code = """
paths = ("/data/one", "/data/two")
df = spark.read.load(paths[0])
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/one"}


def test_read_json_using_percent_chain():
    code = """
year = "2022"
month = "09"
df = spark.read.json("/data/%s_%s.json" % (year, month))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/2022_09.json"}


def test_read_with_get_and_f_string_default():
    code = """
paths = {}
fallback = "sales"
df = spark.read.csv(paths.get("region", f"/data/{fallback}.csv"))
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data/sales.csv"}


def test_read_path_chained_from_multiple_vars():
    code = """
a = "/d"
b = "ata"
c = ".csv"
df = spark.read.csv(a + b + c)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/data.csv"}


def test_read_json_with_attribute_chain_depth_2():
    code = """
class Config:
    def __init__(self):
        self.paths = {"input": "/deep/path.json"}

cfg = Config()
df = spark.read.json(cfg.paths["input"])
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/deep/path.json"}


def test_read_with_if_else_assignment():
    code = """
is_test = True
path = "/test/data.json" if is_test else "/prod/data.json"
df = spark.read.json(path)
    """
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    assert tables == {"/test/data.json", "/prod/data.json"}


def test_read_using_keyword_argument_path():
    code = 'df = spark.read.option("header", True).csv(path="/opt/data/input.csv")'
    tables = PysparkTablesExtractor.extract_tables_from_code(code)
    # Optional: your extractor may need to support kwargs (future)
    assert tables == set()  # or {"/opt/data/input.csv"} if supporting kwargs
