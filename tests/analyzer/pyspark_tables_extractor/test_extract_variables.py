import ast
import pytest

from spark_ast_lineage.analyzer.pyspark_tables_extractor import PysparkTablesExtractor


def test_simple_assignment():
    code = 'a = "hello"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"a": "hello"}


def test_multiple_assignments():
    code = 'a = 10\nb = 20\nc = "test"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"a": 10, "b": 20, "c": "test"}


def test_tuple_unpacking():
    code = 'a, b = 5, "world"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"a": 5, "b": "world"}


def test_list_unpacking():
    code = '[a, b] = [42, "data"]'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"a": 42, "b": "data"}


def test_dictionary_assignment():
    code = 'a = {"key": "value"}'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"a": {"key": "value"}}


def test_function_call_assignment():
    code = "a = some_function()"
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert "a" in variables  # Value may not be resolved statically


def test_attribute_assignment():
    code = 'self.a = "class_var"'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"self.a": "class_var"}


def test_nested_assignment():
    code = "a = b = c = 99"
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {"a": 99, "b": 99, "c": 99}


def test_ignore_expressions():
    code = 'print("Hello World")'
    tree = ast.parse(code)
    variables = PysparkTablesExtractor._extract_variables(tree, code)
    assert variables == {}


if __name__ == "__main__":
    pytest.main()
