# **pyspark-ast-lineage**

**pyspark-ast-lineage** is a Python package designed to perform **static analysis of PySpark table lineage** using **Abstract Syntax Tree (AST)** parsing. The tool extracts table dependencies from PySpark scripts **without executing any Spark code**, providing valuable insights into **data lineage**, **governance**, and **debugging** tasks.

---

## **Features**
- **No Spark execution required** – purely static analysis  
- **Detects table lineage** from PySpark DataFrame API and SQL queries
- **Provides a custom `safeEvaluator`** to evaluate complex string scenarios including ternary operators, `if-else` clauses, class-based logic, etc..
- **Works in Python scripts** (pending support for Jupyter notebooks)  
- **Verbose output** with variable and code fragment details  
- **Syntax highlighting** for better readability of extracted code fragments  
- **Customizable output** for easy integration with different data governance frameworks  

---

## **Design Pattern - Factory and Registry**
`pyspark-ast-lineage` adopts the **Factory Design Pattern** to ensure extensibility and modularity in handling various PySpark table extraction strategies. A registry function is used to keep track of different extractors based on the type of AST node being processed. This pattern allows us to easily add support for new types of table extraction without modifying the core logic. By registering each new extractor, the system can dynamically select the appropriate one based on the type of node it encounters during AST parsing. This design improves code maintainability and fosters open-source collaboration by allowing others to extend the package easily.

### Benefits of the Factory and Registry Approach:
- **Extensibility**: New extractors can be added easily without changing the core logic.
- **Modularity**: Each extraction strategy is isolated in its own class.
- **Community Contribution**: The registry pattern makes it easy for contributors to add new functionality by simply implementing a new extractor and registering it.

---

## **Complex Scenario Handling - SafeEvaluator**
One of the key features of `pyspark-ast-lineage` is the **`safeEvaluator`**, which was implemented to handle complex scenarios in PySpark code. It evaluates variables with multiple possible values, including those derived from `if-else` conditions, ternary operators, or even values coming from different classes or functions. This allows for accurate lineage tracking even when code fragments use conditional logic to define their values.

For instance, `safeEvaluator` can handle complex expressions like:
```python
output_path = "/data/{'json', 'parquet'}" if output_type == "json" else "/data/global_sales"
```
Where `safeEvaluator` smartly evaluates and returns the possible values for the variable, ensuring that the system correctly tracks all dependencies.

The **`safeEvaluator`** is designed to be flexible, and can evaluate various types of expressions, providing more accurate results when dealing with dynamic file paths or table names defined by complex conditions in PySpark scripts.

---

## **Installation**

You can install `pyspark-ast-lineage` directly from PyPI using pip:

```bash
pip install pyspark-ast-lineage
```

Alternatively, you can clone the repository and install it manually:

```bash
git clone https://github.com/richardesp/pyspark-ast-lineage
cd pyspark-ast-lineage
pip install .
```

---

## **Usage**

The main functionality of `pyspark-ast-lineage` is to extract table names from PySpark code. You can run the tool through the command-line interface (CLI) as follows:

```bash
python -m pyspark_ast_lineage.cli <python_script.py> --verbose
```

### CLI Options:
- `file`: **Required**. Path to the PySpark Python script you want to analyze.  
- `--verbose`: **Optional**. Provides detailed extraction output, including resolved code fragments, processed variables, and line numbers.

### Example:

```bash
python -m pyspark_ast_lineage.cli examples/example_data_processing.py --verbose
```

This will analyze the `example_data_processing.py` script, extract table dependencies, and print the verbose output with table lineage details, processed variables, and code fragments.

---

## **Output Example**

### Verbose Output:
```plaintext
Extracting tables from examples/example_data_processing.py ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 0:00:00

----------------------------------------------------------------------
Table: /data/2024/march_sales.csv
Extractor: ReadExtractor
Line Number: 15
Operator Applied: ReadExtractor
Processed variables: {'file_path': {'/data/2024/march_sales.csv'}}
Code Fragment: 
  1 # Read CSV file                                                                                                                                     
  2 df = spark.read.csv(file_path, header=True, inferSchema=True)                                                                                       
  3                                                                                                                                                     
----------------------------------------------------------------------

...
```

---

## **Code Structure**

The `pyspark-ast-lineage` project is structured to maintain modularity and scalability to enhance open-source collaboration. Below is an overview of the directory layout:

```
.
├── LICENSE                     # License information
├── README.md                   # Project description and usage instructions
├── examples/                   # Example PySpark scripts for testing and demonstration
│   └── example_data_processing.py # Example file to demonstrate functionality
├── mypy.ini                    # Configuration for MyPy static type checker
├── pyproject.toml              # Project configuration and dependencies
├── src/                        # Main source code
│   ├── pyspark_ast_lineage     # Core package
│   │   ├── __init__.py         # Package initializer
│   │   ├── analyzer/           # Core analyzer module
│   │   │   ├── __init__.py     # Analyzer package initializer
│   │   │   ├── extractors/     # Extractors for various table operations
│   │   │   │   ├── __init__.py # Extractors package initializer
│   │   │   │   ├── base.py     # Base extractor class
│   │   │   │   ├── read_extractor.py # Read operation extractor
│   │   │   │   ├── registry.py # Registry for extractors
│   │   │   │   ├── sql_extractor.py # SQL query extractor
│   │   │   │   ├── table_extractor.py # Table extraction logic
│   │   │   │   └── write_extractor.py # Write operation extractor
│   │   │   ├── pyspark_tables_extractor.py # The main entry point for PySpark table extraction
│   │   │   └── utils/          # Utility functions for extraction and evaluation
│   │   │       ├── __init__.py # Utils package initializer
│   │   │       ├── evaluators/ # Evaluator modules for variable and expression evaluation
│   │   │       │   ├── __init__.py # Evaluators package initializer
│   │   │       │   ├── eval_attribute.py # Attribute evaluation
│   │   │       │   ├── eval_binop.py # Binary operations evaluation
│   │   │       │   ├── eval_boolop.py # Boolean operations evaluation
│   │   │       │   ├── eval_call_dict_methods.py # Evaluation of dictionary method calls
│   │   │       │   ├── eval_call_string_methods.py # Evaluation of string method calls
│   │   │       │   ├── eval_ifexp.py # Ternary/if expressions evaluation
│   │   │       │   ├── eval_joinedstr.py # String concatenation evaluation
│   │   │       │   ├── eval_name.py # Name evaluation
│   │   │       │   ├── eval_subscript.py # Subscript (indexing) evaluation
│   │   │       │   └── registry.py # Registry for evaluators
│   │   │       ├── extractor_factory.py # Factory for creating appropriate extractors
│   │   │       ├── safe_evaluator.py # Safe evaluator to handle complex scenarios
│   │   │       └── variable_tracker.py # Tracks and resolves variables in code
│   │   ├── cli.py               # Command-line interface script for executing the tool
│   │   └── utils/               # General utilities used across the project
│   │       └── __init__.py       # Utilities package initializer
├── tests/                       # Unit tests for the project
│   ├── __init__.py              # Tests package initializer
│   └── analyzer/                # Tests for the analyzer module
│       ├── __init__.py          # Analyzer tests package initializer
│       ├── extractors/          # Tests for the extractors
│       │   ├── __init__.py      # Extractor tests package initializer
│       │   ├── test_read_extractor.py # Tests for ReadExtractor
│       │   ├── test_sql_extractor.py # Tests for SQLExtractor
│       │   ├── test_table_extractor.py # Tests for TableExtractor
│       │   └── test_write_extractor.py # Tests for WriteExtractor
│       └── pyspark_tables_extractor/ # Tests for pyspark_tables_extractor.py
│           ├── __init__.py      # Tests for pyspark_tables_extractor package
│           └── test_extract_variables.py # Tests for variable extraction
```

### Key Directories and Files

- **`pyspark_ast_lineage/`**: Main source folder containing the package's code.
    - **`cli.py`**: Contains the command-line interface logic for running the tool from the terminal.
    - **`analyzer/`**: Core module that implements AST-based extraction of table dependencies.
        - **`extractors/`**: Contains specific extractors for different PySpark operations, such as `read_extractor.py` and `write_extractor.py`. These extractors work with various PySpark operations to detect table dependencies.
        - **`evaluators/`**: Contains complex evaluators for handling intricate Python expressions such as ternary operators, attribute accesses, and more. These are used during the variable resolution process.
        - **`utils/`**: Provides utility functions, including `safe_evaluator.py` for evaluating complex scenarios and `variable_tracker.py` for managing variable states during code analysis.
    - **`spark_ast_lineage.egg-info/`**: Metadata generated by the package build tool (`setuptools`) that contains package information and dependencies.

### **Important Classes & Functions**

- **`PysparkTablesExtractor`**: Main class responsible for analyzing PySpark code and extracting table dependencies from DataFrame API calls and SQL queries.
    - **`extract_tables_from_code()`**: The function that parses the Python code and extracts the table names, either in a simple or verbose mode, providing detailed information on the extraction.
    - **`clean_multiple_values()`**: Utility function that processes variables with multiple possible values (e.g., sets or lists) and resolves them to single values.
    - **`resolve_variables_in_code()`**: Replaces variable references in the code with their respective values for accurate code fragment extraction.

- **`extractors/`**:
    - **`base.py`**: The base extractor class that defines common functionality for all extractors.
    - **`read_extractor.py`**: Handles operations related to reading data from external sources (e.g., CSV, Parquet).
    - **`table_extractor.py`**: Handles operations related with `spark.table` API function.
    - **`sql_extractor.py`**: Extracts table dependencies from SQL queries.
    - **`write_extractor.py`**: Handles operations related to writing data to external sources.

---

## **Contributing**

We welcome contributions to **pyspark-ast-lineage**! If you have suggestions, improvements, or bug fixes, feel free to submit an **issue** or a **pull request**. Please, follow the instructions outlined in the [CONTRIBUTING.md](CONTRIBUTING.md) file for guidelines.

---

## **License**

This project is licensed under the **GNU General Public License (GPL)**. See the [LICENSE](LICENSE) file for details.

---

## **Questions and Support**

For any questions, open an issue or contact the maintainers:

- GitHub: [@richardesp](https://github.com/richardesp)

Thank you for contributing!