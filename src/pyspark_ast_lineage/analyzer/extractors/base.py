import abc
import ast


class BaseExtractor(abc.ABC):
    """
    Abstract base class for all PySpark method extractors.

    All extractors must implement the `extract` method, which takes in an AST node
    representing a function call and a dictionary of preprocessed variables.

    This ensures consistency and enables static analysis tools and type checkers
    to understand the interface.
    """

    @abc.abstractmethod
    def extract(self, node: ast.Call, variables: dict[str, set]) -> set[str]:
        """
        Extract relevant table names, paths, or identifiers from a PySpark method call.

        Args:
            node (ast.Call): The function call AST node (e.g. `df.write.save(...)`)
            variables (dict[str, set]): Dictionary mapping variable names to possible values.

        Returns:
            Set[str]: A set of resolved static values (e.g. table names or paths).
        """
        pass
