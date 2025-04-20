import ast
import logging

from spark_ast_lineage.analyzer.extractors.registry import EXTRACTOR_REGISTRY

logger = logging.getLogger(__name__)


def get_extractor(node):
    """
    Factory method that returns the appropriate extractor instance for a given AST node.

    This method inspects a function call in the AST (e.g., `spark.read.table("...")`
    or `df.write.saveAsTable("...")`) and looks up a matching extractor class
    from the global EXTRACTOR_REGISTRY based on the method name (e.g., "table", "sql", "saveAsTable").

    The registry is populated using the `@register_extractor` decorator to allow
    for modular and collaborative extension of supported PySpark methods.

    Args:
        node (ast.AST): An AST node representing a function call.

    Returns:
        Optional[Extractor]: An instance of the corresponding extractor class if matched,
        or `None` if the method is unsupported.
    """
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        method_name = node.func.attr
        extractor_cls = EXTRACTOR_REGISTRY.get(method_name)

        if extractor_cls:
            logger.debug(f"Using {extractor_cls.__name__} for method: {method_name}")
            return extractor_cls()

    return None
