EXTRACTOR_REGISTRY = {}


def register_extractor(method_name: str):
    """
    Decorator to register an extractor class for a specific PySpark method name.

    This allows the extractor factory to dynamically discover and use the right
    extractor based on the method being called in the source code (e.g., "table",
    "sql", "saveAsTable", etc.).

    You can use it to register one or more methods like this:

        @register_extractor("table")
        class TableExtractor(BaseExtractor):
            ...

    Or register the same class for multiple methods:

        @register_extractor("saveAsTable")
        @register_extractor("insertInto")
        class WriteExtractor(BaseExtractor):
            ...

    Args:
        method_name (str): The method name to bind to the extractor (e.g., "sql", "table").

    Returns:
        Callable: A class decorator that adds the class to the global extractor registry.
    """

    def decorator(cls):
        EXTRACTOR_REGISTRY[method_name] = cls
        return cls

    return decorator
