from collections import defaultdict
import ast

EVALUATOR_REGISTRY: dict[type[ast.AST], list] = defaultdict(list)


def register_evaluator(node_type: type[ast.AST]):
    """
    Decorator to register an evaluator function for a specific AST node type.

    Multiple evaluators can be registered for the same node type.
    """

    def decorator(func):
        EVALUATOR_REGISTRY[node_type].append(func)
        return func

    return decorator
