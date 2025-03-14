import ast
import logging

logger = logging.getLogger(__name__)


@DeprecationWarning
class SafeEvaluator(ast.NodeVisitor):

    SAFE_NODES = {
        ast.Expression,
        ast.BinOp,
        ast.UnaryOp,
        ast.Constant,
        ast.Constant,
        ast.List,
        ast.Tuple,
        ast.Dict,
        ast.Set,
        ast.Name,
        ast.Load,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.Mod,
        ast.Pow,
        ast.Constant,
    }

    def __init__(self, variables):
        self.variables = variables

    def visit(self, node):
        if not isinstance(node, tuple(self.SAFE_NODES)):
            raise ValueError(f"Unsafe operation detected: {ast.dump(node)}")
        return super().visit(node)

    def visit_BinOp(self, node):
        left = self.visit(node.left)
        right = self.visit(node.right)

        if isinstance(node.op, ast.Add):
            return left + right
        elif isinstance(node.op, ast.Sub):
            return left - right
        elif isinstance(node.op, ast.Mult):
            return left * right
        elif isinstance(node.op, ast.Div):
            return left / right
        elif isinstance(node.op, ast.Mod):
            return left % right
        elif isinstance(node.op, ast.Pow):
            return left**right
        else:
            raise ValueError("Unsupported operation")

    def visit_Num(self, node):
        return node.n

    def visit_Str(self, node):
        return node.s

    def visit_Constant(self, node):  # For Python 3.8+
        return node.value

    def visit_List(self, node):
        return [self.visit(elt) for elt in node.elts]

    def visit_Tuple(self, node):
        return tuple(self.visit(elt) for elt in node.elts)

    def visit_Dict(self, node):
        return {self.visit(k): self.visit(v) for k, v in zip(node.keys, node.values)}

    def visit_Name(self, node):
        if node.id in self.variables:
            return self.variables[node.id]
        raise ValueError(f"Unknown variable: {node.id}")

    def evaluate(self, expr):
        """
        Evaluates a given expression safely.
        """
        try:
            tree = ast.parse(expr, mode="eval")
            return self.visit(tree.body)
        except Exception as e:
            logger.warning(f"Evaluation error: {e}")
            return None
