__author__ = 'topsykretts'

import parsimonious
from parsimonious.grammar import Grammar


def flatten(S):
    """
    Sourced from:
    http://www.sanfoundry.com/python-program-flatten-nested-list-using-recursion/
    """
    if S == []:
        return S
    if isinstance(S[0], list):
        return flatten(S[0]) + flatten(S[1:])
    return S[:1] + flatten(S[1:])


class HaystackToSQL(parsimonious.NodeVisitor):
    """
    Metadata uses Project Haystack format and structure.
    So, this class is used for parsing haystack style query to Spark SQL like query.
    Markers in haystack are supposed to be stored with value "1".
    https://project-haystack.org/doc/Filters
    This work is derived from Christopher Andronikos <can@gegroup.com.au> 's project.
    """
    def __init__(self):
        self.grammar = Grammar("""\
            filter = condOr
            condOr = condAnd ( condOrSeq )*
            condOrSeq = ws "or" ws condAnd
            condAnd = term ( condAndSeq )*
            condAndSeq = ws "and" ws term
            term = cmp / parens / has / missing
            parens = "(" ws filter ws ")"
            has = name ws pathSeq
            missing = ws "not" ws path
            cmp = path ws* cmpOp ws* val
            cmpOp = "==" / "!=" / "<=" / ">=" / "<" / ">"
            path = name ws* pathSeq
            pathSeq = ( "->" name )*
            ws = " "*

            name = ~"(?!not)[a-z][_a-z0-9A-Z]*"

            val = str / bool / number
            bool = "true" / "false"
            str = ~r'"(?P<content>[A-Z 0-9]*)"'i
            number = decimal
            decimal = "-"? digits ("." digits)?
            digits = ~r'[0-9][0-9_]*'i
            """)
        self.cols = None

    def visit_term(self,node,children):
        return children

    def visit_name(self,node,children):
        self.cols.append(node.text.strip())
        return True

    def visit_condAnd(self,node,children):
        children = flatten(children)
        return " and ".join(children)

    def visit_condAndSeq(self,node,children):
        return children[3][0]

    def visit_condOr(self,node,children):
        children = flatten(children)
        return " or ".join(children)


    def visit_cmp(self,node,children):
        path = children[0]
        op = children[2]
        val = children[4][0]
        return path.strip()+" "+op+" "+val.strip()

    def visit_cmpOp(self,node,children):
        if node.text == "==":
            return "="
        else:
            return node.text

    def visit_bool(self,node,children):
        return True if node.text=="true" else False

    def visit_str(self,node,children):
        content = node.match.group('content')
        return "'"+content+"'"

    def visit_number(self,node,children):
        content = float(node.text)
        return content

    def visit_decimal(self,node,children):
        content = float(node.text)
        return content

    def visit_digits(self,node,children):
        return node.text

    def visit_strChar(self,node,children):
        """
        See below function `visit_double_quoted_text`
        https://programtalk.com/vs2/python/8755/dxr/dxr/query.py/
        """
        content = node.match.group('content')
        return content

    def visit_val(self,node,children):
        return children

    def visit_parens(self,node,children):
        children = flatten(children)
        rets = []
        for sub_child in children:
            rets.append("( "+sub_child+" )")
        return rets

    def visit_filter(self,node,children):
        return children

    def visit_condOrSeq(self,node,children):
        children = flatten(children)

        return children

    def visit_has(self,node,children):
        tag = node.text.strip()
        return tag + " = 1"

    def visit_path(self,node,children):
        return node.text

    def visit_pathSeq(self,node,children):
        if(len(children)>1):
            raise(Exception("Pathing not supported"))
        return children

    def generic_visit(self, node, children):
        if node.expr_name=='ws':
            pass
        elif node.expr_name == '':
            pass
        else:
            print("generic", "'"+node.expr_name+"'")
        return children

    def visit_missing(self, node, children):
        children = flatten(children)
        tag = children[0].strip()
        return tag + " != 1"

    def parse(self, text):
        self.cols = []
        ret = super().parse(text)
        return ret, self.cols
