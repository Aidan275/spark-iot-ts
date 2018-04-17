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
    https://project-haystack.org/doc/Json
    So, this class is used for parsing haystack style query to Spark SQL like query.
    https://project-haystack.org/doc/Filters
    This work is derived from Christopher Andronikos <can@gegroup.com.au> 's project.

    Generally markers and references are used in query/filter
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

            val = bool / ref/ str / number / null
            bool = "true" / "false"
            null = "null"
            ref = "@" refVal (ws str)?
            refVal = ~r'[0-9a-zA-Z_:\-.\~]*'i
            str = ~r'"(?P<content>[0-9a-zA-Z _:\-.\~]*)"'i
            number = decimal
            decimal = "-"? digits ("." digits)?
            digits = ~r'[0-9][0-9_]*'i
            """)
        self.cols = None

    def visit_term(self,node,children):
        return children

    def visit_name(self, node, children):
        self.cols.append(node.text.strip())
        return True

    def visit_condAnd(self, node, children):
        children = flatten(children)
        return self.construct_query(children, "and")

    def construct_query(self, childern, join_type):
        ref_map = {}
        ref_less = []
        join_type = " " + join_type + " "
        for node in childern:
            if "->" in node:
                values = node.split("->")
                ref = values[0].strip()
                val = values[1].strip()
                if ref in ref_map.keys():
                    val_list = ref_map[ref]
                    val_list.append(val)
                else:
                    val_list = [val]
                ref_map[ref] = val_list
            else:
                ref_less.append(node)

        for ref in ref_map.keys():
            val = ref_map[ref]
            query = ref + " in (select " + ref + " from metadata where " + join_type.join(val) + ")"
            ref_less.append(query)
        return join_type.join(ref_less)

    def visit_condAndSeq(self, node, children):
        return children[3][0]

    def visit_condOr(self, node, children):
        children = flatten(children)
        return self.construct_query(children, "or")

    def visit_cmp(self, node, children):

        # determine if value is decimal
        isValDecimal = False
        isValBool = False
        for child in node:
            for sub_child in child:
                if sub_child.expr_name == "decimal":
                    isValDecimal = True
                if sub_child.expr_name == "bool":
                    isValBool = True
                break

        path = children[0]
        op = children[2]
        val = children[4][0]

        if isValDecimal:
            path = path.strip() + "_num_"
        if isValBool:
            path = path.strip() + "_bool_"
        # handling null and not null query..
        if val is None:
            val = "null"
            if op == "=":
                op = "is"
            elif op == "!=":
                op = "is not"
        elif len(val) > 3 and val[:3] == "'r:":
            # handling references
            if op == "=":
                return "(" + path.strip()+" "+op+" "+val.strip() + " or " + path.strip() + " LIKE " + val.strip()[:-1] + " %')"
            elif op == "!=":
                return "(" + path.strip()+" "+op+" "+val.strip() + " and " +\
                    path.strip() + " NOT LIKE " + val.strip()[:-1] + " %')"

        return path.strip()+" "+op+" "+val.strip()

    def visit_cmpOp(self, node, children):
        if node.text == "==":
            return "="
        else:
            return node.text

    def visit_null(self, node, children):
        return None

    def visit_bool(self, node, children):
        return '1' if node.text == "true" else '0'

    def visit_str(self, node, children):
        content = node.match.group('content')
        return "'" + content + "'"

    def visit_number(self, node, children):
        content = node.text
        return content

    def visit_decimal(self, node, children):
        content = node.text
        return content

    def visit_digits(self, node, children):
        return node.text

    def visit_strChar(self, node, children):
        """
        See below function `visit_double_quoted_text`
        https://programtalk.com/vs2/python/8755/dxr/dxr/query.py/
        """
        content = node.match.group('content')
        return content

    def visit_val(self, node, children):
        return children

    def visit_parens(self, node, children):
        children = flatten(children)
        rets = []
        for sub_child in children:
            rets.append("( " + sub_child + " )")
        return rets

    def visit_filter(self, node, children):
        return children

    def visit_condOrSeq(self, node, children):
        children = flatten(children)

        return children

    def visit_has(self, node, children):
        tag = node.text.strip()
        return tag + " = 'm:'"

    def visit_path(self, node, children):
        return node.text

    def visit_pathSeq(self, node, children):
        return children

    def generic_visit(self, node, children):
        if node.expr_name == 'ws':
            pass
        elif node.expr_name == '':
            pass
        else:
            print("generic", "'" + node.expr_name + "'")
        return children

    def visit_missing(self, node, children):
        children = flatten(children)
        tag = children[0].strip()
        return tag + " is null"

    def visit_ref(self, node, childern):
        return "'" + flatten(childern)[0] + "'"

    def visit_refVal(self, node, children):
        return "r:"+node.text

    def parse(self, text):
        self.cols = []
        ret = super().parse(text)
        return ret, self.cols
