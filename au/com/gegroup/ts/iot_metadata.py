from au.com.gegroup.haystack import HaystackToSQL, flatten

__author__ = 'topsykretts'
import os
from elasticsearch import Elasticsearch
import json


class IOTMetadata:
    """
    Class with methods for following purpose:
    1) Read metadata using haystack format
    2) Add/Edit/Remove tags in metadata
    """

    def __init__(self, **kwargs):
        """

        :param kwargs: es cconnection options like meta.es.nodes, meta.es.port, meta.es.resource
        :return:
        """
        host = None
        port = None
        resource = None
        if kwargs is not None:
            host = kwargs.get("meta.es.nodes")
            port = kwargs.get("meta.es.port")
            resource = kwargs.get("meta.es.resource")
        if host is None:
            host = os.getenv("META_ES_NODES", "localhost")
        if port is None:
            port = os.getenv("META_ES_PORT", "9200")
        if resource is None:
            resource = os.getenv("META_ES_RESOURCE")

        if resource is not None:
            sep_index = resource.find("/")
            rev_sep_index = resource.rfind("/")
            if sep_index != -1 and sep_index == rev_sep_index:
                self.index = resource[:sep_index]
                self.type = resource[sep_index + 1:]
        if self.index is None or self.type is None:
            raise Exception("Elasticsearch resource should be of format <index>/<type> and should be set by "
                            "either passing `meta.es.resource` param in kwargs "
                            "or setting environment variable META_ES_RESOURCE")
        self.host = host
        self.port = port

        self.es = Elasticsearch(hosts=self.host + ":" + self.port)
        self.query = None
        self.parser = HaystackToEs()

    def read(self, debug=False):
        if self.query is None:
            return self.es.search(index=self.index, doc_type=self.type)["hits"]
        else:
            if debug:
                print(self.query)
            return self.es.search(index=self.index, doc_type=self.type, q=self.query)

    def prettyPrint(self, data):
        print(json.dumps(data, indent=3, ensure_ascii=False))

    def update(self, tags, debug=False, result=False):
        """
        :param tags: haystack Marker tags to add
        """
        if tags is None:
            raise Exception("Tags should be either list of string or string value (comma separated)")

        if isinstance(tags, str):
            _tags = tags.split(",")
        else:
            _tags = tags

        scripts = []
        prefix = "ctx._source."
        for tag in _tags:
            scripts.append(prefix + tag.strip() + ' = "m:"')
        script = ";".join(scripts) + ";"
        if debug:
            print(script)
            print(self.query)
        body = {
            "script": script
        }
        self.es.update_by_query(index=self.index, doc_type=self.type, body=body, q=self.query, refresh=True)
        if result:
            return self.es.search(index=self.index, doc_type=self.type, q=self.query)
        else:
            return True

    def remove(self, tags, debug=False, result=False):
        if tags is None:
            raise Exception("Tags should be either list of string or string value (comma separated)")

        if isinstance(tags, str):
            _tags = tags.split(",")
        else:
            _tags = tags

        scripts = []
        prefix = "ctx._source.remove(\""
        for tag in _tags:
            scripts.append(prefix + tag.strip() + '")')
        script = ";".join(scripts) + ";"
        if debug:
            print(script)
            print(self.query)
        body = {
            "script": script
        }
        self.es.update_by_query(index=self.index, doc_type=self.type, body=body, q=self.query, refresh=True)
        if result:
            return self.es.search(index=self.index, doc_type=self.type, q=self.query)
        else:
            return True

    def find(self, query):
        self.query = self.parser.parse(query)[0]
        return self


class HaystackToEs(HaystackToSQL):
    """
    Convert haystack format query to ES format
    """

    def __init__(self):
        super().__init__()

    def visit_term(self, node, children):
        return children

    def visit_name(self, node, children):
        self.cols.append(node.text.strip())
        return True

    def visit_condAnd(self, node, children):
        children = flatten(children)
        return self.construct_query(children, "AND")

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
        return self.construct_query(children, "OR")

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
        # Node
        path = children[0]
        op = children[2]
        val = children[4][0]
        # handling null and not null query..
        if isValDecimal:
            path = path.strip() + "_num_"
        elif isValBool:
            path = path.strip() + "_bool_"
        else:
            path = path.strip() + ".raw"

        if val is None:
            if op == ":":
                return "NOT _exists_:" + path.strip()
            elif op == "!=":
                return "_exists_:" + path.strip()
        elif len(val) > 3 and val[:3] == "r\:":
            # handling references
            if op == ":":
                return path.strip() + " " + op + " " + val.strip() + " OR " + path.strip() + " " + op + " " + val.strip() + "\ *"
            elif op == "!=":
                return "NOT " + path.strip() + " : " + val.strip() + " AND NOT " + \
                       path.strip() + " : " + val.strip() + "\ *"
        elif op == "!=":
            return "NOT " + path.strip() + " : " + val.strip()

        return path.strip() + " " + op + " " + val.strip()

    def visit_cmpOp(self, node, children):
        if node.text == "==":
            return ":"
        elif node.text != "!=":
            return ":" + node.text
        else:
            return node.text

    def visit_null(self, node, children):
        return None

    def visit_bool(self, node, children):
        return 'true' if node.text == "true" else 'false'

    def visit_str(self, node, children):
        content = node.match.group('content')
        prefix = ''
        if ":" in content:
            prefix = "s:"
        return '"' + prefix + content + '"'

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
        return tag + '.raw:"m:"'

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
        return "NOT " + tag + '.raw:"m:"'

    def visit_ref(self, node, childern):
        return flatten(childern)[0].replace(":", "\\:")

    def visit_refVal(self, node, children):
        return "r:" + node.text
