__author__ = 'topsykretts'
from pyhaystack.client.niagara import Niagara4HaystackSession
import json

# initialize a session for niagara connection

http_args = {
    "headers": {
        "Accept": "application/json"
    }
}
session = Niagara4HaystackSession(uri="your_url",
                                  username="your_username",
                                  password="your_password",
                                  pint=False,
                                  grid_format="json",
                                  http_args=http_args
                                  )
op = session.about()
op.wait()
haystack_about = op.result

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
es = Elasticsearch("http://your_server:port")
es_index = "niagara4_metadata_scp_json_v1"
bulk_kwargs = {
    "timeout": '30s',
    "refresh": 'true'
}


class HaystackJsonResponse:
    """
    """
    def __init__(self):
        self.rows=None
        self.meta=None
        self.cols=None

    def callback(self, response):
        print("Status code = ", response.status_code)
        print("Body = ", response.body[:10])
        data_str = response.body.decode('utf-8')
        data = json.loads(data_str)
        # print(json.dumps(data, indent=3))
        self.rows = data["rows"]
        self.meta = data["meta"]
        self.cols = data["cols"]

response = HaystackJsonResponse()


def map_json_ref_to_haystack(value):
    if value is None:
        return value
    if len(value) < 3:
        raise Exception("Not an Reference Exception. Haystack Json reference format is `r:<id> [dis]`")
    if value[1] == ":":
        if value[0] == "r":
            # reference case
            space_index = value.find(" ")
            if space_index == -1:
                actual_value = "@" + value[2:]
            else:
                # actual_value = '@' + value[2: space_index] + ' "'+value[space_index+1:]+ '"'
                actual_value = '@' + value[2: space_index]
            return actual_value
    raise Exception("Not an Reference Exception. Haystack Json reference format is `r:<id> [dis]`")


def index_in_es(rows):
    metadata_actions = []
    es_type="metadata"
    for row in rows:
        es_id = row["id"]
        trunc_id = map_json_ref_to_haystack(es_id)[1:]
        row['_index'] = es_index
        row['_type'] = es_type
        row['_id'] = trunc_id
        row["raw_id"] = trunc_id
        # handling numeric fields
        extra_row = {}
        for field in row.keys():
            value = row[field]
            if isinstance(value, bool):
                if value:
                    my_val = "true"
                else:
                    my_val = "false"
                extra_row[field+"_bool_"] = my_val
                extra_row[field] = str(value)
            elif value is not None and isinstance(value, str) and len(value) > 3 and value[:2] == "n:":
                space_index = value.find(" ")
                if space_index == -1:
                    str_value = value[2:]
                else:
                    str_value = value[2:space_index]
                extra_row[field+"_num_"] = float(str_value)

        row.update(extra_row)
        metadata_actions.append(row)
    bulk(es, metadata_actions, **bulk_kwargs)


# inserting site metadata
session._get('read?filter=site', response.callback, http_args)
site_rows = response.rows
index_in_es(site_rows)

# inserting equip metadata
session._get('read?filter=equip', response.callback, http_args)
equip_rows = response.rows
index_in_es(equip_rows)

# inserting point metadata
session._get('read?filter=point', response.callback, http_args)
point_rows = response.rows
index_in_es(point_rows)

