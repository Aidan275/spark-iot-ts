from pyspark.shell import sc, sqlContext
# these will be defined in notebook itself
__author__ = 'topsykretts'
from pyhaystack.client.niagara import Niagara4HaystackSession
# initialize a session for niagara connection
http_args = {
    "headers": {
        "Accept": "application/json"
    }
}
session = Niagara4HaystackSession(uri="http://175.45.115.115",
                                  username="admin1",
                                  password="Enviroman1",
                                  pint=False,
                                  grid_format="json",
                                  http_args=http_args
                                  )
op = session.about()
op.wait()
about = op.result

import json


class HaystackJsonResponse:
    """
    """

    def __init__(self):
        self.rows = None
        self.meta = None
        self.cols = None

    def callback(self, response):
        print("Status code = ", response.status_code)
        print("Body = ", response.body[:10])
        data_str = response.body.decode('utf-8')
        data = json.loads(data_str)
        print(json.dumps(data, indent=3))
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


def index_in_es(row):
    es_id = row["id"]
    trunc_id = map_json_ref_to_haystack(es_id)[1:]
    row["raw_id"] = trunc_id
    extra_row = {}
    for field in row.keys():
        value = row[field]
        if isinstance(value, bool):
            if value:
                my_val = "true"
            else:
                my_val = "false"
            extra_row[field + "_bool_"] = my_val
            extra_row[field] = str(value)
        elif value is not None and isinstance(value, str) and len(value) > 3 and value[:2] == "n:":
            space_index = value.find(" ")
            if space_index == -1:
                str_value = value[2:]
            else:
                str_value = value[2:space_index]
            extra_row[field + "_num_"] = float(str_value)

    row.update(extra_row)
    return json.dumps(row, ensure_ascii=False)


# getting metadata
session._get('read?filter=site or equip or point', response.callback, http_args)
point_rows = response.rows

points_rdd = sc.parallelize(point_rows)
json_rdd = points_rdd.map(lambda row: index_in_es(row))
es_df = sqlContext.read.json(json_rdd)
es_df.show()
es_options = {
    "path": "niagara4_metadata_scp_json_v2/metadata",
    "es.nodes": "localhost",
    "es.port": "9200",
    "es.index.auto.create": "true",
    "es.mapping.id": "raw_id"
}
es_df.write.format("org.elasticsearch.spark.sql").options(**es_options).mode("append").save()
