__author__ = 'topsykretts'

from nubespark.haystack.trio_to_json import *


def ignore_id_desc(value):
    if value is None:
        return value
    space_index = value.find(" ")
    if space_index == -1:
        actual_value = value[1:]
    else:
        actual_value = value[1: space_index]
    return actual_value


def process_row_as_es_json(rows, haystack_data):
    if haystack_data:
        row = rows["json_value"]
        if row is None:
            row = {}
        else:
            row = json.loads(row)
    else:
        row = {}
    temp_tags = rows["tags_csv"]
    if temp_tags is not None:
        csv_tags_str = trio_to_json(temp_tags, separator=",")
        csv_tags = json.loads(csv_tags_str)
    else:
        csv_tags = {}
    row["original_id"] = rows["id"]
    trunc_id = ignore_id_desc(rows["id"])
    row["raw_id"] = trunc_id
    row["id"] = "r:" + trunc_id
    # handling numeric fields
    extra_row = {}
    for field in csv_tags.keys():
        value = csv_tags[field]
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

    csv_tags.update(extra_row)
    row.update(csv_tags)
    return json.dumps(row, ensure_ascii=False)


import pyspark.sql.functions as func
from nubespark.haystack.get_data_as_json import raw_get
from pyspark.sql import Row


def saveToEs(df, **es_options):
    df.write.format("org.elasticsearch.spark.sql").options(**es_options).mode("append").save()


def _get_csv_metadata(sqlContext, filename, points=False):
    sc = sqlContext._sc
    csv_metadata = sqlContext.read.format("com.databricks.spark.csv") \
        .option("header", "true") \
        .option("quote", "'") \
        .option("delimiter", ",") \
        .load(filename)
    # csv_metadata.show()
    csv_metadata = csv_metadata.select("id", "tags_csv")
    if not points:
        json_rdd = csv_metadata.rdd.map(lambda rows: process_row_as_es_json(rows, points))
    else:
        points_rows = raw_get("read?filter=point and his and kind!=\"Str\"")
        points_rdd = sc.parallelize(points_rows)

        def extract_id(row):
            return Row(id='@' + row['id'][2:], json_value=json.dumps(row, ensure_ascii=False))

        points_rdd = points_rdd.map(lambda row: extract_id(row))
        points_df = sqlContext.createDataFrame(points_rdd)
        joined_df = csv_metadata.join(points_df, "id", "left_outer")
        json_rdd = joined_df.rdd.map(lambda rows: process_row_as_es_json(rows, points))
    es_df = sqlContext.read.json(json_rdd)
    return es_df


def load_csv_metadata(sqlContext, filename, isPoint=False, **es_options):
    if es_options is None:
        raise Exception("es_options must be present with meta.es.nodes, meta.es.port, meta.es.resource set.")
    nodes = es_options.get("meta.es.nodes")
    port = es_options.get("meta.es.port")
    resource = es_options.get("meta.es.resource")

    if nodes is None or port is None or resource is None:
        raise Exception("es_options must be present with meta.es.nodes, meta.es.port, meta.es.resource set.")

    es_opts = {
        "es.resource": resource,
        "es.nodes": nodes,
        "es.port": port,
        "es.index.auto.create": "true",
        "es.mapping.id": "raw_id"
    }

    es_df = _get_csv_metadata(sqlContext, filename, isPoint)
    es_df.show(5)
    saveToEs(es_df, **es_opts)
