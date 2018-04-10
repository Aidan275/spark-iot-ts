from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import json
from pyspark.sql import Row
import datetime, pytz
import iso8601 as iso
from pyspark.sql.types import *

__author__ = 'topsykretts'


def getSparkSessionInstance(sparkConf):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .enableHiveSupport() \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


# todo configure as cmd line options or config file

kafka_topics = ["niagara_iot_scp_apr_v1"]
kafka_params = {"metadata.broker.list": "localhost:9092", "auto.offset.reset": "smallest"}
checkpoint_dir = kafka_topics[0] + "_checkpoint"

es_path = "niagara4_metadata_scp_json_v1/metadata"
es_nodes = "45.76.115.24"
es_port = "9203"

dataset = "niagara4_history_scp_v2"

default_site = "PH_site"
default_equipRef = "PH_equip"


def createContextFunction():
    """

    :return:
    """
    conf = SparkConf()
    conf.setAppName("KafkaToFiloDBWithCp")
    conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 60)
    sparkSession = getSparkSessionInstance(conf)
    kafkaStream = KafkaUtils.createDirectStream(ssc, kafka_topics, kafka_params)
    kafkaStream.foreachRDD(lambda rdd: process_rdd(rdd))
    ssc.checkpoint(checkpoint_dir)
    return ssc


def convert_to_timestamp(date_string):
    return int((iso.parse_date(date_string) - datetime.datetime.fromtimestamp(
        0, pytz.utc)).total_seconds() * 1000 * 1000 * 1000)


def clean_data(json_str):
    json_val = json.loads(json_str)
    pointName = str(json_val['pointName'])
    val = str(json_val['val'])
    ts = str(json_val['ts'])
    timestamp = iso.parse_date(ts)
    datetime_val = convert_to_timestamp(ts)
    equipName = ""
    rows = [timestamp, datetime_val, pointName, equipName, val]
    return Row(*rows)


def get_schema():
    return StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("datetime", LongType(), False),
        StructField("pointName", StringType(), False),
        StructField("equipName", StringType(), False),
        StructField("raw_value", StringType(), False)
    ])


def process_rdd(rdd):
    if not rdd.count() == 0:
        spark_session = getSparkSessionInstance(rdd.context.getConf())
        row_rdd = rdd.map(lambda value: clean_data(value[1]))
        streamDF = spark_session.createDataFrame(row_rdd, get_schema())
        streamDF.createOrReplaceTempView("streamDF")
        points = spark_session.read.format("org.elasticsearch.spark.sql") \
            .option("path", es_path) \
            .option("es.nodes", es_nodes) \
            .option("es.port", es_port) \
            .load()

        points.cache()
        points.registerTempTable("points")

        joinedDF = spark_session.sql("SELECT * from streamDF as h left join points as p on h.pointName = p.id")

        from pyspark.sql.functions import udf, col, lit

        def clean_raw_value(value, unit, kind):
            if "Bool" == kind:
                if "true" == value:
                    return 1.0
                else:
                    return 0.0
            else:
                if unit is not None:
                    return float(value.replace(unit, "").strip())
                else:
                    return float(value)

        clean_udf = udf(lambda value, unit, kind: clean_raw_value(value, unit, kind), DoubleType())
        if "unit" not in joinedDF.columns:
            joinedDF = joinedDF.withColumn("unit", lit(""))
        joinedDF = joinedDF.withColumn("value", clean_udf(col("raw_value"), col("unit"), col("kind")))

        def get_year_month(val):
            return datetime.datetime.fromtimestamp(val / (1000 * 1000 * 1000)).strftime("%Y-%m")

        year_month_udf = udf(lambda date_val: get_year_month(date_val), StringType())
        final_df = joinedDF.withColumn("yearMonth", year_month_udf(col("datetime")))
        # generating partition key as using only siteRef as partition may produce to large partition and cassandra partition should be < 1 GB
        final_df = final_df.select("timestamp", "datetime", "yearMonth", "pointName", "siteRef", "equipRef", "value",
                                   "unit")
        final_df = final_df.withColumnRenamed("equipRef", "equipName")

        def fill_null_value(value, filler):
            if value is None:
                return filler
            else:
                return value

        fill_null_value_udf = udf(lambda value, filler: fill_null_value(value, filler))

        final_df = final_df.withColumn("siteRef", fill_null_value_udf(col("siteRef"), lit(default_site)))
        final_df = final_df.withColumn("equipName", fill_null_value_udf(col("equipName"), lit(default_equipRef)))

        final_df.show()
        # final_df.printSchema()

        final_df.write.format("filodb.spark") \
            .option("dataset", dataset) \
            .option("partition_keys", "siteRef,yearMonth") \
            .option("row_keys", "datetime,equipName,pointName") \
            .option("chunk_size", "500") \
            .mode("append") \
            .save()


ssc = StreamingContext.getOrCreate(checkpoint_dir, createContextFunction)
ssc.start()
ssc.awaitTermination()
