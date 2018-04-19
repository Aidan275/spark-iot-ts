import numpy
from pyhaystack.client.ops.his import MetaSeries

__author__ = 'topsykretts'
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import json
from pyspark.sql import Row
import datetime, pytz
import iso8601 as iso
from pyspark.sql.types import *
from streaming.niagara_filodb_config import config
import pyhaystack
import hszinc
import sys


def getSparkSessionInstance(sparkConf):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .enableHiveSupport() \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def getNiagaraSessionInstance():
    if 'niagaraSessionInstance' not in globals():
        server_name = 'niagara.server'
        globals()['niagaraSessionInstance'] = pyhaystack.connect(**config.get(server_name))
    return globals()['niagaraSessionInstance']


es_path = config.get("meta.es.resource")
es_nodes = config.get("meta.es.nodes")
es_port = config.get("meta.es.port")

dataset = config.get("history.dataset")


def createContextFunction():
    """

    :return:
    """
    conf = SparkConf()
    conf.setAppName("NiagaraToFiloDB")
    conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 300)
    sparkSession = getSparkSessionInstance(conf)
    # todo bring data from niagara

    if len(sys.argv) > 1:
        arg = sys.argv[1]
        start = None
        try:
            datetime.datetime.strptime(arg, "%Y-%m-%d")
            start = arg
        except:
            raise Exception("start date argument must be of format yyyy-MM-dd")
    else:
        start = None

    def get_histories(point_id, rng):
        """
        """
        # getting history as dataframe
        session = getNiagaraSessionInstance()
        history_op = session.his_read_series(hszinc.Ref(point_id), rng=rng)
        history_op.wait()
        history = history_op.result
        return history

    def get_tz():
        session = getNiagaraSessionInstance()
        about_op = session.about()
        about_op.wait()
        res = about_op.result
        return {"tz": res[0]["serverTime"].tzinfo, "name": res[0]["tz"]}

    def get_rng(start=None):
        tz = get_tz()

        to_zinc_dt = lambda dt: "%s.%s%s" % (
            dt.strftime('%Y-%m-%dT%H:%M:%S'),
            '{:03.0f}'.format(dt.microsecond / 1000.0),
            dt.strftime('%z %Z')[:3] + ":" + dt.strftime('%z %Z')[3:5] + " " + tz["name"]
        )
        # start = to_zinc_dt(tz.localize(datetime.datetime(2018, 2, 1, 0, 0)))
        if start is not None:

            start = to_zinc_dt(datetime.datetime.strptime("%(start)sT00:00:00" % {"start": start},
                                                          "%Y-%m-%dT%H:%M:%S").replace(tzinfo=tz["tz"]))
            actual_now = datetime.datetime.now(tz=tz["tz"])
            now = to_zinc_dt(actual_now)
            print("start = ", start)
            print("now = ", now)
            rng = start + ", " + now
            return rng
        else:
            actual_now = datetime.datetime.now(tz=tz["tz"])
            now = to_zinc_dt(actual_now)
            # assuming there is maximum delay of two hours in availability of history
            before2hrs = actual_now - datetime.timedelta(hours=2)
            yesterday = to_zinc_dt(before2hrs)
            print("start = ", yesterday)
            print("now = ", now)
            rng = yesterday + ", " + now
            return rng

    def get_history_json(point_id, rng):
        try:
            points_his = get_histories(point_id, rng)
        except:
            print("No records found for " + point_id + " in time range " + rng)
            points_his = None

        import json
        his_list = []
        if points_his is not None and isinstance(points_his, MetaSeries):
            # print("Sending ", point_id, " Records to Kafka")
            for index, value in points_his.iteritems():
                row_data = {"ts": str(index)}
                if isinstance(value, numpy.bool_):
                    if value:
                        val = True
                    else:
                        val = False
                    row_data["val"] = val
                else:
                    row_data["val"] = str(value)
                row_data["pointName"] = point_id
                # print(json.dumps(row_data, ensure_ascii=False))
                his_list.append(json.dumps(row_data, ensure_ascii=False))
        return his_list

    # ########### streaming logic ######################
    points = sparkSession.read.format("org.elasticsearch.spark.sql") \
        .option("path", es_path) \
        .option("es.nodes", es_nodes) \
        .option("es.port", es_port) \
        .load()
    points = points.where("point = 'm:' and his = 'm:'")
    points.registerTempTable("points")
    points.cache()
    points.show()
    rng = get_rng(start)
    his_json = points.rdd.flatMap(lambda row: get_history_json(row["raw_id"], rng))
    for json_str in his_json.take(3):
        print(json_str)
    process_rdd(his_json)
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
    rows = [timestamp, datetime_val, pointName, val]
    return Row(*rows)


def get_schema():
    return StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("datetime", LongType(), False),
        StructField("pointName", StringType(), False),
        StructField("raw_value", StringType(), False)
    ])


def process_rdd(rdd):
    if not rdd.count() == 0:
        spark_session = getSparkSessionInstance(rdd.context.getConf())
        row_rdd = rdd.map(lambda value: clean_data(value))
        streamDF = spark_session.createDataFrame(row_rdd, get_schema())
        streamDF.createOrReplaceTempView("streamDF")
        joinedDF = spark_session.sql("SELECT * from streamDF as h left join points as p on h.pointName = p.raw_id")

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
                else:  # Edge case of True or False values but kind isn't Bool
                    try:
                        return float(value)
                    except ValueError:
                        if isinstance(value, str) and value.lower() == "true":
                            return 1.0
                        else:
                            return 0.0

        clean_udf = udf(lambda value, unit, kind: clean_raw_value(value, unit, kind), DoubleType())
        if "unit" not in joinedDF.columns:
            joinedDF = joinedDF.withColumn("unit", lit(""))
        joinedDF = joinedDF.withColumn("value", clean_udf(col("raw_value"), col("unit"), col("kind")))

        def get_year_month(val):
            return datetime.datetime.fromtimestamp(val / (1000 * 1000 * 1000)).strftime("%Y-%m")

        year_month_udf = udf(lambda date_val: get_year_month(date_val), StringType())
        final_df = joinedDF.withColumn("yearMonth", year_month_udf(col("datetime")))
        # generating partition key as using only siteRef as partition may produce to large partition and cassandra partition should be < 1 GB
        final_df = final_df.select("timestamp", "datetime", "yearMonth", "pointName", "value",
                                   "unit")
        # def fill_null_value(value, filler):
        #     if value is None:
        #         return filler
        #     else:
        #         return value
        #
        # fill_null_value_udf = udf(lambda value, filler: fill_null_value(value, filler))
        #
        # final_df = final_df.withColumn("siteRef", fill_null_value_udf(col("siteRef"), lit(default_site)))
        # final_df = final_df.withColumn("equipName", fill_null_value_udf(col("equipName"), lit(default_equipRef)))

        final_df.show()
        # final_df.printSchema()

        final_df.write.format("filodb.spark") \
            .option("dataset", dataset) \
            .option("partition_keys", "yearMonth") \
            .option("row_keys", "datetime,pointName") \
            .option("chunk_size", "500") \
            .mode("append") \
            .save()


ssc = createContextFunction()
ssc.start()
ssc.awaitTermination()
