__author__ = 'topsykretts'

from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, TimestampType
import pyspark.sql.functions as func
import datetime
from pytz import timezone


def filter_null(key1, key2):
    return "%(key1)s_value IS NOT NULL AND %(key2)s_value IS NOT NULL" % ({'key1': key1, 'key2': key2})


def get_value_col(key):
    return col("%(key)s_value" % ({'key': key}))


def join_keys():
    return ["siteRef", "levelRef", "equipRef"]


def is_empty(df):
    return df.first() is None


def is_not_empty(df):
    return not is_empty(df)


def get_timestamp_col(df, col_name, time_col="time", tz=None):
    if tz is None:
        tz = "Australia/Sydney"

    def convert_ns_to_timestamp(ns):
        return datetime.datetime.fromtimestamp(ns / (1000 * 1000 * 1000), tz=timezone(tz))

    udf_convert_timestamp = func.udf(lambda time: convert_ns_to_timestamp(time), TimestampType())
    return df.withColumn(col_name, udf_convert_timestamp(func.col(time_col)))


def join_bool(left, right, left_alias, right_alias, keys=None):
    """

        :param left:
        :param right:
        :param left_alias:
        :param right_alias:
        :param keys:
        :return:
        """
    if keys is None:
        keys = ["siteRef", "equipName"]

    left_df = left.leftJoin(right, tolerance="30 days", key=keys, left_alias=left_alias,
                            right_alias=right_alias)
    right_df = right.leftJoin(left, tolerance="30 days", key=keys, left_alias=right_alias,
                              right_alias=left_alias)
    print("Left Join DF")
    left_df.show()
    print("Right Join DF")
    right_df.show()
    merged_df = left_df.merge(right_df.select(left_df.columns))

    window = Window.partitionBy("equipName").orderBy("time")
    numbered_df = merged_df.withColumn("rn", func.row_number().over(window))

    def correct_null_value(rowNum, value, leadValue):
        if rowNum == 1:
            if value is None:
                value = (leadValue + 1) % 2
            return value
        else:
            return value

    def correct_null_pointName(rowNum, value, leadValue):
        if rowNum == 1:
            if value is None:
                value = leadValue
            return value
        else:
            return value

    udf_correct_null_value = func.udf(lambda rowNum, value, leadValue: correct_null_value(rowNum, value, leadValue),
                                      DoubleType())
    # udf_correct_null_pointName = func.udf(
    #     lambda rowNum, value, leadValue: correct_null_pointName(rowNum, value, leadValue), StringType())

    numbered_df = numbered_df.withColumn(left_alias + "_value", udf_correct_null_value(func.col("rn"),
                                                                                       func.col(
                                                                                           left_alias + "_value"),
                                                                                       func.lead(
                                                                                           left_alias + "_value", 1,
                                                                                           None).over(window)))
    numbered_df = numbered_df.withColumn(right_alias + "_value", udf_correct_null_value(func.col("rn"),
                                                                                        func.col(
                                                                                            right_alias + "_value"),
                                                                                        func.lead(
                                                                                            right_alias + "_value",
                                                                                            1,
                                                                                            None).over(window)))

    return numbered_df.drop("rn")


def mismatch_bool(df, left_alias, right_alias, col_name="mismatch"):
    """

    :param df:
    :param left_alias:
    :param right_alias:
    :return:
    """

    def find_mismatch(left_val, right_val):
        if left_val is None or right_val is None:
            return None
        else:
            if left_val == 0:
                left_bool = False
            else:
                left_bool = True

            if right_val == 0:
                right_bool = False
            else:
                right_bool = True
        if (right_bool and not left_bool) or (not right_bool and left_bool):
            return 1.0
        else:
            return 0.0

    udf_find_mismatch = func.udf(lambda left_val, right_val: find_mismatch(left_val, right_val), DoubleType())
    df = df.withColumn(col_name, udf_find_mismatch(get_value_col(left_alias), get_value_col(right_alias)))
    return to_skyspark_run_signal(df, col_name)


def union_bool(df, left_alias, right_alias, col_name="union"):
    """

        :param df:
        :param left_alias:
        :param right_alias:
        :return:
        """

    def find_union(left_val, right_val):
        if left_val is None or right_val is None:
            return None
        else:
            if left_val == 0:
                left_bool = False
            else:
                left_bool = True

            if right_val == 0:
                right_bool = False
            else:
                right_bool = True

            union = left_bool or right_bool
            if union:
                return 1.0
            else:
                return 0.0

    udf_find_union = func.udf(lambda left_val, right_val: find_union(left_val, right_val), DoubleType())

    df = df.withColumn(col_name, udf_find_union(get_value_col(left_alias), get_value_col(right_alias)))
    return to_skyspark_run_signal(df, col_name)


def intersection_bool(df, left_alias, right_alias, col_name="intersection"):
    """
        Intersection between the join of two run (bool value) dfs
        :param df: the joined df obtained from join_bool method
        :param left_alias:
        :param right_alias:
        :return:
        """

    def find_intersection(left_val, right_val):
        if left_val is None or right_val is None:
            return None
        else:
            if left_val == 0:
                left_bool = False
            else:
                left_bool = True

            if right_val == 0:
                right_bool = False
            else:
                right_bool = True

            union = left_bool and right_bool
            if union:
                return 1.0
            else:
                return 0.0

    udf_find_intersection = func.udf(lambda left_val, right_val: find_intersection(left_val, right_val), DoubleType())

    df = df.withColumn(col_name, udf_find_intersection(get_value_col(left_alias), get_value_col(right_alias)))
    return to_skyspark_run_signal(df, col_name)


def to_skyspark_run_signal(df, check_col):
    """
        In skyspark, the sparks/periods for run (like ENB or STS) is only present when there is
        change in value. This methods convert the boolean value df ts to above format.
        :param df: the dataframe that should be converted
        :param check_col: the value column to be checked
        :return: skyspark format df
        """
    window = Window.partitionBy(["siteRef", "equipName"]).orderBy("time").rowsBetween(-1, -1)

    def keep_row(value, lag_value):
        if lag_value is None:
            return 1.0
        else:
            return value - lag_value

    udf_keep_row = func.udf(lambda value, lag_value: keep_row(value, lag_value), DoubleType())
    df = df.withColumn("should_keep", udf_keep_row(func.col(check_col), func.lag(check_col, 1, None).over(window)))
    df = df.where("should_keep != 0")
    return df.drop("should_keep")


# Function to convert normal boolean timeseries to step boolean timeseries i.e. preserving only changes


def to_step_signal(df, check_col, key_cols):
    """
        Keeping the timestamps when there is only change of signals
        :param df: the dataframe that should be converted
        :param check_col: the value column to be checked
        :param key_cols: list of columns used for partitioning
        :return: step timeseries format df
        """
    window = Window.partitionBy(key_cols).orderBy("time").rowsBetween(-1, -1)

    def keep_row(value, lag_value):
        if lag_value is None:
            return 1.0
        else:
            return value - lag_value

    udf_keep_row = func.udf(lambda value, lag_value: keep_row(value, lag_value), DoubleType())
    df = df.withColumn("should_keep", udf_keep_row(func.col(check_col), func.lag(check_col, 1, None).over(window)))
    df = df.where("should_keep != 0")
    return df.drop("should_keep")


# Function to calculate up duration according to given logic


def _get_up_time(current_time, lead_time, current_val, offset=0, unit="min"):
    if unit == "hr" or unit == "hour":
        divider = 60 * 60
    elif unit == "sec" or unit == "second":
        divider = 1
    else:
        divider = 60

    if current_val == 0:
        return 0.0
    elif lead_time is not None:
        time_diff = (lead_time - current_time).total_seconds() / divider + offset
        if time_diff > 0:
            return round(time_diff, 2)
        else:
            return 0.0
    else:
        return 0.0


get_up_time = func.udf(
    lambda current_time, lead_time, current_val, time_limit, unit: _get_up_time(current_time, lead_time, current_val,
                                                                                time_limit, unit=unit), DoubleType())


def get_step_duration(step_df, check_col, key_cols, offset=0, duration_col="duration", timestamp_col="timestamp", unit="min"):
    """

    :param step_df:
    :param key_cols:
    :param offset:
    :param duration_col:
    :return:
    """
    window = Window.partitionBy(key_cols).orderBy("time").rowsBetween(1, 1)
    df = step_df.withColumn(duration_col,
                            get_up_time(func.col(timestamp_col), func.lead(timestamp_col, 1, None).over(window),
                                        func.col(check_col), func.lit(offset), func.lit(unit)))
    required_cols = []
    required_cols.extend(["time", timestamp_col])
    required_cols.extend(key_cols)
    required_cols.extend([check_col, duration_col])
    return df.select(required_cols)


def get_sparks(bool_df, check_col, key_cols, offset=0, duration_col="duration", timestamp_col="timestamp", unit="min"):
    step_df = to_step_signal(bool_df, check_col, key_cols)
    duration_df = get_step_duration(step_df, check_col, key_cols, offset, duration_col, timestamp_col, unit=unit)
    duration_df = duration_df.withColumnRenamed(check_col, "step_value")
    return duration_df
