__author__ = 'topsykretts'

from pyspark.sql.functions import col


def filter_null(key1, key2):
    return "%(key1)s_value IS NOT NULL AND %(key2)s_value IS NOT NULL"%({'key1': key1, 'key2': key2})


def get_value_col(key):
    return col("%(key)s_value" % ({'key': key}))


def join_keys():
    return ["siteRef", "levelRef", "equipRef"]


def is_empty(df):
    return df.first() is None
