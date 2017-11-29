__author__ = 'topsykretts'

import pandas as pd


def date_to_long(date_string, unit='ms'):
    """
    converts date string provided in pandas datetime64 recognizable format to long, depending on unit provided
    :param date_string: date string in format recognizable by pandas datetime64
    :param unit: one of 'ns', 'us', 'ms', 's' i.e. nanoseconds, microseconds, milliseconds, seconds respectively.
    Default is milliseconds
    :return: date in long/int64 format
    """
    date_ts = pd.to_datetime(date_string)
    date_val = date_ts.value
    if 'ns' == unit.lower():
        return date_val
    elif 'us' == unit.lower():
        return int(date_val / 1000)
    elif 'ms' == unit.lower():
        return int(date_val / 1000000)
    elif 's' == unit.lower():
        return int(date_val / 1000000000)


def get_query(view_name, datetime_filter, tags_filter, key):
    """
    constructs sql query for getting histories based on meta tags and date filter
    :param view_name: temp sql table that is created using "df.createOrReplaceTempView(view_name)"
    :param datetime_filter: date_filter constructed with from_date and to_date
    :param tags_filter: tags_filter constructed with metadata
    :param key: for identifying timeseries and columns when joined
    :return: sql query to get the required timeseries
    """
    # if both None then it will be unbounded read
    if datetime_filter is None and tags_filter is None:
        raise Exception("Either datetime_filter or tags_filter must be present")
    # if both are present, then these should be connected by "and" in sql query to be constructed
    if datetime_filter is not None and tags_filter is not None:
        datetime_filter += " and "
    elif datetime_filter is None:
        datetime_filter = ""
    # if either datetime_filter or tags_filter is None, then replace it with empty string
    if tags_filter is None:
        tags_filter = ""

    select = "select datetime as time, value as %(key)s_value , pointName as %(key)s_pointName, equipRef as equipRef," \
             " levelRef as %(key)s_levelRef, siteRef as %(key)s_siteRef from %(view)s" % \
             ({'key': key, 'view': view_name})
    sql = "%(select)s  where %(date_filter)s  %(tags_filter)s" % (
        {'date_filter': datetime_filter, 'tags_filter': tags_filter, 'select': select})
    return sql


def get_datetime_filter(from_date, to_date):
    date_filters = []
    if from_date is not None:
        date_filters.append("datetime >= %(from)d" % ({'from': from_date}))
    if to_date is not None:
        date_filters.append("datetime <= %(to)d" % ({'to': to_date}))
    return " and ".join(date_filters)


def get_tags_filter(metadata_str):
    tags = metadata_str.split("and")
    tag_filters = []
    for tag in tags:
        if "=" in tag:
            tag_filters.append(tag.strip())
        else:
            tag_filters.append(tag.strip() + " = '1'")
    return " and ".join(tag_filters)

