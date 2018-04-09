__author__ = 'topsykretts'

import pandas as pd


def as_long_tuple(date_tuple, unit='ns'):
    if isinstance(date_tuple[0], int) and isinstance(date_tuple[1], int):
        return date_tuple
    long_tuple = (date_to_long(date_tuple[0], unit), date_to_long(date_tuple[1], unit))
    return long_tuple


def date_to_long(date_string, unit='ns'):
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


def last(delta):
    now = pd.datetime.now()
    past = now - delta
    date_tuple = (past, now)
    return date_tuple


def yesterday():
    one_day = pd.Timedelta('1 days')
    today = pd.datetime.now()
    yest = today - one_day
    start = pd.datetime(yest.year, month=yest.month, day=yest.day)
    date_tuple = (start, get_day_end(start))
    return date_tuple


def today():
    now = pd.datetime.now()
    start = pd.datetime(now.year, month=now.month, day=now.day)
    end = get_day_end(now)
    date_tuple = (start, end)
    return date_tuple


def get_day_end(date):
    start = pd.datetime(date.year, month=date.month, day=date.day)
    end = start + pd.Timedelta('1 days') + pd.Timedelta('-1us')
    return end


def this_month():
    today = pd.datetime.now()
    month_start = pd.datetime(today.year, month=today.month, day=1)
    month_end = pd.datetime(today.year, month=today.month+1, day=1) - pd.Timedelta('1 days')
    month_end = get_day_end(month_end)
    date_tuple = (month_start, month_end)
    return date_tuple


def last_month():
    today = pd.datetime.now()
    month_start = pd.datetime(today.year, month=today.month-1, day=1)
    month_end = pd.datetime(today.year, month=today.month, day=1) - pd.Timedelta('1 days')
    month_end = get_day_end(month_end)
    date_tuple = (month_start, month_end)
    return date_tuple
