__author__ = 'topsykretts'

from ts.flint import FlintContext

from au.com.gegroup.ts.datetime.utils import *
import pandas as pd
import datetime
from pyspark.sql.functions import broadcast


class Reader(object):
    """
    This class is abstraction for reading Flint TS dataframe based on metadata and date filter.
    A new instance of this class should be created for a pair of filo_dataset and load_filter.
    Eg: filo_dataset = 'iot_history_v0' and load_filter = "siteRef = 'Site'" creates reader for
    dataset 'iot_history_v0' for site 'Site'.
    Load filter is None by default.
    """

    def __init__(self, sqlContext, dataset, view_name, site_filter=None):
        """
        constructor for Reader object to read from filodb
        :param sqlContext: current spark's sqlContext
        :param dataset: the filodb dataset name or dataframe that should be loaded
        :param view_name: the name to temp table, that will be used in constructed queries
        :param site_filter: filter string to filter the dataset. Eg: 'siteRef == "Site"'
        :return: Reader object
        """
        self._sqlContext = sqlContext
        self._fc = FlintContext(self._sqlContext)
        self.view_name = view_name
        self.load_filter = site_filter
        self._date_filter = None
        self._tag_filter = None
        self._meta_filter = None
        self._is_sorted = True
        if isinstance(dataset, str):
            self.filodb_dataset = dataset
            df = self._sqlContext.read.format("filodb.spark").option("dataset", dataset).load()
        else:
            self.filodb_dataset = None
            df = dataset
        if site_filter is not None:
            df = df.filter(site_filter)
        self._df = df
        self._df.createOrReplaceTempView(self.view_name)
        self._timestamp = True
        self._tag_query = None
        self._metadata_df = None

    def has_timestamp(self, boolean):
        self._timestamp = boolean
        return self

    def query_metadata(self, query, debug=False, strict=False):
        pass

    def metadata(self, metadata):
        """
        builder that initializes meta tags to be filtered.
        :param metadata: tags to be filtered like "supply and water and temp and sensor"
        :return: reader object with tag_filter initialized
        """
        self._tag_filter = Reader._get_tags_filter(metadata)
        return self

    def history(self, date_tuple):
        """
        builder that initializes date_filter
        :param date_tuple: a tuple (from_date_in_long, to_date_in_long). unit is ms
        :return: reader object with date_filter initialized
        """
        if isinstance(date_tuple, str):
            date_tuple = Reader.process_string_date(date_tuple)
        date_tuple = as_long_tuple(date_tuple)
        self._date_filter = Reader._get_datetime_filter(date_tuple[0], date_tuple[1])
        return self

    @staticmethod
    def process_string_date(string_date):
        if string_date.startswith("past_"):
            delta_expr = string_date.replace("past_", "")
            return last(pd.Timedelta(delta_expr))
        elif string_date == "today":
            return today()
        elif string_date == "yesterday":
            return yesterday()
        elif string_date == "this_month":
            return this_month()
        elif string_date == "last_month":
            return last_month()
        else:
            try:
                if "," in string_date:
                    fr, to = string_date.split(",", 1)
                    fr = fr.strip()
                    to = to.strip()
                    if len(fr) == len("yyyy-MM-dd"):
                        from_date = datetime.datetime.strptime(fr, '%Y-%m-%d')
                    else:
                        from_date = datetime.datetime.strptime(fr, '%Y-%m-%dT%H:%M:%S')
                    if len(to) == len("yyyy-MM-dd"):
                        to_date = datetime.datetime.strptime(to, '%Y-%m-%d')
                    else:
                        to_date = datetime.datetime.strptime(to, '%Y-%m-%dT%H:%M:%S')
                    return from_date, to_date
                else:
                    to = string_date.strip()
                    if len(to) == len("yyyy-MM-dd"):
                        to_date = datetime.datetime.strptime(to, '%Y-%m-%d')
                    else:
                        to_date = datetime.datetime.strptime(to, '%Y-%m-%dT%H:%M:%S')
                    return None, to_date
            except:
                raise Exception("Date format should be one of following \n"
                                "%Y-%m-%d,%Y-%m-%d\n"
                                "%Y-%m-%dT%H:%M:%S,%Y-%m-%dT%H:%M:%S\n"
                                "%Y-%m-%d\n"
                                "%Y-%m-%dT%H:%M:%S")

    def is_sorted(self, is_sorted):
        """
        builder that sets is_sorted param
        :param is_sorted: Either True or False
        :return: reader object
        """
        self._is_sorted = is_sorted
        return self

    def read(self, key_cols=["siteRef", "equipRef"]):
        """
        Construct a query with date_filter, tags_filter and key and reads from temp view created
        :param key: key for the timeseries dataframe that will be used for joins
        :return: flint's Timeseries dataframe
        """
        # todo check direct join
        query = self._get_query()
        ref_cols = ["raw_id"]
        ref_cols.extend(key_cols)
        meta_df = self._metadata_df.where(self._meta_filter).select(ref_cols)
        his_df = self._fc.read.dataframe(self._sqlContext.sql(query), is_sorted=self._is_sorted, unit='ns')
        his_df = his_df.join(broadcast(meta_df), his_df.pointName == meta_df.raw_id, "left_outer").drop("raw_id")
        self._tag_filter = None
        self._meta_filter = None
        self._date_filter = None
        # print("query = ", query)
        return his_df

    def _get_query(self):
        """
        constructs sql query for getting histories based on meta tags and date filter
        :param key: for identifying timeseries and columns when joined
        :return: sql query to get the required timeseries
        """
        # if both None then it will be unbounded read
        if self._date_filter is None and self._tag_filter is None:
            raise Exception("Either datetime_filter or tags_filter must be present")
        # if both are present, then these should be connected by "and" in sql query to be constructed
        if self._date_filter is not None and self._tag_filter is not None:
            datetime_filter = self._date_filter + " and "
        elif self._date_filter is None:
            datetime_filter = ""
        else:
            datetime_filter = self._date_filter
        # if either datetime_filter or tags_filter is None, then replace it with empty string
        if self._tag_filter is None:
            tag_filter = ""
        else:
            tag_filter = self._tag_filter

        timestamp_select = "timestamp as timestamp,"
        if not self._timestamp:
            timestamp_select = ''

        select = "select %(timestamp)s datetime as time, pointName as pointName, value as value , unit as unit  from %(view)s" % \
                 ({'view': self.view_name, 'timestamp': timestamp_select})
        sql = "%(select)s  where %(date_filter)s  %(tags_filter)s" % (
            {'date_filter': datetime_filter, 'tags_filter': tag_filter, 'select': select})
        return sql

    def get_df(self):
        return self._df

    @staticmethod
    def _get_datetime_filter(from_date, to_date):
        date_filters = []
        if from_date is not None:
            date_filters.append("datetime >= %(from)d" % ({'from': from_date}))
        if to_date is not None:
            date_filters.append("datetime <= %(to)d" % ({'to': to_date}))
        return " and ".join(date_filters)

    @staticmethod
    def _get_tags_filter(metadata_str):
        tags = metadata_str.split("and")
        tag_filters = []
        for tag in tags:
            if "=" in tag:
                tag_filters.append(tag.strip())
            else:
                tag_filters.append(tag.strip() + " = '1'")
        return " and ".join(tag_filters)
