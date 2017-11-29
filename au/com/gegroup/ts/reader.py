__author__ = 'topsykretts'

from ts.flint import FlintContext

from au.com.gegroup.ts.datetime.utils import as_long_tuple


class Reader(object):
    """
    This class is abstraction for reading Flint TS dataframe based on metadata and date filter.
    A new instance of this class should be created for a pair of filo_dataset and load_filter.
    Eg: filo_dataset = 'iot_history_v0' and load_filter = "siteRef = 'Site'" creates reader for
    dataset 'iot_history_v0' for site 'Site'.
    Load filter is None by default.
    """
    def __init__(self, sqlContext, filodb_dataset, view_name, load_filter=None):
        """
        constructor for Reader object
        :param sqlContext: current spark's sqlContext
        :param filodb_dataset: the filodb dataset that should be loaded
        :param view_name: the name to temp table, that will be used in constructed queries
        :param load_filter: filter string to filter the dataset. Eg: "siteRef = 'Site'"
        :return: Reader object
        """
        self._sqlContext = sqlContext
        self._fc = FlintContext(self._sqlContext)
        self.view_name = view_name
        self.filodb_dataset = filodb_dataset
        self.load_filter = load_filter
        df = self._sqlContext.read.format("filodb.spark").option("dataset", filodb_dataset).load()
        if load_filter is not None:
            df = df.filter(load_filter)
        df.createOrReplaceTempView(self.view_name)

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
        date_tuple = as_long_tuple(date_tuple)
        self._date_filter = Reader._get_datetime_filter(date_tuple[0], date_tuple[1])
        return self

    def read(self, key):
        """
        Construct a query with date_filter, tags_filter and key and reads from temp view created
        :param key: key for the timeseries dataframe that will be used for joins
        :return: flint's Timeseries dataframe
        """
        query = self._get_query(key)
        return self._fc.read.dataframe(self._sqlContext.sql(query), is_sorted=True, unit='ms')

    def _get_query(self, key):
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
            self._date_filter += " and "
        elif self._date_filter is None:
            self._date_filter = ""
        # if either datetime_filter or tags_filter is None, then replace it with empty string
        if self._tag_filter is None:
            self._tag_filter = ""

        select = "select datetime as time, value as %(key)s_value , pointName as %(key)s_pointName, equipRef as equipRef," \
                 " levelRef as %(key)s_levelRef, siteRef as %(key)s_siteRef from %(view)s" % \
                 ({'key': key, 'view': self.view_name})
        sql = "%(select)s  where %(date_filter)s  %(tags_filter)s" % (
            {'date_filter': self._date_filter, 'tags_filter': self._tag_filter, 'select': select})
        return sql

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

