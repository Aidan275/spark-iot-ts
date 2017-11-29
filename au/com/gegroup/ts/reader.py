__author__ = 'topsykretts'

from .utils import *
from ts.flint import FlintContext


class Reader(object):
    """
    This class is abstraction for reading Flint TS dataframe based on metadata and date filter.
    A new instance of this class should be created for a pair of filo_dataset and load_filter.
    Eg: filo_dataset = 'iot_history_v0' and load_filter = "siteRef = 'Site'" creates reader for
    dataset 'iot_history_v0' for site 'Site'.
    Load filter is None by default.
    """
    # todo work on how to pass context in optimized way
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
        self._tag_filter = get_tags_filter(metadata)
        return self

    def history(self, date_tuple):
        """
        builder that initializes date_filter
        :param date_tuple: a tuple (from_date_in_long, to_date_in_long). unit is ms
        :return: reader object with date_filter initialized
        """
        self._date_filter = get_datetime_filter(date_tuple[0], date_tuple[1])
        return self

    def read(self, key):
        """
        Construct a query with date_filter, tags_filter and key and reads from temp view created
        :param key: key for the timeseries dataframe that will be used for joins
        :return: flint's Timeseries dataframe
        """
        query = get_query(self.view_name, self._date_filter, self._tag_filter, key)
        return self._fc.read.dataframe(self._sqlContext.sql(query), is_sorted=True, unit='ms')
