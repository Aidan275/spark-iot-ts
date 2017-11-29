__author__ = 'topsykretts'

from .utils import *
from ts.flint import FlintContext


class Reader(object):
    """
    This class is abstraction for reading Flint TS dataframe based on metadata and date filter.
    It assumes that iotDF is created as temp view or table in current sqlContext.
    Need to work around the way to pass sqlContext and register iotDF (i.e. integrate filodb in this class)
    """
    # todo work on how to pass context in optimized way
    def __init__(self, sqlContext):
        self._sqlContext = sqlContext
        self._fc = FlintContext(self._sqlContext)

    def metadata(self, metadata):
        self.tag_filter = get_tags_filter(metadata)
        return self

    def history(self, date_tuple):
        self.date_filter = get_datetime_filter(date_tuple[0], date_tuple[1])
        return self

    def read(self, key):
        query = get_query(self.date_filter, self.tag_filter, key)
        return self._fc.read.dataframe(self._sqlContext.sql(query), is_sorted=True, unit='ms')


