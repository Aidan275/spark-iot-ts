__author__ = 'topsykretts'


class Writer(object):
    """
    This class provides abstraction for writing dataframes to filoDB database built on top of cassandra.
    """

    def __init__(self, dataframe, dataset, row_keys):
        """
        Initialize the writer object
        :param dataframe The dataframe to write
        :param dataset: The name of filoDB dataset where the dataframe gets written
        :param row_keys: unique key to identify each row
        :return:
        """
        self._df = dataframe
        self._dataset = dataset
        self._row_keys = row_keys
        # default values
        self._mode = "overwrite"
        self._chunk_size = "100"
        self._partition_keys = ":string %(dataset)s" % ({"dataset": dataset})

    def cols(self, cols):
        """
        Select only some columns from dataframe
        :param cols: A list of column names
        :return:
        """
        self._df = self._df.select(cols)
        return self

    def mode(self, mode):
        """
        Set the write mode
        :param mode: overwrite or append
        :return:
        """
        self._mode = mode
        return self

    def partition_keys(self, partition_keys):
        """
        Set the partition keys for dataframe in filoDB
        :param partition_keys: an filoDB partition_keys expression or column name or combinations.
        :return:
        """
        self._partition_keys = partition_keys
        return self

    def chunk_size(self, chunk_size):
        """
        Set the chunk_size for the partition. See, filoDB documentation for deciding chunk size.
        :param chunk_size: numeric string value.
        :return:
        """
        self._chunk_size = chunk_size
        return self

    def write(self):
        """
        initiate write process
        :return:
        """
        self._df.write.format("filodb.spark") \
            .option("dataset", self._dataset) \
            .option("partition_keys", self._partition_keys) \
            .option("row_keys", self._row_keys) \
            .option("chunk_size", self._chunk_size) \
            .mode(self._mode) \
            .save()
