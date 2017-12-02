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
        self.df = dataframe
        self.dataset = dataset
        self.row_keys = row_keys
        # default values
        self.mode = "overwrite"
        self.chunk_size = "100"
        self.partition_keys = ":string %(dataset)s" % ({"dataset": dataset})

    def cols(self, cols):
        """
        Select only some columns from dataframe
        :param cols: A list of column names
        :return:
        """
        self.df = self.df.select(cols)
        return self

    def mode(self, mode):
        """
        Set the write mode
        :param mode: overwrite or append
        :return:
        """
        self.mode = mode
        return self

    def partition_keys(self, partition_keys):
        """
        Set the partition keys for dataframe in filoDB
        :param partition_keys: an filoDB partition_keys expression or column name or combinations.
        :return:
        """
        self.partition_keys = partition_keys
        return self

    def chunk_size(self, chunk_size):
        """
        Set the chunk_size for the partition. See, filoDB documentation for deciding chunk size.
        :param chunk_size: numeric string value.
        :return:
        """
        self.chunk_size = chunk_size
        return self

    def write(self):
        """
        initiate write process
        :return:
        """
        self.df.write.format("filodb.spark") \
            .option("dataset", self.dataset) \
            .option("partition_keys", self.partition_keys) \
            .option("row_keys", self.row_keys) \
            .option("chunk_size", self.chunk_size) \
            .mode(self.mode) \
            .save()
