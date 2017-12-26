from au.com.gegroup.ts.reader import Reader

__author__ = 'topsykretts'


class IOTHistoryReader(Reader):
    """
    This class is abstraction for reading Flint TS dataframe based on metadata and date filter.
    A new instance of this class should be created for a pair of filo_dataset and load_filter.
    Eg: filo_dataset = 'iot_history_v0' and load_filter = "siteRef = 'Site'" creates reader for
    dataset 'iot_history_v0' for site 'Site'.
    Load filter is None by default.
    """

    def __init__(self, sqlContext, dataset, view_name, load_filter=None):
        """
        constructor for Reader object to read from filodb
        :param sqlContext: current spark's sqlContext
        :param dataset: the filodb dataset name or dataframe that should be loaded
        :param view_name: the name to temp table, that will be used in constructed queries
        :param load_filter: filter string to filter the dataset. Eg: "siteRef = 'Site'"
        :return: Reader object
        """
        super().__init__(sqlContext, dataset, view_name, load_filter)

        # metadata_dataset = os.getenv("FILODB_METADATA_DATASET", "points_metadata_v0")
        # self._metadata_df = sqlContext.read.format("filodb.spark").option("dataset", metadata_dataset).load()
        self._set_metadata()
        self._ref_filter = None

    def _set_metadata(self):
        import os
        user = os.getenv("META_JDBC_USER", "hive")
        password = os.getenv("META_JDBC_PASSWORD", "hive")
        host = os.getenv("META_JDBC_HOST", "localhost")
        port = os.getenv("META_JDBC_PORT", "3306")
        database = os.getenv("META_JDBC_DATABASE", "gegroup")
        table = os.getenv("META_JDBC_TABLE", "metadata")
        jdbc_url = "jdbc:mysql://%(host)s:%(port)s/%(database)s" % ({"host": host, "port": port, "database": database})
        database_properties = {
            "driver": "com.mysql.jdbc.Driver",
            "user": user,
            "password": password,
            "rewriteBatchedStatements": "true"
        }
        self._metadata_df = self._sqlContext.read.jdbc(url=jdbc_url, table=table, properties=database_properties)
        self._metadata_df.cache()
        self._metadata_df.createOrReplaceTempView("metadata")

    def metadata(self, metadata):
        """
        builder that initializes meta tags to be filtered.
        :param metadata: tags to be filtered like "supply and water and temp and sensor"
        :return: reader object with tag_filter initialized
        """
        tag_filters = IOTHistoryReader._get_tags_filter(metadata)
        # self._tag_filter = " select dis from metadata where " + tag_filter
        self._ref_filter = tag_filters['ref_filter']
        tags = tag_filters['meta_tags']
        tag_query = " select dis from metadata where " + tags
        rows = self._sqlContext.sql(tag_query).collect()
        point_names = []
        # for row in rows:
        #     point_names.append("pointName = '"+row[0]+"'")
        # self._tag_filter = "(" + " or ".join(point_names) + ")"

        for row in rows:
            point_names.append("'" + row[0] + "'")
        self._tag_filter = "pointName in (" + ",".join(point_names) + ")"
        return self

    def _get_query(self):
        """
        constructs sql query for getting histories based on meta tags and date filter
        :param key: for identifying timeseries and columns when joined
        :return: sql query to get the required timeseries
        """
        sql = super()._get_query()
        if self._ref_filter is not None and self._ref_filter != "":
            sql += " and " + self._ref_filter
        print("Query = ")
        print(sql)
        return sql

    def get_metadata_df(self):
        return self._metadata_df

    @staticmethod
    def _get_tags_filter(metadata_str):
        tags = metadata_str.split("and")
        meta_tags = []
        ref_filter = []
        for tag in tags:
            if "=" in tag:
                ref_filter.append(tag.strip())
            else:
                meta_tags.append(tag.strip() + " = '1'")
        return {'meta_tags': "and ".join(meta_tags), 'ref_filter': " and ".join(ref_filter)}
