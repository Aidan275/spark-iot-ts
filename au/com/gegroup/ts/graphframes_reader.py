from au.com.gegroup.ts.metadata.graph_metadata import GraphMetadata
from au.com.gegroup.ts.reader import Reader
from pyspark.sql.functions import *
from pyspark.sql.types import *

__author__ = 'topsykretts'


class GraphFramesReader(Reader):

    def __init__(self, sqlContext, dataset, view_name, load_filter=None):
        super().__init__(sqlContext, dataset, view_name, load_filter)
        # Get a GraphFrame instance
        self.graph_df = GraphMetadata.getInstance(sqlContext)
        self._vertices = self.graph_df.vertices
        self._edges = self.graph_df.edges
        # other initializations
        self._ref_filter = None

    def metadata(self, metadata):
        """
        builder that initializes meta tags to be filtered.
        :param metadata: tags to be filtered like "supply and water and temp and sensor"
        :return: reader object with tag_filter initialized with pointNames to be filtered
        """
        tag_filters = GraphFramesReader._get_tags_filter(metadata)
        self._ref_filter = tag_filters['ref_filter']
        tags = tag_filters['meta_tags']

        # filter edges by meta tags
        def filter_by_tags(meta_data):
            return set(meta_data).issuperset(set(tags))

        filter_udf = udf(lambda meta_data: filter_by_tags(meta_data), BooleanType())

        val = self._edges.groupBy("src").agg(collect_set("relationship").alias("metadata"))
        filtered = val.filter(filter_udf(col("metadata"))).select("src")
        point_names = []
        for row in filtered.select("src").collect():
            point_names.append("pointName = '"+row[0]+"'")
        self._tag_filter = " or ".join(point_names)
        return self

    def _get_query(self):
        sql = super()._get_query()
        if self._ref_filter is not None and self._ref_filter != "":
            sql += " and "+self._ref_filter
        return sql
        # convert tag and reference filter to pointName filter

    @staticmethod
    def _get_tags_filter(metadata_str):
        tags = metadata_str.split("and")
        meta_tags = []
        ref_filter = []
        for tag in tags:
            if "=" in tag:
                ref_filter.append(tag.strip())
            else:
                meta_tags.append(tag.strip())
        return {'meta_tags': meta_tags, 'ref_filter': " and ".join(ref_filter)}
