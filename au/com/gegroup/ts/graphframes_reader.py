from au.com.gegroup.ts.reader import Reader
from graphframes import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

__author__ = 'topsykretts'


class GraphFramesReader(Reader):

    def __init__(self, sqlContext, dataset, view_name, load_filter=None):
        super().__init__(sqlContext, dataset, view_name, load_filter)
        # todo read this from config
        vertices_raw = sqlContext.read.csv("timeseries/graph/metadata/vertices.csv", header=True)
        edges_raw = sqlContext.read.csv("timeseries/graph/metadata/edges.csv", header=True)
        # todo singleton GraphFrame object
        # Create a GraphFrame
        self.graph_df = GraphFrame(vertices_raw, edges_raw)
        self._vertices = self.graph_df.vertices
        self._edges = self.graph_df.edges
        self._vertices.cache()
        self._edges.cache()
        # other initializations
        self._ref_filter = None

    def metadata(self, metadata):
        """
        builder that initializes meta tags to be filtered.
        :param metadata: tags to be filtered like "supply and water and temp and sensor"
        :return: reader object with tag_filter initialized with pointNames to be filtered
        """
        tag_filters = Reader._get_tags_filter(metadata)
        self._ref_filter = tag_filters['ref_filter']
        tags = tag_filters['meta_tags']

        # filter edges by meta tags
        def filter_by_tags(meta_data):
            return meta_data.issuperset(set(tags))

        filter_udf = udf(lambda meta_data: filter_by_tags(meta_data), BooleanType())

        val = self._edges.groupBy("src").agg(collect_set("relationship").alias("metadata"))
        filtered = val.filter(filter_udf(col("metadata"))).select("src")
        # filter by reference filter if any
        if self._ref_filter is not None and len(self._ref_filter) > 0:
            joined = filtered.join(self._vertices, filtered.src == self._vertices.id)
            joined_filtered = joined.filter(self._ref_filter)
        else:
            joined_filtered = filtered
        # convert tag and reference filter to pointName filter
        point_names = []
        for row in joined_filtered.select("src").collect():
            point_names.append("pointName = "+row[0])
        self._tag_filter = "and".join(point_names)
        print(self._tag_filter)
        return self

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
