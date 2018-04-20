from graphframes import GraphFrame

__author__ = 'topsykretts'


class GraphMetadata:
    # Here will be the instance stored.
    __instance = None

    @staticmethod
    def getInstance(sqlContext):
        """ Static access method. """
        if GraphMetadata.__instance is None:
            GraphMetadata(sqlContext)
        return GraphMetadata.__instance

    def __init__(self, sqlContext):
        """ Virtually private constructor. """
        if GraphMetadata.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            import os
            vertices_path = os.getenv('META_VERTICES_PATH', "timeseries/graph/metadata/vertices.csv")
            edges_path = os.getenv('META_EDGES_PATH', "timeseries/graph/metadata/edges.csv")

            vertices_raw = sqlContext.read.csv(vertices_path, header=True)
            edges_raw = sqlContext.read.csv(edges_path, header=True)
            # Create a GraphFrame
            graph_df = GraphFrame(vertices_raw, edges_raw)
            graph_df.vertices.cache()
            graph_df.edges.cache()
            GraphMetadata.__instance = graph_df

