__author__ = 'topsykretts'


class ESMetadata:
    """
    Singleton class that initializes JDBC metadata
    """
    __instance = None

    @staticmethod
    def getInstance(sqlContext):
        """ Static access method. """
        if ESMetadata.__instance is None:
            ESMetadata(sqlContext)
        return ESMetadata.__instance

    def __init__(self, sqlContext):
        """ Virtually private constructor. """
        if ESMetadata.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            print("Initializing Elasticsearch Metadata.....")
            self.refresh(sqlContext)

    def refresh(self, sqlContext):
            import os
            host = os.getenv("META_ES_NODES", "localhost")
            port = os.getenv("META_ES_PORT", "9200")
            resource = os.getenv("META_ES_RESOURCE", "metadata/metadata")

            es_properties = {
                "es.nodes": host,
                "es.port": port,
                "es.resource": resource
            }
            metadata_df = sqlContext.read.format("org.elasticsearch.spark.sql").options(**es_properties).load()
            # metadata_df.cache()
            ESMetadata.__instance = metadata_df

