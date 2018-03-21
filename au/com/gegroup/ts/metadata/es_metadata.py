__author__ = 'topsykretts'


class ESMetadata:
    """
    Singleton class that initializes JDBC metadata
    """
    __instance = None

    @staticmethod
    def getInstance(sqlContext, **kwargs):
        """ Static access method. """
        if ESMetadata.__instance is None:
            ESMetadata(sqlContext, **kwargs)
            return ESMetadata.__instance
        do_refresh = kwargs.get("meta.es.refresh")
        if do_refresh is not None and (
                    (isinstance(do_refresh, bool) and do_refresh) or (
                            isinstance(do_refresh, str) and do_refresh == "true")):
            ESMetadata.refresh(sqlContext, **kwargs)
        return ESMetadata.__instance

    def __init__(self, sqlContext, **kwargs):
        """ Virtually private constructor. """
        if ESMetadata.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            print("Initializing Elasticsearch Metadata.....")
            ESMetadata.refresh(sqlContext, **kwargs)

    @staticmethod
    def refresh(sqlContext, **kwargs):
        import os
        host = None
        port = None
        resource = None
        if kwargs is not None:
            host = kwargs.get("meta.es.nodes")
            port = kwargs.get("meta.es.port")
            resource = kwargs.get("meta.es.resource")
        if host is None:
            host = os.getenv("META_ES_NODES", "localhost")
        if port is None:
            port = os.getenv("META_ES_PORT", "9200")
        if resource is None:
            resource = os.getenv("META_ES_RESOURCE", "metadata/metadata")

        es_properties = {
            "es.nodes": host,
            "es.port": port,
            "es.resource": resource
        }
        metadata_df = sqlContext.read.format("org.elasticsearch.spark.sql").options(**es_properties).load()
        # metadata_df.cache()
        ESMetadata.__instance = metadata_df
