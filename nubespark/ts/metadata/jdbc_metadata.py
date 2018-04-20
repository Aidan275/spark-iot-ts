__author__ = 'topsykretts'


class JDBCMetadata:
    """
    Singleton class that initializes JDBC metadata
    """
# Here will be the instance stored.
    __instance = None

    @staticmethod
    def getInstance(sqlContext):
        """ Static access method. """
        if JDBCMetadata.__instance is None:
            JDBCMetadata(sqlContext)
        return JDBCMetadata.__instance

    def __init__(self, sqlContext):
        """ Virtually private constructor. """
        if JDBCMetadata.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            print("Initializing JDBCMetadata.....")
            self.refresh(sqlContext)

    def refresh(self, sqlContext):
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
            metadata_df = sqlContext.read.jdbc(url=jdbc_url, table=table, properties=database_properties)
            JDBCMetadata.__instance = metadata_df

