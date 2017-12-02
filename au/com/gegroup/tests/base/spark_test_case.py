from au.com.gegroup.tests.base.base_test_case import BaseTestCase

__author__ = 'topsykretts'

import unittest


class SparkTestCase(BaseTestCase):
    """
    Spark Test Case class that initializes spark session.
    """
    @classmethod
    def tearDownClass(cls):
        """

        :param cls:
        :return:
        """
        cls.__teardown()

    @classmethod
    def setUpClass(cls):
        """

        :param cls:
        :return:
        """
        cls.__setup()

    @classmethod
    def __setup(cls, options=None):
        """Starts spark and sets attributes `sc,sqlContext and flintContext"""
        from pyspark.sql import SparkSession, SQLContext
        spark_session = SparkSession.builder.appName("test").master("local") \
            .config("spark.filodb.store", "in-memory") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()

        cls.sparkSession = spark_session
        cls.sc = spark_session.sparkContext
        cls.sqlContext = SQLContext(cls.sc)

    @classmethod
    def __teardown(cls):
        cls.sc.stop()
        cls.sc._gateway.shutdown()
        cls.sc._gateway = None
        delattr(cls, 'sparkSession')
        delattr(cls, 'sqlContext')
        delattr(cls, 'sc')
