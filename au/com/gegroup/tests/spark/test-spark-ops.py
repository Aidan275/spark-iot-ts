import unittest
from au.com.gegroup.tests.base.spark_test_case import SparkTestCase
from au.com.gegroup.ts.reader import Reader
from au.com.gegroup.ts.spark.utils import join_keys

__author__ = 'topsykretts'


class SparkOperationsTest(SparkTestCase):
    """
    Test spark operations like join and aggregations
    """
    def test_multiple_joins(self):
        """
        testing more than one join
        :return:
        """
        schema = ["datetime", "siteRef", "levelRef", "equipRef", "pointName", "value",
                  "supply", "return", "sp", "air", "sensor"]
        ts = [
            (1000, "site1", "level1", "Site ACU 1", "ACU-1_SAT", 23.52, "1", None, None, "1", "1"),
            (1001, "site1", "level1", "Site ACU 1", "ACU-1_RAT", 20.52, None, "1", None, "1", "1"),
            (1002, "site1", "level1", "Site ACU 1", "ACU-1_SAT", 21.52, "1", None, None, "1", "1"),
            (1003, "site1", "level1", "Site ACU 1", "ACU-1_RAT", 18.52, None, "1", None, "1", "1"),
            (1000, "site1", "level1", "Site ACU 1", "ACU-1_SATSP", 15.6, "1", None, "1", "1", None),
            (1003, "site1", "level1", "Site ACU 1", "ACU-1_SATSP", 16.7, "1", None, "1", "1", None)
        ]
        input_df = self.sqlContext.createDataFrame(ts, schema)
        reader = Reader(self.sqlContext, input_df, "test_iot_sot_2")

        sat_ts = reader.metadata("supply and air and sensor").read()
        rat_ts = reader.metadata("return and air and sensor").read()
        satsp_ts = reader.metadata("sp and air").read()

        joined_ts1 = rat_ts.\
            leftJoin(sat_ts, tolerance="1M", key=join_keys(), left_alias="rat",
                                     right_alias="sat").\
            leftJoin(satsp_ts, tolerance="1M", key=join_keys(), right_alias="satsp")

        joined_ts1.show()

if __name__ == '__main__':
    unittest.main()
