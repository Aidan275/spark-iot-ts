import unittest
from au.com.gegroup.tests.base.spark_test_case import SparkTestCase
from au.com.gegroup.ts.iot_his_reader import IOTHistoryReader
from au.com.gegroup.ts.spark.utils import *

__author__ = 'topsykretts'


class UnionIntersectionTest(SparkTestCase):
    """
    Test cases to check union and intersection between sts and enb or sts/enb and others
    """

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def test_intersection(self):
        schema = ["datetime", "siteRef", "levelRef", "equipName", "pointName", "value"]
        ts = [
            (1000, "site1", "level1", "Site ACU 1", "ACU-1_ENB", 0.0),
            # (1000, "site1", "level1", "Site ACU 1", "ACU-1_STS", 1.0),
            (1005, "site1", "level1", "Site ACU 1", "ACU-1_ENB", 1.0),
            (1010, "site1", "level1", "Site ACU 1", "ACU-1_ENB", 0.0),
            (1001, "site1", "level1", "Site ACU 1", "ACU-1_STS", 0.0),
            (1006, "site1", "level1", "Site ACU 1", "ACU-1_STS", 1.0),
            (1011, "site1", "level1", "Site ACU 1", "ACU-1_STS", 0.0)
        ]
        input_df = self.sqlContext.createDataFrame(ts, schema)
        reader = IOTHistoryReader(self.sqlContext, dataset=input_df, view_name="test_iot_hybrid_union_inter_1")
        reader.has_timestamp(False)
        # read enb and sts history
        acu1_enb = reader.metadata("run and cmd and fan and equipRef == \"Site ACU 1\"").read()
        acu1_sts = reader.metadata("run and sensor and fan and equipRef == \"Site ACU 1\"").read()

        acu1_enb.show()
        acu1_sts.show()

        # join enb and sts
        merged_df = join_bool(acu1_enb, acu1_sts, left_alias="enb", right_alias="sts")
        print("Merged DF")
        merged_df.show()
        intersection_df = intersection_bool(merged_df, "enb", "sts")
        print("Intersection DF")
        intersection_df.show()

    def test_union(self):
        schema = ["datetime", "siteRef", "levelRef", "equipName", "pointName", "value"]
        ts = [
            (1000, "site1", "level1", "Site ACU 1", "ACU-1_ENB", 0.0),
            # (1000, "site1", "level1", "Site ACU 1", "ACU-1_STS", 1.0),
            (1005, "site1", "level1", "Site ACU 1", "ACU-1_ENB", 1.0),
            (1010, "site1", "level1", "Site ACU 1", "ACU-1_ENB", 0.0),
            (1001, "site1", "level1", "Site ACU 1", "ACU-1_STS", 0.0),
            (1006, "site1", "level1", "Site ACU 1", "ACU-1_STS", 1.0),
            (1011, "site1", "level1", "Site ACU 1", "ACU-1_STS", 0.0)
        ]
        input_df = self.sqlContext.createDataFrame(ts, schema)
        reader = IOTHistoryReader(self.sqlContext, dataset=input_df, view_name="test_iot_hybrid_union_inter_2")
        reader.has_timestamp(False)
        # read enb and sts history
        acu1_enb = reader.metadata("run and cmd and fan and equipRef == \"Site ACU 1\"").read()
        acu1_sts = reader.metadata("run and sensor and fan and equipRef == \"Site ACU 1\"").read()

        acu1_enb.show()
        acu1_sts.show()

        # join enb and sts
        merged_df = join_bool(acu1_enb, acu1_sts, left_alias="enb", right_alias="sts")
        print("Merged DF")
        merged_df.show()
        union_df = union_bool(merged_df, "enb", "sts")
        print("Union DF")
        union_df.show()

    def test_multiple_intersection(self):
        schema = ["datetime", "siteRef", "levelRef", "equipName", "pointName", "value"]
        ts = [
            (1000, "site1", "level1", "Site ACU 1", "ACU-1_ENB", 0.0),
            (1000, "site1", "level1", "Site ACU 2", "ACU-2_ENB", 0.0),
            (1001, "site1", "level1", "Site ACU 1", "ACU-1_STS", 0.0),
            (1001, "site1", "level1", "Site ACU 2", "ACU-2_STS", 0.0),
            (1005, "site1", "level1", "Site ACU 1", "ACU-1_ENB", 1.0),
            (1005, "site1", "level1", "Site ACU 2", "ACU-2_ENB", 1.0),
            (1006, "site1", "level1", "Site ACU 1", "ACU-1_STS", 1.0),
            (1006, "site1", "level1", "Site ACU 2", "ACU-2_STS", 1.0),
            (1010, "site1", "level1", "Site ACU 1", "ACU-1_ENB", 0.0),
            (1010, "site1", "level1", "Site ACU 2", "ACU-2_ENB", 0.0),
            (1011, "site1", "level1", "Site ACU 1", "ACU-1_STS", 0.0),
            (1011, "site1", "level1", "Site ACU 2", "ACU-2_STS", 0.0)
        ]
        input_df = self.sqlContext.createDataFrame(ts, schema)
        reader = IOTHistoryReader(self.sqlContext, dataset=input_df, view_name="test_iot_hybrid_union_inter_1")
        reader.has_timestamp(False)
        # read enb and sts history
        acu_enb = reader.metadata("run and cmd and fan").read()
        acu_sts = reader.metadata("run and sensor and fan").read()
        acu_enb.show()
        acu_sts.show()
        merged_df = join_bool(acu_enb, acu_sts, "enb", "sts")
        print("Merged DF")
        merged_df.show()
        intersection_df = intersection_bool(merged_df, "enb", "sts")
        intersection_df.show()


if __name__ == '__main__':
    unittest.main()
