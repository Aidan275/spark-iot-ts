import unittest
import json

from nubespark.haystack import trio_to_json
from nubespark.tests.base.base_test_case import BaseTestCase

__author__ = 'topsykretts'


def prettyPrint(json_str):
    json_val = json.loads(json_str)
    print(json.dumps(json_val, indent=3, ensure_ascii=False))


class HaystackTrioToJsonTest(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_markers(self):
        trio_string = """
        his
        point
        air
        temp
        supply
        """
        json_str = trio_to_json(trio_string)
        prettyPrint(json_str)

    def test_marker_na_null_str_ref(self):
        trio_string = """
        dummy: N
        weatherInfo: NA
        his
        point
        air
        supply
        temp
        equipRef: @123456abc
        siteRef: @site1
        kind: "Number"
        axSlotPath: "C.Niagara:Equip:AHU:Point"
        """
        json_str = trio_to_json(trio_string)
        prettyPrint(json_str)

    def test_numeric(self):
        trio_string = """
        infinity: INF
        negInfinity: -INF
        notANumber: NaN
        integer: 123
        integerWithUnit: 45%
        float: 34.56
        floatWithUnit: 456.34Kw
        expNum: 5.4e-45
        expNumWithUnit: 6.4e10cm^3
        """
        json_str = trio_to_json(trio_string)
        prettyPrint(json_str)

    def test_separator(self):
        trio_string = 'supply,air,temp,kind:"Number",equipRef:@123abc456,precision:1.0'
        json_str = trio_to_json(trio_string, separator=",")
        prettyPrint(json_str)

if __name__ == '__main__':
    unittest.main()
