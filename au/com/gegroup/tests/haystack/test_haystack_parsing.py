import unittest

from au.com.gegroup.tests.base.base_test_case import BaseTestCase

__author__ = 'topsykretts'


class HaystackParsingTest(BaseTestCase):
    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        delattr(cls, "parser")

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from au.com.gegroup.haystack import HaystackToSQL
        cls.parser = HaystackToSQL()

    def test_parsing_simple_marker(self):
        """
        Test different queries...
        :return:
        """
        tag_haystack_query = "supply and air and temp and sensor"
        tag_sql_result = self.parser.parse(tag_haystack_query)[0]
        print(tag_sql_result)
        assert "supply = 'm:' and air = 'm:' and temp = 'm:' and sensor = 'm:'" == tag_sql_result

    def test_parsing_markers_with_parens_and_not(self):
        tag_haystack_query = "supply and (air or pressure) and temp and not sensor"
        tag_sql_result = self.parser.parse(tag_haystack_query)[0]
        assert "supply = 'm:' and ( air = 'm:' or pressure = 'm:' ) and temp = 'm:' and sensor is null" == tag_sql_result

    def test_parsing_tag_and_non_tag_query(self):
        haystack_query = "return and water and temp and not sensor and equipRef==\"equip123\""
        sql_result = self.parser.parse(haystack_query)[0]
        assert "return = 'm:' and water = 'm:' and temp = 'm:' and sensor is null and equipRef = 'equip123'" == sql_result

    def test_nested_parenthesis(self):
        haystack_query = '(return and (temp or otherVal == "abc" )) or (supply and pressure)'
        sql_result = self.parser.parse(haystack_query)[0]
        assert "( return = 'm:' and ( temp = 'm:' or otherVal = 'abc' ) ) or ( supply = 'm:' and pressure = 'm:' )" == \
               sql_result

    def test_non_tag_only(self):
        haystack_query = 'siteRef == "site1"'
        sql_result = self.parser.parse(haystack_query)[0]
        print(sql_result)
        assert "siteRef = 'site1'" == sql_result

    def test_numeric_comp(self):
        haystack_query = "val == 120 and temperature == 34.54 and rate >= 30 and rate <= 67.56"
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert "val_num_ = 120 and temperature_num_ = 34.54 and rate_num_ >= 30 and rate_num_ <= 67.56" == sql_result[0]

    def test_bool_match(self):
        haystack_query = 'feature == true'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert "feature_bool_ = 1" == sql_result[0]

    def test_ref_only(self):
        haystack_query = 'siteRef == @a12345fD'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert "(siteRef = 'r:a12345fD' or siteRef LIKE 'r:a12345fD %')" == sql_result[0]

    def test_ref_with_desc(self):
        haystack_query = '(boilerPlantRef == @12345f-67890D "56 Pitt St Boiler Plant") and (air or water)'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert "( (boilerPlantRef = 'r:12345f-67890D' or" \
               " boilerPlantRef LIKE 'r:12345f-67890D %') ) and ( air = 'm:' or water = 'm:' )" == sql_result[0]

    def test_nested_ref(self):
        haystack_query = '(equipRef->boiler and equipRef->capacity == 100' \
                         ' and water) or (equipRef->fan and equipRef->equip)'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        print(set(sql_result[1]))
        assert "( water = 'm:' and equipRef in (select equipRef from metadata where" \
               " boiler = 'm:' and capacity_num_ = 100) ) or" \
               " ( equipRef in (select equipRef from metadata where fan = 'm:' and equip = 'm:') )" == sql_result[0]

    def test_nested_ref_mix(self):
        haystack_query = 'equipRef->boiler and equipRef->capacity == 100 and siteRef->boiler and siteRef->air'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        print(sql_result[1])
        assert "siteRef in (select siteRef from metadata where boiler = 'm:' and air = 'm:') and " \
               "equipRef in (select equipRef from metadata where boiler = 'm:' and capacity_num_ = 100)" == sql_result[0] or \
               "equipRef in (select equipRef from metadata where boiler = 'm:' and capacity_num_ = 100) and" \
               " siteRef in (select siteRef from metadata where boiler = 'm:' and air = 'm:')" == sql_result[0]

    def test_null_query(self):
        haystack_query = "air and temp and return and sensor == null"
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert "air = 'm:' and temp = 'm:' and return = 'm:' and sensor is null" == sql_result[0]

    def test_not_null_query(self):
        haystack_query = "air and temp and return and sensor != null"
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert "air = 'm:' and temp = 'm:' and return = 'm:' and sensor is not null" == sql_result[0]


if __name__ == '__main__':
    unittest.main()
