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
        assert "supply = 1 and air = 1 and temp = 1 and sensor = 1" == tag_sql_result

    def test_parsing_markers_with_parens_and_not(self):
        tag_haystack_query = "supply and (air or pressure) and temp and not sensor"
        tag_sql_result = self.parser.parse(tag_haystack_query)[0]
        assert "supply = 1 and ( air = 1 or pressure = 1 ) and temp = 1 and sensor != 1" == tag_sql_result

    def test_parsing_tag_and_non_tag_query(self):
        haystack_query = "return and water and temp and not sensor and equipRef==\"equip123\""
        sql_result = self.parser.parse(haystack_query)[0]
        assert "return = 1 and water = 1 and temp = 1 and sensor != 1 and equipRef = 'equip123'" == sql_result

    def test_nested_parenthesis(self):
        haystack_query = '(return and (temp or otherVal == "abc" )) or (supply and pressure)'
        sql_result = self.parser.parse(haystack_query)[0]
        assert "( return = 1 and ( temp = 1 or otherVal = 'abc' ) ) or ( supply = 1 and pressure = 1 )" == \
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
        assert "val = 120 and temperature = 34.54 and rate >= 30 and rate <= 67.56" == sql_result[0]

    def test_bool_match(self):
        haystack_query = 'feature == true'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert "feature = 1" == sql_result[0]

    def test_ref_only(self):
        haystack_query = 'siteRef == @a12345fD'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert "siteRef = 'a12345fD'" == sql_result[0]

    def test_ref_with_desc(self):
        haystack_query = 'boilerPlantRef == @12345f-67890D "56 Pitt St Boiler Plant"'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert "boilerPlantRef = '12345f-67890D'"

    def test_nested_ref(self):
        haystack_query = '(equipRef->boiler and equipRef->capacity == 100' \
                         ' and water) or (equipRef->fan and equipRef->equip)'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        print(set(sql_result[1]))
        assert "( water = 1 and equipRef in (select equipRef from metadata where" \
               " boiler = 1 and capacity = 100) ) or" \
               " ( equipRef in (select equipRef from metadata where fan = 1 and equip = 1) )" == sql_result[0]

    def test_nested_ref_mix(self):
        haystack_query = 'equipRef->boiler and equipRef->capacity == 100 and siteRef->boiler and siteRef->air'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        print(sql_result[1])
        assert "siteRef in (select siteRef from metadata where boiler = 1 and air = 1) and " \
               "equipRef in (select equipRef from metadata where boiler = 1 and capacity = 100)" == sql_result[0] or \
               "equipRef in (select equipRef from metadata where boiler = 1 and capacity = 100) and" \
               " siteRef in (select siteRef from metadata where boiler = 1 and air = 1)" == sql_result[0]

if __name__ == '__main__':
    unittest.main()
