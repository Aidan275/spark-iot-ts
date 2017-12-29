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
        from au.com.gegroup.haystack import to_sql_parser
        cls.parser = to_sql_parser

    def test_parsing_simple_marker(self):
        """
        Test different queries...
        :return:
        """
        tag_haystack_query = "supply and air and temp and sensor"
        tag_sql_result = self.parser.parse(tag_haystack_query)
        assert "supply = 1 and air = 1 and temp = 1 and sensor = 1" == tag_sql_result

    def test_parsing_markers_with_parens_and_not(self):
        tag_haystack_query = "supply and (air or pressure) and temp and not sensor"
        tag_sql_result = self.parser.parse(tag_haystack_query)
        assert "supply = 1 and ( air = 1 or pressure = 1 ) and temp = 1 and sensor != 1" == tag_sql_result

    def test_parsing_tag_and_non_tag_query(self):
        haystack_query = "return and water and temp and not sensor and equipRef==\"equip123\""
        sql_result = self.parser.parse(haystack_query)
        assert "return = 1 and water = 1 and temp = 1 and sensor != 1 and equipRef = 'equip123'" == sql_result

    def test_nested_parenthesis(self):
        haystack_query = '(return and (temp or otherVal == "abc" )) or (supply and pressure)'
        sql_result = self.parser.parse(haystack_query)
        assert "( return = 1 and ( temp = 1 or otherVal = 'abc' ) ) or ( supply = 1 and pressure = 1 )" == \
               sql_result

    def test_non_tag_only(self):
        haystack_query = 'siteRef == "site1"'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result)
        assert "siteRef = 'site1'" == sql_result


if __name__ == '__main__':
    unittest.main()
