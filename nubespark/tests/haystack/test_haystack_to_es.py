from nubespark.tests.base.base_test_case import BaseTestCase

__author__ = 'topsykretts'


class HaystackToEsParsingTest(BaseTestCase):
    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        delattr(cls, "parser")

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from nubespark.ts.iot_metadata import HaystackToEs
        cls.parser = HaystackToEs()

    def test_parsing_simple_marker(self):
        """
        Test different queries...
        :return:
        """
        tag_haystack_query = "supply and air and temp and sensor"
        tag_sql_result = self.parser.parse(tag_haystack_query)[0]
        print(tag_sql_result)
        assert 'supply.raw:"m:" AND air.raw:"m:" AND temp.raw:"m:" AND sensor.raw:"m:"' == tag_sql_result

    def test_parsing_markers_with_parens_and_not(self):
        tag_haystack_query = "supply and (air or pressure) and temp and not sensor"
        tag_sql_result = self.parser.parse(tag_haystack_query)[0]
        print(tag_sql_result)
        assert 'supply.raw:"m:" AND ( air.raw:"m:" OR pressure.raw:"m:" ) AND temp.raw:"m:" AND NOT sensor.raw:"m:"' == tag_sql_result

    def test_parsing_tag_and_non_tag_query(self):
        haystack_query = "return and water and temp and not sensor and equipRef==\"equip123\""
        sql_result = self.parser.parse(haystack_query)[0]
        print(sql_result)
        assert 'return.raw:"m:" AND water.raw:"m:" AND temp.raw:"m:" AND NOT sensor.raw:"m:" AND equipRef.raw : "equip123"' == sql_result

    def test_nested_parenthesis(self):
        haystack_query = '(return and (temp or otherVal == "abc" )) or (supply and pressure)'
        sql_result = self.parser.parse(haystack_query)[0]
        print(sql_result)
        assert '( return.raw:"m:" AND ( temp.raw:"m:" OR otherVal.raw : "abc" ) ) OR ( supply.raw:"m:" AND pressure.raw:"m:" )' == \
               sql_result

    def test_non_tag_only(self):
        haystack_query = 'siteRef == "site1"'
        sql_result = self.parser.parse(haystack_query)[0]
        print(sql_result)
        assert 'siteRef.raw : "site1"' == sql_result

    def test_numeric_comp(self):
        haystack_query = "val == 120 and temperature != 34.54 and rate >= 30 and rate <= 67.56"
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert "val_num_ : 120 AND NOT temperature_num_ : 34.54 AND rate_num_ :>= 30 AND rate_num_ :<= 67.56" == sql_result[0]

    def test_bool_match(self):
        haystack_query = 'feature == true'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert "feature_bool_ : true" == sql_result[0]

    def test_ref_only(self):
        haystack_query = 'id == @S.SCP.Ground_AC_Unit_1.Run_Light'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert "(id.raw : r\:S.SCP.Ground_AC_Unit_1.Run_Light OR id.raw : r\:S.SCP.Ground_AC_Unit_1.Run_Light\ *)" == sql_result[0]

    def test_ref_only_not_equal(self):
        haystack_query = 'id != @S.SCP.Ground_AC_Unit_1.Run_Light'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert "(NOT id.raw : r\:S.SCP.Ground_AC_Unit_1.Run_Light AND NOT id.raw : r\:S.SCP.Ground_AC_Unit_1.Run_Light\ *)" == sql_result[0]

    def test_ref_with_desc(self):
        haystack_query = '(boilerPlantRef == @12345f-67890D "56 Pitt St Boiler Plant") and (air or water)'
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert '( (boilerPlantRef.raw : r\:12345f-67890D OR' \
               ' boilerPlantRef.raw : r\:12345f-67890D\ *) ) AND ( air.raw:"m:" OR water.raw:"m:" )' == sql_result[0]

    def test_null_query(self):
        haystack_query = "air and temp and return and sensor == null"
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert 'air.raw:"m:" AND temp.raw:"m:" AND return.raw:"m:" AND NOT _exists_:sensor.raw' == sql_result[0]

    def test_not_null_query(self):
        haystack_query = "air and temp and return and sensor != null"
        sql_result = self.parser.parse(haystack_query)
        print(sql_result[0])
        assert 'air.raw:"m:" AND temp.raw:"m:" AND return.raw:"m:" AND _exists_:sensor.raw' == sql_result[0]
