from au.com.gegroup.ts.iot_metadata import IOTMetadata

__author__ = 'topsykretts'

from au.com.gegroup.tests.base.base_test_case import BaseTestCase
import json


class HaystackToEsParsingTest(BaseTestCase):
    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        delattr(cls, "es")

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        es_options = {
            "meta.es.nodes": "localhost",
            "meta.es.port": "9200",
            "meta.es.resource": "niagara4_metadata_scp_json_v1/metadata"
        }
        cls.es = IOTMetadata(**es_options)

    def prettyPrint(self, obj):
        print(json.dumps(obj, indent=3, ensure_ascii=False))

    def test_all_read(self):
        """
        Test different queries...
        :return:
        """
        res = self.es.read()
        self.prettyPrint(res)

    def test_read_by_query(self):
        query = "his and point and return"
        res = self.es.find(query).read()
        self.prettyPrint(res)

    def test_by_id(self):
        query = "id==@C.Drivers.NiagaraNetwork.Bourke_Rd_184.points.Floor_4_AC_Unit_1.Zone_Temp"
        res = self.es.find(query).read()
        self.prettyPrint(res["hits"]["hits"])

    def test_update_by_query(self):
        query = "his and point and navName==\"CHWV\""

        res = self.es.find(query).update("chiller, water, valve, jpt", debug=True)
        self.prettyPrint(res)

    def test_remove_by_query(self):
        query = "his and point and navName==\"CHWV\""
        res = self.es.find(query).remove("jpt", debug=True, result=True)
        self.prettyPrint(res)
