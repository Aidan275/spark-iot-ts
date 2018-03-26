__author__ = 'topsykretts'

server_lookup = {
    "niagara_iot_s1": {
        "implementation": "n4",
        "uri": "http://your_server:port",
        "username": "your_username",
        "password": "your_pass",
        "pint": True
    }
}
import pyhaystack
import hszinc

# get session
server_name = 'niagara_iot_s1'
session = pyhaystack.connect(**server_lookup[server_name])

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
es = Elasticsearch()


def update_metadata_es():
    metadata_op = session.find_entity("site or equip or point")
    metadata_op.wait()
    metadata = metadata_op.result
    # converting to es format
    metadata_actions = []
    es_index = "niagara4_metadata_server1"
    es_type="metadata"
    for es_id in metadata.keys():
        doc = {}
        doc['_index'] = es_index
        doc['_type'] = es_type
        doc['_id'] = es_id
        doc['id'] = es_id
        data = metadata[es_id]
        for tag in data.tags:
            val = data.tags[tag]
            if isinstance(val, hszinc.datatypes.Ref):
                val = str(val)[1:]
            elif isinstance(val, hszinc.datatypes.MarkerType):
                val = '1'
            elif isinstance(val, bool):
                if val:
                    val = 1.0
                else:
                    val = 0.0
            doc[tag] = val
        metadata_actions.append(doc)
    # updating es metadata
    return bulk(es, metadata_actions)
ret = update_metadata_es()
print(ret)
