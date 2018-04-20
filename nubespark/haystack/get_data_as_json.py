__author__ = 'topsykretts'

from pyhaystack.client.niagara import Niagara4HaystackSession
# initialize a session for niagara connection

http_args = {
    "headers": {
        "Accept": "application/json"
    }
}
session = Niagara4HaystackSession(uri="http://175.45.115.115",
                                  username="admin1",
                                  password="Enviroman1",
                                  pint=False,
                                  grid_format="json",
                                  http_args=http_args
                                  )

op = session.about()
op.wait()
res = op.result

import json


class HaystackJsonResponse:
    """
    """

    def __init__(self):
        self.rows = None
        self.meta = None
        self.cols = None

    def callback(self, response):
        print("Status code = ", response.status_code)
        # print("Body = ", response.body[:10])
        data_str = response.body.decode('utf-8')
        data = json.loads(data_str)
        # print(json.dumps(data, indent=3))
        self.rows = data["rows"]
        self.meta = data["meta"]
        self.cols = data["cols"]


response = HaystackJsonResponse()


def get_point_by_id(point_id):
    session._get('read?filter=id==@%(point_id)s' % {"point_id": point_id}, response.callback, http_args)
    point_rows = response.rows
    if isinstance(point_rows, list) and len(point_rows) > 0:
        return json.dumps(point_rows[0], ensure_ascii=False)
    else:
        return None


def raw_get(query):
    session._get(query, response.callback, http_args)
    point_rows = response.rows
    return point_rows
