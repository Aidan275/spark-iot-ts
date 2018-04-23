from nubespark.ts.iot_his_reader import IOTHistoryReader

__author__ = 'topsykretts'


class SparksReader(IOTHistoryReader):
    """
    Extending IOTHistoryReader with customization for reading sparks
    """
    def get_select_statement(self, timestamp_select):
        select = "select %(timestamp)s datetime as time, pointName as pointName, step_value as step_value , duration as duration  from %(view)s" % \
                 ({'view': self.view_name, 'timestamp': timestamp_select})
        return select

