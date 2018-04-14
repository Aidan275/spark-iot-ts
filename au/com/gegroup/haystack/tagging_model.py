__author__ = 'topsykretts'

from abc import ABC, abstractmethod
import json

class HVal(ABC):
    """
    Haystack value super class
    """

    def __init__(self, key=None, val=None):
        self._type = "object"
        self._val = val
        self._key = key

    def setVal(self, val):
        self._val = val

    def setKey(self, key):
        self._key = key

    def returnAsJson(self, map):
        return json.dumps(map, ensure_ascii=False)[1:-1]
    @abstractmethod
    def toJson(self):
        """
        convert to json representation
        :return:
        """
        pass

    @abstractmethod
    def parseTrioSyntax(self, string):
        pass


class HMarker(HVal):
    """
    Haystack Marker
    """

    def toJson(self):
        return self.returnAsJson({self._key: "m:"})

    def parseTrioSyntax(self, string):
        if ":" not in string:
            self._key = string.strip()
            return True
        else:
            return False


class HRef(HVal):
    """
    Haystack Reference
    """

    def parseTrioSyntax(self, string):
        if ":" in string and "@" in string:
            key, val = string.split(":", 2)

            val = val.strip()
            if val[:1] != '@':
                return False
            self._val = val.replace("@", "")
            self._key = key.strip()
            return True
        else:
            return False

    def toJson(self):
        return self.returnAsJson({self._key: 'r:'+self._val})


class HStr(HVal):
    """
    Haystack String
    Currently only support for string with quotes but actual Trio supports simple string without quotes and multi-line
    """

    def parseTrioSyntax(self, string):
        if ":" in string:
            key, val = string.split(":", 1)
            key = key.strip()
            val = val.strip()
            if val[:1] == '"' and val[-1:] == '"':
                self._key = key.strip()
                self._val = val.strip()[1:-1]
                return True
            else:
                return False
        else:
            return False

    def toJson(self):
        if ":" in self._val:
            val = "s:" + self._val
        else:
            val = self._val
        return self.returnAsJson({self._key: val})


class HNull(HVal):
    def toJson(self):
        return self.returnAsJson({self._key: None})

    def parseTrioSyntax(self, string):
        if ":" in string:
            key, val = string.split(":", 1)
            key = key.strip()
            val = val.strip()
            if val == "N":
                self._key = key
                return True
            else:
                return False
        else:
            return False


class HNA(HVal):
    def toJson(self):
        return self.returnAsJson({self._key: "z:"})

    def parseTrioSyntax(self, string):
        if ":" in string:
            key, val = string.split(":", 1)
            key = key.strip()
            val = val.strip()
            if val == "NA":
                self._key = key
                return True
            else:
                return False
        else:
            return False


class HNum(HVal):

    def __init__(self, key=None, val=None, unit=None):
        super().__init__(key, val)
        self._unit = unit

    def toJson(self):
        if self._val == float("inf"):
            val = "INF"
        elif self._val == float("-inf"):
            val = "-INF"
        elif self._val == float("NaN"):
            val = "NaN"
        else:
            if self._unit is None:
                unit = ""
            else:
                unit = self._unit
            val = "n:" + str(self._val)

            if val is not None and len(unit) > 0:
                val += " " + unit

        return self.returnAsJson({self._key: val})

    def parseTrioSyntax(self, string):
        string = string.strip()
        if ":" in string:
            key, val = string.split(":", 1)
            self._key = key.strip()
            val = val.strip()

            if val == "INF":
                self._val = float("inf")
            elif val == "-INF":
                self._val = float("-inf")
            elif val == "NaN":
                self._val = float("NaN")
            else:
                num_list = HNum.get_matching_num(val)
                if len(num_list) > 0 and val.startswith(num_list[0]):
                    unit = val.replace(num_list[0], "")
                    self._val = float(num_list[0])
                    self._unit = unit
                    return True
                else:
                    return False

    @staticmethod
    def get_matching_num(num):
        import re
        return re.findall(r"[+-]? *(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", num)
