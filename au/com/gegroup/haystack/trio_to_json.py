__author__ = 'topsykretts'

from au.com.gegroup.haystack.tagging_model import *

# todo implement other
parserList = [
    HNull(),
    HNA(),
    HMarker(),
    HRef(),
    HStr(),
    HNum()
]


def trio_to_json(trio_string):
    """
    https://project-haystack.org/doc/Trio
    https://project-haystack.org/doc/Json
    :param trio_string The string in Trio (Text Record Input/Output) format that should be converted to json format
    """
    tokens = []
    for token in trio_string.split("\n"):
        token = token.strip()
        for parser in parserList:
            if len(token) > 1 and parser.parseTrioSyntax(token):
                val = parser.toJson()
                tokens.append(val)
                break
    return "{" + ",".join(tokens) + "}"


