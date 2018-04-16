#!/bin/bash

curl -XPUT ${1}:${2}/_template/metadata_template -H "Content-Type:application/json" -d '{
  "template": "niagara*",
"mappings": {
    "metadata": {
      "dynamic_templates": [
        {
          "*_num_as_float_for_range_filter": {
           "match": "*_num_",
            "mapping":{
              "type":"float",
              "ignore_malformed": true
            }
          }
        },
        {
          "*_bool_as_boolean": {
           "match": "*_bool_",
            "mapping":{
              "type":"boolean"
            }
          }
        },
        {
          "everything_as_text_with_keyword": {
            "match_mapping_type": "string",
            "mapping": {
              "type": "text",
              "fields": {
                "raw": {
                  "type":  "keyword",
                  "ignore_above": 256
                }
              }
            }
          }
        }
      ]
    }
  }
}'