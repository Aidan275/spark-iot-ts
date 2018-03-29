#!/bin/bash

curl -XPUT ${1}:${2}/_template/metadata_template -H "Content-Type:application/json" -d '{
  "template": "niagara*",
"mappings": {
    "metadata": {
      "dynamic_templates": [
        {
          "everything_as_keywords": {
            "match_mapping_type": "*",
            "mapping": {
              "type": "keyword"
            }
          }
        }
      ]
    }
  }
}'