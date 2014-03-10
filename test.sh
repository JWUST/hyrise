#!/bin/bash
#preheat
curl -X POST --silent --data-urlencode 'query@ccsched_test.json' http://localhost:5000/jsonQuery > /dev/null
#go
echo "go go go"
seq 100 | parallel -n0 "curl -X POST --silent  --data "performance=true" --data-urlencode 'query@ccsched_test.json' http://localhost:5000/jsonQuery | ./parse_json.py"
