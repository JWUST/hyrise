#!/usr/bin/env python
import json
import sys

json = json.loads(sys.stdin.read())

performanceData = json['performanceData']
#print performanceData

acc_duration = 0
ct_duration = 0
acc_misses = 0
ct_misses = 0

for p in performanceData:
    if p.get('papi_event') == 'PAPI_L3_TCM':
        acc_duration += p['duration']
        ct_duration +=1
    
        acc_misses += p['data']
        ct_misses += 1


print '--'
print "avg. duration: %d" % (acc_duration/ct_duration)
print "avg. l3 cache misses: %d" % (acc_misses/ct_misses)
