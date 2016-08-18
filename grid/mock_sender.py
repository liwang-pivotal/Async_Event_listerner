import requests
import json
from random import randint

baseURL = "http://localhost:7071/gemfire-api/v1/UnitTelemetry/"

#times = ['2015-11-15T17:25:47:119Z']

for i in range(10000):
    time = '2015-11-1'+str(randint(0,9))+'T'+str(randint(0,24))+':'+str(randint(0,60))+':'+str(randint(0,60))+'Z'
    payload = json.dumps({'vin': '1FVACWDT39HAJ3771', 'capture_datetime': time})
    key = hash(payload)

    r = requests.put(baseURL+str(key), data=payload, headers={'content-type':'application/json'})
    
    #payload_1 = json.dumps({'vin': '4FVACWDT39HAJ3771', 'capture_datetime': time})
    #key = hash(payload_1)

    #r = requests.put(baseURL+str(key), data=payload_1, headers={'content-type':'application/json'})
