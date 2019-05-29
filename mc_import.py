import xmltodict
import requests
import civis
#import json
#import os
#from requests.auth import HTTPBasicAuth
#import pandas
#import xml.etree.ElementTree as ET

url = "https://secure.mcommons.com/api/profiles"
user = "anne.ramirez@ppfa.org"
pw = "Dre$m0B0$t"
querystring = {"include_custom_columns":"false",
               "include_subscriptions":"false",
              "include_clicks":"false",
              "include_members":"false",
              "limit":"10"}

payload = ""
headers = {'Content-type' : 'application/json'}

r = requests.request("GET", url, auth = HTTPBasicAuth(user,pw), data=payload, headers=headers, params=querystring)
print(json.dumps(xmltodict.parse(r.text)))
