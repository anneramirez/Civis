import xmltodict
import requests
import civis
import json
import os
from requests.auth import HTTPBasicAuth
import pandas as pd
import xml.etree.ElementTree as ET
from pandas.io.json import json_normalize

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

def handle_id(_,profile):
    print(profile['id'])
    return true

data = xmltodict.parse(r.content, item_depth=3, item_callback=handle_id)
flat = json_normalize(data, sep='_')
print(flat)
#df = pd.DataFrame(flat)


#client = civis.APIClient()

