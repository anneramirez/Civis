import xmltodict
import requests
import civis
import json
from requests.auth import HTTPBasicAuth
import pandas as pd
import os
import datetime

today = datetime.date.today()
yesterday = today - datetime.timedelta(days = 1)
update_from = str(yesterday)+" 00:00:00 UTC"
update_to = str(yesterday)+" 23:59:59 UTC"

user = os.environ.get('MC_CREDENTIAL_USERNAME')
pw = os.environ.get('MC_CREDENTIAL_PASSWORD')
company_key = os.environ.get('company_key')
endpoint = os.environ.get('endpoint')
object_name = os.environ.get('object')
staging_table = os.environ.get('staging_table')


### VAR Global ###
auth = HTTPBasicAuth(user,pw)
url = "https://secure.mcommons.com/api/" + endpoint

params = {'company':company_key,
          'page':1,
         'limit':1000,
         'start_time':update_from,
         'end_time':update_to}
                  
### API Call ###
def getAPIdata(url,auth,params):
    resp = requests.get(url, auth=auth, params=params)
    return resp 

def processXML(d):
    tree = xmltodict.parse(d.content, attr_prefix='', cdata_key='value', dict_constructor=dict)
    return tree

###Flatten###
def flatten_dict(d, separator='_', prefix=''):
    return { prefix + separator + k if prefix else k : v
            for k, v in d.items()
            for k, v in flatten_dict(v, separator, k).items()
            } if isinstance(d, dict) else { prefix : d }
            
def flatXML(tree):
    flat = [flatten_dict(x) for x in tree]
    return flat

  
### PROFILES Loop through pages to get all results ###
obj = object_name
def loopPages(url,auth,params): 
    records = []
    while True: #params['page'] < 3: #change to while True when done testing!!
        try:
            resp = getAPIdata(url,auth,params)
            tree = processXML(resp)
            path = tree['response'][obj+'s'][obj]
            r = flatXML(path)
            records.extend(r)
            #if (int(tree['response']['profiles']['num']) > 0):
             #add data file to set
            
            params['page'] += 1 #go to next page
            #else:
             #   break
        except:
            break
    params['page'] = 1
    return records

dataIncoming = loopPages(url,auth,params)  

dfIncoming = pd.DataFrame(dataIncoming)
  
### Dataframe to Civis ###
client = civis.APIClient()
civis.io.dataframe_to_civis(dfIncoming, 'redshift-ppfa', staging_table, existing_table_rows='drop', distkey='id')

countI=len(dfIncoming)

print(str(countI) + " incoming messages imported")
