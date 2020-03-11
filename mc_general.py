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
update_from = str(yesterday)+" 05:00:00 UTC"
update_to = str(today)+" 04:59:59 UTC"

user = os.environ.get('MC_CREDENTIAL_USERNAME')
pw = os.environ.get('MC_CREDENTIAL_PASSWORD')
company_key = os.environ.get('company_key')
endpoint = os.environ.get('endpoint')
object_name = os.environ.get('object')
staging_table = os.environ.get('staging_table')

#TESTING#
print(endpoint, object_name, staging_table)

### VAR Global ###
auth = HTTPBasicAuth(user,pw)
url = "https://secure.mcommons.com/api/" + endpoint

params = {'company':company_key}

if endpoint not in ['groups','tinyurls']:
    params.update( {'page':1,
                    'limit':1000,
                    'start_time':update_from,
                    'end_time':update_to} )
                  
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

def pushData(d):
    df = pd.DataFrame(d)
    if endpoint == 'sent_messages':
        if not 'broadcast_id' in df.columns:
            df['broadcast_id'] = float("NaN")
    client = civis.APIClient()
    civis.io.dataframe_to_civis(df, 'redshift-ppfa', staging_table, existing_table_rows='append', headers='true',max_errors=500)
    countd=len(df)
    print(str(countd) + ' ' + object_name+"s" + " imported")
  
### PROFILES Loop through pages to get all results ###
obj = object_name
def loopPages(url,auth,params): 
    startTime = datetime.datetime.now()
    records = []
    while True: #params['page'] < 3: #change to while True when done testing!!
        try:
            resp = getAPIdata(url,auth,params)
            tree = processXML(resp)
            path = tree['response'][obj+'s'][obj]
            r = flatXML(path)
            records.extend(r)
            if endpoint == 'groups':
                break
            if params['page']%100 == 0: #evaluate current page, if multiple of 100 (so 100k records) push to Civis and continue with an empty list
                pushData(records)
                records = []
                timeElapsed = datetime.datetime.now()-startTime
                print("Processed " + str(params['page']) + " pages in " + str(timeElapsed))
            params['page'] += 1 #go to next page
            #else:
             #   break
        except Exception as ex:
            print("Exception raised in looppages on page " + str(params['page']))
            print(ex)
            break
    print(str(params['page']) + " total pages processed, executing final data push')
    params['page'] = 1
    pushData(records)

loopPages(url,auth,params)  
print("Imported " + object_name)
