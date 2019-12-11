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
profiles_table = os.environ.get('profiles_table')
clicks_table = os.environ.get('clicks_table')
customs_table = os.environ.get('customs_table')

### VAR Global ###
auth = HTTPBasicAuth(user,pw)
url = "https://secure.mcommons.com/api/" + endpoint

params = {'company':company_key,
          'campaign_id':'154363',
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

def process_sublist(t,obj):       
    subs = []
    single = {}
    for p in t:
        for k,v in p.items():
            if k =='id':
                try:
                    path = p[obj+'s'][obj]
                    if isinstance(path, dict): #this is single clicks
                        single.update({'profile_id':v})
                        for a,b in path.items():
                            single.update({a:b})
                        subs.append(single) 
                        single = {}
                    elif isinstance(path, list):
                        for s in path:
                            single.update({'profile_id':v})
                            for a,b in s.items():
                                single.update({a:b})
                            subs.append(single) 
                            single = {}
                except Exception as ex:
                    continue                 
    return subs

def cleanPro(path):
    for p in path:
        del(p['clicks'])
        del(p['custom_columns'])
    return path
  
### PROFILES Loop through pages to get all results ###
obj = 'profile'
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

dataPro, dataClick, dataCustom = loopPages(url,auth,params)  

dfPro = pd.DataFrame(dataPro)
dfCli = pd.DataFrame(dataClick)
dfCus = pd.DataFrame(dataCustom)
  
### Dataframe to Civis ###
client = civis.APIClient()
civis.io.dataframe_to_civis(dfPro, 'redshift-ppfa', profiles_table, existing_table_rows='drop', distkey='id')
civis.io.dataframe_to_civis(dfCli, 'redshift-ppfa', clicks_table, existing_table_rows='drop', distkey='id')
civis.io.dataframe_to_civis(dfCus, 'redshift-ppfa', customs_table, existing_table_rows='drop', distkey='profile_id')

countP=len(dfPro)
countCl=len(dfCli)
countCu=len(dfCus)

print(str(countP) + " profiles imported")
print(str(countCl) + " clicks imported")
print(str(countCu) + " custom fields imported")
