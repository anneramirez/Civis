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
broadcasts_table = os.environ.get('broadcasts_table')
included_groups_table = os.environ.get('included_groups_table')
excluded_groups_table = os.environ.get('excluded_groups_table')


### VAR Global ###
auth = HTTPBasicAuth(user,pw)
url = "https://secure.mcommons.com/api/broadcasts"

params = {'company':company_key,
          'start_time':update_from,
          'end_time':update_to,
          'page':1     
          }
                  
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

def process_sublist(t,objs):       
    subs = []
    single = {}
    for p in t:
        for k,v in p.items():
            if k =='id':
                try:
                    for x in p[objs]:
                        path = p[objs][x]
                        if isinstance(path, dict): #this is single clicks
                            single.update({'broadcast_id':v})
                            for a,b in path.items():
                                single.update({a:b})
                            subs.append(single) 
                            single = {}
                        elif isinstance(path, list):
                            for s in path:
                                single.update({'broadcast_id':v})
                                for a,b in s.items():
                                    single.update({a:b})
                                subs.append(single) 
                                single = {}
                except Exception as ex:
                    print(ex)
                    continue                
    return subs

def cleanPro(path):
    for p in path:
        del(p['included_groups'])
        del(p['excluded_groups'])
    return path
  
### Loop through pages to get all results ###
obj = 'broadcast'
def loopPages(url,auth,params): 
    recordsBro = []
    recordsInc = []
    recordsExc = []
    while True: #change to while True when done testing!!
        try:
            resp = getAPIdata(url,auth,params)
            tree = processXML(resp)
            path = tree['response'][obj+'s'][obj]
            inc = process_sublist(path,'included_groups')
            exc = process_sublist(path,'excluded_groups')
            recordsInc.extend(inc)
            recordsExc.extend(exc)
            clean = cleanPro(path)
            r = flatXML(clean)
            recordsBro.extend(r)
            #if (int(tree['response']['profiles']['num']) > 0):
             #add data file to set
            
            params['page'] += 1 #go to next page
            #else:
             #   break
        except Exception as ex:
            print(ex)
            break
    print(params['page'])
    params['page'] = 1
    return recordsBro, recordsInc, recordsExc

dataBro, dataInc, dataExc = loopPages(url,auth,params)  

dfB = pd.DataFrame(dataBro)
dfI = pd.DataFrame(dataInc)
dfE = pd.DataFrame(dataExc)
  
### Dataframe to Civis ###
client = civis.APIClient()
civis.io.dataframe_to_civis(dfB, 'redshift-ppfa', broadcasts_table, existing_table_rows='drop')
civis.io.dataframe_to_civis(dfI, 'redshift-ppfa', included_groups_table, existing_table_rows='drop')
civis.io.dataframe_to_civis(dfE, 'redshift-ppfa', excluded_groups_table, existing_table_rows='drop')

countB=len(dfB)
countI=len(dfI)
countE=len(dfE)

print(str(countB) + " broadcasts imported")
print(str(countI) + " included groups imported")
print(str(countE) + " excluded groups imported")
