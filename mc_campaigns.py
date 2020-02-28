import xmltodict
import requests
import civis
import json
from requests.auth import HTTPBasicAuth
import pandas as pd
import os
import datetime

user = os.environ.get('MC_CREDENTIAL_USERNAME')
pw = os.environ.get('MC_CREDENTIAL_PASSWORD')
endpoint = 'campaigns'
object_name = 'campaign'
campaigns_table = os.environ.get('campaigns_table')
opt_in_paths_table = os.environ.get('opt_in_paths_table')

### VAR Global ###
auth = HTTPBasicAuth(user,pw)
url = "https://secure.mcommons.com/api/" + endpoint

params = {'company':'CO5945A0888A908151444FB59D3D3AC455',
         'include_opt_in_paths':1}
         
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
    
def pushData(dataCam,dataOpt):
    dfCam = pd.DataFrame(dataCam)
    dfOpt = pd.DataFrame(dataOpt)
    client = civis.APIClient()
    civis.io.dataframe_to_civis(dfCam, 'redshift-ppfa', campaigns_table, existing_table_rows='append', headers='true',max_errors=500)
    civis.io.dataframe_to_civis(dfOpt, 'redshift-ppfa', opt_in_paths_table, existing_table_rows='append', headers='true',max_errors=500)
    print(civis.io.read_civis_sql('select count(id) from ' + campaigns_table,'redshift-ppfa'))
    print(str(len(dfCam)) + ' ' + object_name+"s" + " imported")
    print(str(len(dfOpt)) + " opt-in paths imported")         
         
def process_sublist(t,obj):       
    subs = []
    single = {}
    for p in t:
        for k,v in p.items():
            if k =='id':
                try:
                    path = p[obj+'s'][obj]
                    if isinstance(path, dict): #this is single clicks
                        single.update({'campaign_id':v})
                        for a,b in path.items():
                            single.update({a:b})
                        subs.append(single) 
                        single = {}
                    elif isinstance(path, list):
                        for s in path:
                            single.update({'campaign_id':v})
                            for a,b in s.items():
                                single.update({a:b})
                            subs.append(single) 
                            single = {}
                except Exception as ex:
                    continue                 
    return subs

def cleanPro(path):
    for p in path:
        del(p['opt_in_paths'])
    return path
  
### Loop through pages to get all results ###
obj = object_name
def loopPages(url,auth,params): 
    recordsCam = []
    recordsOpt = []
    while True: #change to while True when done testing!!
        try:
            resp = getAPIdata(url,auth,params)
            tree = processXML(resp)
            path = tree['response'][obj+'s'][obj]
            optins = process_sublist(path,'opt_in_path')
            recordsOpt.extend(optins)
            clean = cleanPro(path)
            r = flatXML(clean)
            recordsCam.extend(r)
            params['page'] += 1 #go to next page
        except Exception as ex:
            print(ex)
            break
    params['page'] = 1
    pushData(recordsCam,recordsOpt)
    
loopPages(url,auth,params)
