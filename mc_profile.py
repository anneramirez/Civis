import xmltodict
import requests
import civis
import json
from requests.auth import HTTPBasicAuth
import pandas as pd
import os

update_from = os.environ.get('update_from')
update_to = os.environ.get('update_to')
user = os.environ.get('MC_CREDENTIAL_USERNAME')
pw = os.environ.get('MC_CREDENTIAL_PASSWORD')
company = os.environ.get('company_key')

### VAR Global ###
#user = "anne.ramirez+civisapi@ppfa.org"
#pw = "q0QnWX3Z5Pki"
auth = HTTPBasicAuth(user,pw)
url = "https://secure.mcommons.com/api/profiles"

params = {'include_custom_columns':'false',
                   'include_subscriptions':'false',
                    'include_clicks':'true',
                    'include_members':'false',
                    'page':1,
                    'from':update_from,
                    'to':update_to,
          'company':company_key
                  }
                  
### API Call ###
def getAPIdata(url,auth,params):
    resp = requests.get(url, auth=auth, params=params)
    return resp 

def processXML(d):
    tree = xmltodict.parse(d.content, attr_prefix='', dict_constructor=dict)
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
                    print(k,v,ex)
                    continue                 
    return subs

def cleanPro(path):
    for p in path:
        del(p['clicks'])
    return path
  
### PROFILES Loop through pages to get all results ###
obj = 'profile'
def loopPages(url,auth,params): 
    recordsPro = []
    recordsSub = []
    while params['page'] < 3: #change to while True when done testing!!
        try:
            resp = getAPIdata(url,auth,params)
            tree = processXML(resp)
            path = tree['response'][obj+'s'][obj]
            clicks = process_sublist(path,'click')
            recordsSub.extend(clicks)
            clean = cleanPro(path)
            r = flatXML(clean)
            recordsPro.extend(r)
            #if (int(tree['response']['profiles']['num']) > 0):
             #add data file to set
            
            params['page'] += 1 #go to next page
            #else:
             #   break
        except:
            break
    params['page'] = 1
    return recordsPro, recordsSub  

dataPro, dataClick = loopPages(url,auth,params)  

dfP = pd.DataFrame(dataPro)
dfC = pd.DataFrame(dataClicks)
  
### Dataframe to Civis ###
client = civis.APIClient()
civis.io.dataframe_to_civis(dfP, 'redshift-ppfa', 'anneramirez.mc_action_profiles', existing_table_rows='append', distkey='id')
civis.io.dataframe_to_civis(dfC, 'redshift-ppfa', 'anneramirez.mc_action_clicks', existing_table_rows='append', distkey='id')

print("Success!")
