import xmltodict
import requests
import civis
import json
from requests.auth import HTTPBasicAuth
import pandas as pd
import os
import datetime

client = civis.APIClient()

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

def pushData(dataBro, dataInc, dataExc):
    dfB = pd.DataFrame(dataBro)
    dfI = pd.DataFrame(dataInc)
    dfO = pd.DataFrame(dataExc)
    civis.io.dataframe_to_civis(dfB, 'redshift-ppfa', broadcasts_table, existing_table_rows='append', headers='true',max_errors=500)
    civis.io.dataframe_to_civis(dfI, 'redshift-ppfa', included_groups_table, existing_table_rows='append', headers='true',max_errors=500)
    civis.io.dataframe_to_civis(dfO, 'redshift-ppfa', excluded_groups_table, existing_table_rows='append',headers='true', max_errors=500)
    print(civis.io.read_civis_sql('select count(distinct id) from ' + broadcasts_table,'redshift-ppfa'))
    print(datetime.datetime.now())
    print(str(len(dfB)) + " broadcasts imported")
    print(str(len(dfI)) + " inccluded groups imported")
    print(str(len(dfO)) + " excluded groups imported")

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
    pageCount = 1
    print("start while loop " + str(datetime.datetime.now()))
    while params['page'] <= pageCount: #change to while True when done testing!!
        try:
            resp = getAPIdata(url,auth,params)
            print("resp " + str(datetime.datetime.now()))
            tree = processXML(resp)
            pages = tree['response'][obj+'s']
            pageCount = int(pages.get('page_count'))
            path = tree['response'][obj+'s'][obj]
            inc = process_sublist(path,'included_groups')
            exc = process_sublist(path,'excluded_groups')
            recordsInc.extend(inc)
            recordsExc.extend(exc)
            clean = cleanPro(path)
            r = flatXML(clean)
            recordsBro.extend(r)
            params['page'] += 1 #go to next page
            #else:
             #   break
        except Exception as ex:
            print(ex)
            print(resp)
            print(resp.text)
            print(params['page'])
            break
    print(params['page'])
    params['page'] = 1
    pushData(recordsBro, recordsInc, recordsExc)

loopPages(url,auth,params)

old_broadcasts_table = broadcasts_table + '_backup'
bro_t = civis.io.read_civis_sql('select count(distinct id) from ' + old_broadcasts_table,'redshift-ppfa')
bro_count = int(bro_t[1][0])
new_t = civis.io.read_civis_sql('select count(distinct id) from ' + broadcasts_table,'redshift-ppfa')
new_count = int(new_t[1][0])

if new_count < bro_count:
          loopPages(url,auth,params)
elif new_count >= bro_count:
          print("Count check passed!")
