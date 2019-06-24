##parameterized script###

import xmltodict
import requests
import civis
import json
from requests.auth import HTTPBasicAuth
import pandas as pd
import os 

### VAR Environment ###
endpoint = os.environ.get('endpoint')
update_from = os.environ.get('update_from')
obj = 'object' #add this;

### VAR Global ###
url = "https://secure.mcommons.com/api/" + endpoint
user = "anne.ramirez@ppfa.org"
pw = "Dre$m0B0$t"
auth = HTTPBasicAuth(user,pw)
payload = ""
headers = {}

### VAR Resources ###
params_profiles = {'include_custom_columns':'false',
                   'include_subscriptions':'false',
                    'include_clicks':'false',
                    'include_members':'false',
                    'page':1,
                    'from':update_from}
obj = 'profile'
params_subscriptions = {'include_custom_columns':'false',
                   'include_subscriptions':'true',
                    'include_clicks':'false',
                    'include_members':'false'}
params_clicks = {'include_custom_columns':'false',
                   'include_subscriptions':'true',
                    'include_clicks':'true',
                    'include_members':'false'}
### VAR Outgoing ###
params_outgoing = {'start_time':'2019-06-20',
        'page':1,
          'limit':100
         }
obj = 'message'

## Set column names per endpoint ##
profiles_columns = ['id',
                           'first_name',
                                'last_name',
                                'phone_number',
                                'email',
                                'status',
                                'created_at',
                                'updated_at',
                                'source_id',   
                                'source_type',
                                'source_name', 
                                'source_opt_in_path_id',
                                'opted_out_at',
                                'outed_out_source',
                                'address_street1',
                                'address_street2',
                                'address_city',
                                'address_state',
                                'address_postal_code',
                                'address_country',
                                'run_date']
subscriptions_columns = ['id'.
                         'campaign_id',
                         'campaign_name',
                         'campaign_description',
                         'opt_in_path_id',
                         'status',
                         'opt_in_source',
                         'created_at',
                         'activated_at',
                         'opted_out_at',
                         'opt_out_source']
                         
                   
                         

###Flatten###
def flatten_dict(d, separator='_', prefix=''):
    return { prefix + separator + k if prefix else k : v
            for k, v in d.items()
            for k, v in flatten_dict(v, separator, k).items()
            } if isinstance(d, dict) else { prefix : d }

### Parse XML Response ###
def processXML(d, obj):
    tree = xmltodict.parse(d.content, attr_prefix='')
    node = tree['response'][obj+'s'][obj]
    return tree, node

### Flat XML Response PROFILES###
def flatXML(node):
    flat = [flatten_dict(x) for x in node]
    return flat

### API Call ###
def getAPIdata(url,auth,params):
    resp = requests.get(url, auth=auth, params=params)
    return resp 

### SUBSCRIPTIONS Loop through profiles to pull out subscriptions ###
def loopSubs(d):
    i = 0
    records = []
    for index in d:
        try:
            records.append((d[i]['id'],d[i]['subscriptions']))
            i += 1
        except KeyError:
            if (d[i]['status'] == 'Profiles with no Subscriptions'):
                i =+ 1
                continue
            else:
                print('error')
                break
    return records

### PROFILES Loop through pages to get all results ###
def loopPages(url,auth,params): 
    recordsPro = []
    recordsSubs = []
    while (params['page'] < 4): #change to while True when done testing!!
        try:
            resp = getAPIdata(url,auth,params)
            tree = processXML(resp)
            r = flatXML(tree)
            if (int(tree['response']['profiles']['num']) > 0): #Why did i add this? can't find when i got an error
                recordsPro.extend(r) #add data file to set
                subs = loopSubs(tree['response']['profiles']['profile'])
                recordsSubs.extend(subs)
                params['page'] += 1 #go to next page
            else:
                break
        except:
            break
    return recordsPro, recordsSubs

dataPro, dataSubs = loopPages(url,auth,params)

df = pd.DataFrame(dataPro,
                  columns=endpoint_columns) #how to use parameters for this?

### Dataframe to Civis ###
client = civis.APIClient()
civis.io.dataframe_to_civis(df, 'redshift-ppfa', 'anneramirez.mc_profiles', existing_table_rows='append', distkey='id')




#### possible solution for subs ####
### API Call ###
def getAPIdata(url,auth,params):
    resp = requests.get(url, auth=auth, params=params)
    return resp 

def processXML(d):
    tree = xmltodict.parse(d.content, attr_prefix='', dict_constructor=dict)
    return tree
  
raw = getAPIdata(url,auth,params)
tree=processXML(raw)
t = tree['response']['profiles']['profile']

def subResource(t) #t = tree['response']['profiles']['profile']
  subs = []
  single = {}
  for p in t:
    for k,v in p.items():
        if k =='id':
            try:
                path = p[obj+'s'][obj]
                if isinstance(path, dict): #this is single dict vs list of dicts ie if profile only has one click
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



