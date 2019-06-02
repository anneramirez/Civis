import xmltodict
import requests
import civis
import json
from requests.auth import HTTPBasicAuth
import pandas as pd
from pandas.io.json import json_normalize

### VAR Global ###
user = "anne.ramirez@ppfa.org"
pw = "Dre$m0B0$t"
auth = HTTPBasicAuth(user,pw)
payload = ""
headers = {'Content-type' : 'application/json'}

### VAR Profiles ###
url = "https://secure.mcommons.com/api/profiles"
params = {"include_custom_columns":"false",
               "include_subscriptions":"false",
              "include_clicks":"false",
              "include_members":"false",
              "page":1}


###Flatten###
def flatten_dict(d, separator='_', prefix=''):
    return { prefix + separator + k if prefix else k : v
            for k, v in d.items()
            for k, v in flatten_dict(v, separator, k).items()
            } if isinstance(d, dict) else { prefix : d }

###draft Parse XML Response PROFILES###
def processXML(d):
    tree = xmltodict.parse(d.content, attr_prefix='')
    return tree

###draft Flat XML Response PROFILES###
def flatXML(tree):
    flat = [flatten_dict(x) for x in tree['response']['profiles']['profile']]
    return flat

def getAPIdata(url,auth,params):
    resp = requests.get(url, auth=auth, data=payload, headers=headers, params=params)
    #d = processXML(resp)
    #r = flatXML(d)
    return resp 
  
def loopPages(url,auth,params): 
    records = []
    while True:
        resp = getAPIdata(url,auth,params)
        tree = processXML(resp)
        r = flatXML(tree)
        #if ('results' in r) and (r['results'] is not None) and (len(r['results']) > 0): #loop
        if (int(tree['response']['profiles']['num']) > 0):
            #name = names + '_' + str(p['page']) #??
            #fileBack = writeData(r,name) #create data file
            #rlist = processXML(r)
            records.extend(r) #add data file to set
            params['page'] += 1 #go to next page
        if params['page'] == 3:
            break  
        else:
            break
    return records

records = loopPages(url,auth,params)

df = pd.DataFrame(records,
                  columns=['id','first_name','last_name',
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
'run_date'])

### Dataframe to Civis ###
client = civis.APIClient()
civis.io.dataframe_to_civis(df, 'redshift-ppfa', 'anneramirez.mc_profiles',existing_table_rows='drop',distkey='id')
