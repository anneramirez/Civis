import xmltodict
import requests
import civis
import json
from requests.auth import HTTPBasicAuth
import pandas as pd
import os 

### VAR Environment ###
endpoint = os.environ.get('endpoint')

### VAR Global ###
user = "anne.ramirez@ppfa.org"
pw = "Dre$m0B0$t"
auth = HTTPBasicAuth(user,pw)
payload = ""
headers = {'Content-type' : 'application/json'}

### VAR Profiles ###
url = "https://secure.mcommons.com/api/" + endpoint
params = {'include_custom_columns':'false',
          'include_subscriptions':'false',
          'include_clicks':'false',
          'include_members':'false',
          'page':1}


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
    resp = requests.get(url, auth=auth, params=params)
    return resp 
  
def loopPages(url,auth,params): 
    records = []
    while (params['page'] < 4):
        resp = getAPIdata(url,auth,params)
        tree = processXML(resp)
        r = flatXML(tree)
        if (int(tree['response']['profiles']['num']) > 0):
            records.extend(r) #add data file to set
            params['page'] += 1 #go to next page
        else:
            break
    return records

data = loopPages(url,auth,params)

df = pd.DataFrame(data,
                  columns=['id',
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
                                'run_date'])

### Dataframe to Civis ###
client = civis.APIClient()
civis.io.dataframe_to_civis(df, 'redshift-ppfa', 'anneramirez.mc_profiles', existing_table_rows='drop', distkey='id')
