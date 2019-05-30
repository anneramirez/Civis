import xmltodict
import requests
import civis
import json
from requests.auth import HTTPBasicAuth
import pandas as pd
from pandas.io.json import json_normalize

url = "https://secure.mcommons.com/api/profiles"
user = "anne.ramirez@ppfa.org"
pw = "Dre$m0B0$t"
querystring = {"include_custom_columns":"false",
               "include_subscriptions":"false",
              "include_clicks":"false",
              "include_members":"false",
              "limit":"10"}

payload = ""
headers = {'Content-type' : 'application/json'}

r = requests.request("GET", url, auth = HTTPBasicAuth(user,pw), data=payload, headers=headers, params=querystring)
data = xmltodict.parse(r.content, attr_prefix='')

def flatten_dict(dd, separator='_', prefix=''):
    return { prefix + separator + k if prefix else k : v
             for kk, vv in dd.items()
             for k, v in flatten_dict(vv, separator, kk).items()
             } if isinstance(dd, dict) else { prefix : dd }

flat = [flatten_dict(x) for x in data['response']['profiles']['profile']]
df = pd.DataFrame(flat,columns=['id','first_name','last_name',
'phone_number',
'email',
'status',
'created_at',
'updated_at',
'source_id'   
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
civis.io.dataframe_to_civis(df, 'redshift-ppfa', 'anneramirez.mc_profiles',existing_table_rows='drop',distkey:id)
