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
              "page":"1"}


#r = requests.request("GET", url, auth = HTTPBasicAuth(user,pw), data=payload, headers=headers, params=params)
#data = xmltodict.parse(r.content, attr_prefix='')

###draft get request###
def getAPIdata(url,auth,params):
    r = requests.get(url, auth=auth, data=payload, headers=headers, params=params)
    return r

###Flatten###
def flatten_dict(d, separator='_', prefix=''):
  return { prefix + separator + k if prefix else k : v
             for k, v in d.items()
             for k, v in flatten_dict(v, separator, k).items()
             } if isinstance(d, dict) else { prefix : d }

###draft Process XML Response PROFILES###
def processXML(r):
  data = xmltodict.parse(r.content, attr_prefix='')
  flat = [flatten_dict(x) for x in data['response']['profiles']['profile']]
  return flat
  
###draft loop function###
def loopPages(url,auth,params): 
  records = []
  while True:
    r = getAPIdata(url,auth,params)
    if ('results' in r) and (r['results'] is not None) and (len(r['results']) > 0): #loop
      #name = names + '_' + str(p['page']) #??
      #fileBack = writeData(r,name) #create data file
      rlist = processXML(r)
      records.append(rlist) #add data file to set
      params['page'] += 1 #go to next page
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
