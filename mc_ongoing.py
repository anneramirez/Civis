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

### VAR Global ###
url = "https://secure.mcommons.com/api/" + endpoint
user = "anne.ramirez@ppfa.org"
pw = "Dre$m0B0$t"
auth = HTTPBasicAuth(user,pw)
payload = ""
headers = {'Content-type' : 'application/json'}

### VAR Profiles ###
params = {'include_custom_columns':'false',
          'include_subscriptions':'false',
          'include_clicks':'false',
          'include_members':'false',
          'page':1,
          'from':update_from}

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
                         
'subscriptions_subscription': [OrderedDict([('campaign_id', '16031'), ('campaign_name', 'Static Facebook Opt-in'), ('campaign_description', 'Web path for opt-ins on Facebook App when no specific campaign is running.'), ('opt_in_path_id', '19441'), ('status', 'Opted-Out'), ('opt_in_source', 'Pro-Choice breaking news'), ('created_at', '2009-06-02T04:13:39Z'), ('activated_at', '2009-06-02T04:13:39Z'), ('opted_out_at', '2009-09-25T18:18:22Z'), ('opt_out_source', 'Hard bounce')]),                         
                         

###Flatten###
def flatten_dict(d, separator='_', prefix=''):
    return { prefix + separator + k if prefix else k : v
            for k, v in d.items()
            for k, v in flatten_dict(v, separator, k).items()
            } if isinstance(d, dict) else { prefix : d }

### Parse XML Response ###
def processXML(d):
    tree = xmltodict.parse(d.content, attr_prefix='')
    return tree

### Flat XML Response PROFILES###
def flatXML(tree):
    flat = [flatten_dict(x) for x in tree['response']['profiles']['profile']]
    return flat

### API Call ###
def getAPIdata(url,auth,params):
    resp = requests.get(url, auth=auth, params=params)
    return resp 

### Loop through pages to get all results ###
def loopPages(url,auth,params): 
    records = []
    while True:
        try:
            resp = getAPIdata(url,auth,params)
            tree = processXML(resp)
            r = flatXML(tree)
            if (int(tree['response']['profiles']['num']) > 0):
                records.extend(r) #add data file to set
                params['page'] += 1 #go to next page
            else:
                break
        except:
            break
    return records

data = loopPages(url,auth,params)

df = pd.DataFrame(data,
                  columns=endpoint_columns)

### Dataframe to Civis ###
client = civis.APIClient()
civis.io.dataframe_to_civis(df, 'redshift-ppfa', 'anneramirez.mc_profiles', existing_table_rows='append', distkey='id')
