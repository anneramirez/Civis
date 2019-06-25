import xmltodict
import requests
import civis
import json
from requests.auth import HTTPBasicAuth
import pandas as pd
import os

update_from = os.environ.get('update_from')

### VAR Global ###
user = "anne.ramirez+civisapi@ppfa.org"
pw = "q0QnWX3Z5Pki"
auth = HTTPBasicAuth(user,pw)
url = "https://secure.mcommons.com/api/profiles"

params = {'include_custom_columns':'false',
                   'include_subscriptions':'false',
                    'include_clicks':'true',
                    'include_members':'false',
                    'page':1,
                    'from':update_from
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
    
    
