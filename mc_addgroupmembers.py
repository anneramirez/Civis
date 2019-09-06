import requests
import civis
import json
import os
from requests.auth import HTTPBasicAuth

### VAR Environment ###
user = os.environ.get('MC_CREDENTIAL_USERNAME')
pw = os.environ.get('MC_CREDENTIAL_PASSWORD')
group_id = os.environ.get('group_id')
db_table = os.environ.get('db_table')
company = os.environ.get('company_key')

params = {'group_id':group_id, #487022 for testing
          'company':company,
         'phone_number':0}

### VAR Global ###
auth = HTTPBasicAuth(user,pw)
url='https://secure.mcommons.com/api/add_group_member'

### Query Civis ###
client = civis.APIClient()
data=civis.io.read_civis(db_table, 'redshift-ppfa') #anneramirez.mc_test for testing

### API Call Function ###
def getAPIdata(url,auth,params):
    resp = requests.get(url, auth=auth, params=params)
    return resp 

### Chunks of 200 ###
b=1
e=200
while b<=len(data):
    phone_string=(",".join(str(v) for n in data[b:e] for v in n))
    params['phone_number'] = phone_string
    r = getAPIdata(url,auth,params)
    b+=200
    e+=200
print('Pushed ',e,' phones to MC group ',group_id,' - ',r)