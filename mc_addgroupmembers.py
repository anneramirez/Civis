import xmltodict
import requests
import civis
import json
from requests.auth import HTTPBasicAuth
import pandas as pd

### VAR Global ###
user = "anne.ramirez@ppfa.org"
pw = "Dre$m0B0$t"
auth = HTTPBasicAuth(user,pw)

url='https://secure.mcommons.com/api/add_group_member'
params = {'group_id':487022,
         'phone_number':0}

### Query Civis ###
client = civis.APIClient()
d=civis.io.read_civis('anneramirez.mc_test', 'redshift-ppfa')
phone_string=(",".join(str(v) for n in d[1:] for v in n))
params['phone_number'] = phone_string

print(params)
{'group_id': 484973, 'phone_number': '13014048269,14155958458'}

### just need to make api call! ###
### also this um doesn't need to be a separate script, can do in civis python script ###
