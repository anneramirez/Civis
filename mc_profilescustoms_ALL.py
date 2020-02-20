import xmltodict
import requests
import civis
import json
from requests.auth import HTTPBasicAuth
import pandas as pd
import os
import datetime

today = datetime.date.today()
yesterday = today - datetime.timedelta(days = 1)
update_from = str(yesterday)+" 05:00:00 UTC"
update_to = str(today)+" 04:59:59 UTC"

user = os.environ.get('MC_CREDENTIAL_USERNAME')
pw = os.environ.get('MC_CREDENTIAL_PASSWORD')
company_key = os.environ.get('company_key')
profiles_table = os.environ.get('profiles_table')
clicks_table = os.environ.get('clicks_table')
customs_table = os.environ.get('customs_table')
subscriptions_table = os.environ.get('subscriptions_table')

### VAR Global ###
auth = HTTPBasicAuth(user,pw)
url = "https://secure.mcommons.com/api/profiles"

params = {'company':company_key,
          'include_custom_columns':'true',
          'include_subscriptions':'true',
          'include_clicks':'true',
          'include_members':'false',
          'page':1,
          'limit':1000        
          }

### PROFILES Loop through pages to get all results ###
obj = 'profile'
def loopPages(url,auth,params): 
    startTime= datetime.datetime.now()
    rPro = []
    rCus = []
    while True: #change to while True when done testing!!
        try:
            resp = getAPIdata(url,auth,params)
            tree = processXML(resp)
            path = tree['response'][obj+'s'][obj]
            customs = process_sublist(path,'custom_column')
            rCus.extend(customs)
            clean = cleanPro(path)
            r = flatXML(clean)
            rPro.extend(r)
            params['page'] += 1 #go to next page    
            if params['page']%100 == 0: #evaluate current page
                pushData(rPro,rCus)
                rPro = []
                rCus = []
            if params['page']%10 == 0:
                timeElapsed=datetime.datetime.now()-startTime
                print("Processed " + str(params['page']) + " pages in " + str(timeElapsed))

            #else:
             #   break
        except Exception as ex:
            print(ex)
            break
    params['page'] = 1
    pushData(rPro,rCus)
