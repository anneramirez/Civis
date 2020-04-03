import xmltodict
import requests
import civis
import json
from requests.auth import HTTPBasicAuth
import pandas as pd
import os
import datetime
import sys
import time

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

col_names = ['address_city', 'address_country', 'address_postal_code',
       'address_state', 'address_street1', 'address_street2', 'created_at',
       'email', 'first_name', 'id', 'last_name',
       'last_saved_districts_congressional_district',
       'last_saved_districts_split_district',
       'last_saved_districts_state_lower_district',
       'last_saved_districts_state_upper_district', 'last_saved_location_city',
       'last_saved_location_country', 'last_saved_location_latitude',
       'last_saved_location_longitude', 'last_saved_location_postal_code',
       'last_saved_location_precision', 'last_saved_location_state',
       'opted_out_at', 'opted_out_source', 'phone_number', 'source_id',
       'source_message_id', 'source_name', 'source_opt_in_path_id',
       'source_type', 'status', 'updated_at']

### API Call ###
def getAPIdata(url,auth,params):
	resp = requests.get(url, auth=auth, params=params)
	return resp 

def processXML(d):
	tree = xmltodict.parse(d.content, attr_prefix='', cdata_key='value', dict_constructor=dict)
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

##Count Check##
def countCheck():
	old_profiles_table = profiles_table + '_backup'
	pro_t = civis.io.read_civis_sql('select count(distinct id) from ' + old_profiles_table,'redshift-ppfa')
	pro_count = int(pro_t[1][0])
	new_t = civis.io.read_civis_sql('select count(distinct id) from ' + profiles_table,'redshift-ppfa')
	new_count = int(new_t[1][0])
	if new_count >= pro_count:
		return True

###Push to Civis###
def pushData(dataPro,dataCli,dataCus,dataSub):
	dfPro = pd.DataFrame(dataPro, columns=col_names)
	dfCli = pd.DataFrame(dataCli)
	dfCus = pd.DataFrame(dataCus)
	dfSub = pd.DataFrame(dataSub)
	dfPro = dfPro.drop(columns='source_email',errors='ignore')
	dfPro['page'] = str(params['page'])
	client = civis.APIClient()
	civis.io.dataframe_to_civis(dfPro, 'redshift-ppfa', profiles_table, existing_table_rows='append', headers='true',max_errors=500)
	civis.io.dataframe_to_civis(dfCli, 'redshift-ppfa', clicks_table, existing_table_rows='append', headers='true',max_errors=500)
	civis.io.dataframe_to_civis(dfCus, 'redshift-ppfa', customs_table, existing_table_rows='append',headers='true', max_errors=500)
	civis.io.dataframe_to_civis(dfSub, 'redshift-ppfa', subscriptions_table, existing_table_rows='append',headers='true', max_errors=500)
	print(civis.io.read_civis_sql('select count(distinct id) from ' + profiles_table,'redshift-ppfa'))
	countP=len(dfPro)
	countCl=len(dfCli)
	countCu=len(dfCus)
	countS=len(dfSub)
	print(datetime.datetime.now())
	print(str(countP) + " profiles imported")
	print(str(countCl) + " clicks imported")
	print(str(countCu) + " custom fields imported")
	print(str(countS) + " subscriptions imported")


def process_sublist(t,obj):    
	subs = []
	single = {}
	for p in t:
		for k,v in p.items():
			if k =='id':
				try:
					path = p[obj+'s'][obj]
					if isinstance(path, dict): #this is single clicks
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

def cleanPro(path):
	for p in path:
		try:
			del(p['clicks'])
			del(p['custom_columns'])
			del(p['subscriptions'])
		except Exception as ex:
			continue
	return path
  
### PROFILES Loop through pages to get all results ###
obj = 'profile'
def loopPages(url,auth,params): 
	startTime= datetime.datetime.now()
	recordsPro = []
	recordsCli = []
	recordsCus = []
	recordsSub = []
	num = 1
	attempts = 0
	while attempts < 5: #change to while True when done testing!!
		try:
			resp = getAPIdata(url,auth,params)
			tree = processXML(resp)
			n = tree['response'][obj+'s']
			num = int(n.get('num'))	
			path = tree['response'][obj+'s'][obj]
			clicks = process_sublist(path,'click')
			customs = process_sublist(path,'custom_column')
			subscriptions = process_sublist(path,'subscription')
			recordsCli.extend(clicks)
			recordsCus.extend(customs)
			recordsSub.extend(subscriptions)
			clean = cleanPro(path)
			r = flatXML(clean)
			recordsPro.extend(r) 
			if params['page']%25 == 0: #evaluate current page
				pushData(recordsPro,recordsCli,recordsCus,recordsSub)
				recordsPro = []
				recordsCli = []
				recordsCus = []
				recordsSub = []
			if params['page']%100 == 0:
				timeElapsed=datetime.datetime.now()-startTime
			print("Processed " + str(params['page']) + " pages in " + str(timeElapsed))
			params['page'] += 1 #go to next page  
		except Exception as ex:
			if num == 0:
				pushData(recordsPro,recordsCli,recordsCus,recordsSub)
			if countCheck():
				break
			print("Unexpected error:", sys.exc_info()[0])
			print(str(resp) + ' on page ' + str(params['page']))
			print(resp.text[:200])
			time.sleep(60)
			attempts += 1
	params['page'] = 1
	pushData(recordsPro,recordsCli,recordsCus,recordsSub)

loopPages(url,auth,params)

if countCheck():
	print("Count check passed!")
else:
	civis.io.query_civis('drop table ' + profiles_table, 'redshift-ppfa')
	civis.io.query_civis('drop table ' + clicks_table, 'redshift-ppfa')
	civis.io.query_civis('drop table ' + customs_table, 'redshift-ppfa')
	civis.io.query_civis('drop table ' + subscriptions_table, 'redshift-ppfa')
	loopPages(url,auth,params)
