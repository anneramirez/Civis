import xmltodict
import requests
import civis
import json
import os
from requests.auth import HTTPBasicAuth
import pandas as pd
import xml.etree.ElementTree as ET
import io


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
xml_data = io.StringIO(r.text)

def iter_docs(profile):
    profile_attr = profile.attrib
    for doc in profile.iter('document'):
        doc_dict = profile_attr.copy()
        doc_dict.update(doc.attrib)
        doc_dict['data'] = doc.text
        yield doc_dict
        
def iter_profile(etree):
    for profile in etree.iter('profile'):
        for row in iter_docs(profile):
            yield row
        
etree = ET.parse(xml_data) #create an ElementTree object 
doc_df = pd.DataFrame(list(iter_profile(etree)))


#client = civis.APIClient()

