import xmltodict
import requests
import civis
import json
import os
from requests.auth import HTTPBasicAuth
import pandas as pd
import xml.etree.ElementTree as ET

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
xml_data = r.content

class XML2DataFrame:

  def __init__(self, xml_data):
    self.root = ET.XML(xml_data)
  def parse_root(self, root):
    def parse_element(element, parsed=None):
      #Collect {key:attribute} and {tag:text} from thie XML element and all its children into a single dictionary of strings.
      if parsed is None:
        parsed = dict()
      for key in element.keys():
        if key not in parsed:
          parsed[key] = element.attrib.get(key)
        if element.text:
          parsed[element.tag] = element.text
        else:
          raise ValueError('duplicate attribute {0} at element {1}'.format(key, element.getroottree().getpath(element)))       
      for child in list(element):
        self.parse_element(child, parsed)
        return parsed
       #Return a list of dictionaries from the text and attributes of the children under this XML root.
    for child in root.getchildren():
      return parse_element(child)
      
  def process_data(self):
  # Initiate the root XML, parse it, and return a dataframe #
    structure_data = self.parse_root(self.root)
    return pd.DataFrame(structure_data)

xml2df = XML2DataFrame(xml_data)
xml_dataframe = xml2df.process_data()

print(xml_data)

#client = civis.APIClient()

