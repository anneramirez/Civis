import requests

url="https://api.securevan.com/v4/people/114745858/canvassResponses"
payload={
  "canvassContext": {
    "contactTypeId": 8,
    "inputTypeId": 11
  },
  "responses": [
    {
      "activistCodeId": 4132411,
      "action": "Remove",
      "type": "ActivistCode"
    }
  ]
}
headers={'content-type':'application/json'}

r = requests.post(url=url, json=payload, headers=headers, auth=('11607','password'))
print(r.text)
