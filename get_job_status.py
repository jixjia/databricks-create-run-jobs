import requests
import json
import urllib.parse
import time

# Job API References
# https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/jobs (Azure)
# https://docs.databricks.com/dev-tools/api/latest/jobs.html (AWS) 

# (1) Get all Job Ids

shard = 'https://japaneast.azuredatabricks.net/api/'

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer {PERSONAL_TOKEN}'
}

# Construct API call (Get Jobs)
endpoint = '2.0/jobs/runs/get?run_id=13'
url = shard + endpoint

# Make http request
r = requests.get(url, headers=headers)
res = r.json()

if r.status_code == 200:
    print(res['state']['state_message'], res['state']['life_cycle_state'])
        