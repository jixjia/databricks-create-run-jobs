'''
Author:         Jixin Jia (Gin)
Created:        2020-04-17
Description:    This program executes all jobs created by script 'create_jobs.py'

Databricks Job API References
https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/jobs (Azure)
https://docs.databricks.com/dev-tools/api/latest/jobs.html (AWS) 
'''

import requests
import json
import urllib.parse
import time
import datetime

shard = 'https://japaneast.azuredatabricks.net/api/'

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer {PERSONAL_TOKEN}'
}

# (1) Get all Job Ids
r = requests.get(shard + '2.0/jobs/list', headers=headers)
res = r.json()

if r.status_code == 200:
    for i in res['jobs']:
        job_name = i['settings']['name']
        job_id = i['job_id']

        if 'PerformanceTest' not in job_name:
            continue # Skip jobs not related to our test
        else:
            print('[INFO] Begin runing Job: {} ({})'.format(job_id, job_name))

        # Run Job
        try:
            job_status = ''
            payload = {"job_id": job_id}
            r2 = requests.post(shard + '2.0/jobs/run-now', headers=headers, data=json.dumps(payload))
            res2 = r2.json()

            if r2.status_code == 200:
                run_id = res2['run_id']

                # Check Job Run Status
                while job_status != 'TERMINATED':
                    r3 = requests.get(shard + '2.0/jobs/runs/get?run_id={}'.format(run_id), headers=headers)
                    res3 = r3.json()
                    job_status = res3['state']['life_cycle_state']
                    state_message = res3['state']['state_message']
                    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    print('>> {} Job_Id: {} Run_Id: {} Job_Status: {} ({})'.format(time_now, job_id, run_id, job_status, state_message))
                    
                    # Wait 1 min before querying job status again
                    time.sleep(60)
        
        except Exception as e:
            print('[ERROR] Error running Job: {} -> {}'.format(job_name, e))
else:
    print('[ERROR] Unable to query Databricks job list via API')