'''
Author:         Jixin Jia (Gin)
Created:        2020-04-17
Description:    This program creates a series of jobs with varying cluster configurations.

Databricks Job API References
https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/jobs (Azure)
https://docs.databricks.com/dev-tools/api/latest/jobs.html (AWS) 
'''

import requests
import json
import urllib.parse
import time

instances = ['Standard_L4s', 
             'Standard_L8s', 
             'Standard_L16s', 
             'Standard_L32s']   # List of compute instances we wish to test

worker_nodes = [2, 4, 8, 16]    # List of worker nodes we wish to test

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer {PERSONAL_TOKEN}'
}

for i in instances:
    for n in worker_nodes:
        job_name = "{}_{}Nodes_PerformanceTest".format(i, n)
        payload = {
            "name": job_name,
            "notebook_task": {
                "notebook_path": "/Users/jixin.jia@databricks.com/*DevTest/NYC Taxi Trip ETL Job"
            },
            "new_cluster": {
                "spark_version": "6.4.x-scala2.11",
                "spark_conf": {},
                "node_type_id": i,
                "num_workers": n,
                "spark_env_vars": {
                    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                },
                "driver_node_type_id": "Standard_L8s",
                "enable_elastic_disk": True,
                "init_scripts": []
            },
            "timeout_seconds": 10800,
            "max_retries": 1,
            "email_notifications": {
                "on_success": ["jixin.jia@databricks.com"],
                "on_failure": ["jixin.jia@databricks.com"]
            }
        }

        # Construct API call
        request_body = json.dumps(payload)
        shard = 'https://japaneast.azuredatabricks.net/api/'
        endpoint = '2.0/jobs/create'
        url = shard + endpoint
        
        # Make http request
        r = requests.post(url, headers=headers, data=request_body)
        res = r.json()
        if r.status_code == 200:
            print("Created Job '{}'".format(job_name))

        time.sleep(1)