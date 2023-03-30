import requests
import json
import time
import adal


class Header:
    headers = {
    'Authorization': 'Bearer token',
    'Content-Type': 'application/json'
    }

class Azure:
    clientId="64504f4e-0832-4423-a0f6-41eecd4ec157"
    tenantId="0e00fe3e-023d-4c82-89ad-6c85d5f78f19"
    secretVal="hVP8Q~1uiw5x.G0xxokcALqx8Q5oEsx5B52JXbDq"

def getOrupdateHeader():
    context=adal.AuthenticationContext("https://login.microsoft.com/"+Azure.tenantId)
    token=context.acquire_token_with_client_credentials(resource="https://dev.azuresynapse.net/",client_id=Azure.clientId,client_secret=Azure.secretVal)
    Header.headers["Authorization"] = f"{token['tokenType']} {token['accessToken']}"

def createSparkApp(SparkJobName,SparkPoolName,PythonScriptUrl,config):
    getOrupdateHeader()
    urlCreate = f"https://neom-mkplace-onboarding.dev.azuresynapse.net/sparkJobDefinitions/{SparkJobName}?api-version=2020-12-01"
    urlGet = f"https://neom-mkplace-onboarding.dev.azuresynapse.net/sparkJobDefinitions/{SparkJobName}?api-version=2020-12-01"
    urlSubmit = f"https://neom-mkplace-onboarding.dev.azuresynapse.net/sparkJobDefinitions/{SparkJobName}/execute?api-version=2020-12-01"

    payload = json.dumps({
  "properties": {
    "description": "A sample spark job definition",
    "targetBigDataPool": {
      "referenceName": f"{SparkPoolName}",
      "type": "BigDataPoolReference"
    },
    "requiredSparkVersion": "3.3",
    "language": "python",
    "scanFolder": False,
    "jobProperties": {
      "name": f"{SparkJobName}",
      "file": f"{PythonScriptUrl}",
      "args":[
        f"-c {config}",
      ],
      "numExecutors": 2,
      "executorCores": 4,
      "executorMemory": "5g",
      "driverCores": 4,
      "driverMemory": "5g"
        }
    }
    })
    
    responseCreate = requests.request("PUT", urlCreate, headers=Header.headers, data=payload)
    # print("1:",responseCreate.text)
    if responseCreate.status_code==202:
        GetSuccess = False
        while GetSuccess==False:
            responseGet = requests.request("GET",urlGet,headers=Header.headers)
            print(responseGet.status_code)
            if responseGet.status_code==200:
                GetSuccess = True
            else:
                time.sleep(5)
    # print("1 -",urlCreate,"\n","",Header.headers,"\n",payload,"\n",urlSubmit)
    responseSubmit = requests.request("POST",urlSubmit,headers=Header.headers)
    # print("2:",responseSubmit.text)
    return responseSubmit.text


def getSparkJobDetails(SparkJobName):
    getOrupdateHeader()
    url = "https://neom-mkplace-onboarding.dev.azuresynapse.net/monitoring/workloadTypes/spark/Applications?api-version=2020-12-01"
    response = requests.request("GET",url,headers=Header.headers)
    if response.status_code==401:
        return response.json()
    response = response.json()["sparkJobs"]
    for jobs in response:
        if jobs["name"]==SparkJobName:
            return jobs["state"]
    return "Not Found"
    

