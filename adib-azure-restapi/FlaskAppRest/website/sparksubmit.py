import requests
import json
import time
import adal
import oci
import datetime
import requests

class Oci:
    config = oci.config.from_file("./oci/config")
    # config = {'log_requests': False,
    #             'additional_user_agent': '',
    #             'pass_phrase': None,
    #             'user': 'ocid1.user.oc1..aaaaaaaatvngq2664jmcr3uxbxos2x53zf5fnki67vn3icedt6ovlxcfz4ra',
    #             'fingerprint': '7a:e3:7c:e3:ee:f7:79:4c:39:92:6d:e1:be:bf:4e:7d',
    #             'key_file': "./oci/oci_api_key.pem",
    #             'tenancy': 'ocid1.tenancy.oc1..aaaaaaaalaagcgcry5jzbruizivwrby2ah6x4owtht5nvqc67piupviktaca',
    #             'region': 'ap-hyderabad-1'
    #         }
    object_storage_client = oci.object_storage.ObjectStorageClient(config)
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
    

def createPreAuthUrl():
    bucket_name = "bucket-hrms-1"
    bucket = None
    try:
        bucket = Oci.object_storage_client.create_bucket(
            namespace_name=Oci.object_storage_client.get_namespace().data,
            create_bucket_details=oci.object_storage.models.CreateBucketDetails(
                name=bucket_name,
                compartment_id="ocid1.compartment.oc1..aaaaaaaahiweveqst5c7stdbgigrsdwr6ozpltor2jgwucwfbmqtupm3xyza"
            )
        )
    except oci.exceptions.ServiceError as e:
        if e.status == 409 and "BucketAlreadyExists" in str(e):
            bucket = Oci.object_storage_client.get_bucket(
                namespace_name=Oci.object_storage_client.get_namespace().data,
                bucket_name=bucket_name
            )
            print("Bucket already exists")
        else:
            print("Some error : ",e)
    expiration_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=5)
    preauth_req = oci.object_storage.models.CreatePreauthenticatedRequestDetails(
                                                                    name='upload_csv_par',
                                                                    object_name="data.csv",
                                                                    access_type='ObjectWrite',
                                                                    time_expires=expiration_time,
                                                                )
    preauth_response = Oci.object_storage_client.create_preauthenticated_request(bucket.data.namespace,bucket_name,preauth_req)
    if preauth_response.status!=200:
        return "Url didn't get created"
    preauth_url = 'https://objectstorage.ap-hyderabad-1.oraclecloud.com' + preauth_response.data.access_uri
    return preauth_url