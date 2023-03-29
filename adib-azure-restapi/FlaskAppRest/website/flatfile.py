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

def createPreAuthUrl(uploadedFileName):
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
                                                                    object_name=uploadedFileName,
                                                                    access_type='ObjectWrite',
                                                                    time_expires=expiration_time,
                                                                )
    preauth_response = Oci.object_storage_client.create_preauthenticated_request(bucket.data.namespace,bucket_name,preauth_req)
    if preauth_response.status!=200:
        return "Url didn't get created"
    preauth_url = 'https://objectstorage.ap-hyderabad-1.oraclecloud.com' + preauth_response.data.access_uri
    return preauth_url