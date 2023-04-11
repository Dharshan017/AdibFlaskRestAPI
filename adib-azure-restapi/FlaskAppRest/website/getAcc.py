# # # Replace <subscription_id> with your subscription ID
# # subscription_id = "93606994-db8e-418a-96eb-7714d1f177ce"
# # # Replace <resource_group> with the name of your resource group
# # resource_group = "synapseworkspace-managedrg-4aee9442-41a0-454b-b36a-29fc0e64b856"
# # # Replace <workspace> with the name of your Synapse workspace
# # workspace = "adib-work"


# from azure.identity import EnvironmentCredential
# import requests
# import os

# credential = EnvironmentCredential(client_id=os.getenv("AZURE_CLIENT_ID"),username="dharshan172001@gmail.com",password="Dharshan017#")


# token = credential.get_token("https://dev.azuresynapse.net/.default")

# headers = {
#     "Authorization": "Bearer " + token.token,
#     "Content-Type": "application/json"
# }

# # response = requests.get("https://dev.azuresynapse.net/", headers=headers)
# print(token)
