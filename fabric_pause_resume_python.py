# This Python script lets you pause and resume a Microsoft Fabric Capacity.
# It is mainly for learning and demo purposes and does not follow best practices (for example, secret is included directly in the code).
#
# ATTENTION: This script was developed by community users and is provided as-is, without official Microsoft support. 
#
# Note: This version is not set up for Azure Automation.
# For an automation-ready script, see "fabric_pause_resume_azure_automation.py" in the same repository.


import requests

TENANT_ID = "your-tenant-id"
CLIENT_ID = "your-client-id"
CLIENT_SECRET = "your-client-secret"
SUBSCRIPTION_ID = "your-subscription-id"
RESOURCE_GROUP = "your-resource-group"
CAPACITY_NAME = "your-capacity-name"

AZURE_AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
RESOURCE = "https://management.azure.com/"
API_VERSION = "2023-11-01"
FABRIC_BASE_URL = f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Fabric/capacities/{CAPACITY_NAME}"

def get_token():
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "resource": RESOURCE
    }
    response = requests.post(AZURE_AUTH_URL, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

def pause_capacity(token):
    url = f"{FABRIC_BASE_URL}/suspend?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url, headers=headers)
    print(f"Pause status: {response.status_code} - {response.text}")

def resume_capacity(token):
    url = f"{FABRIC_BASE_URL}/resume?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url, headers=headers)
    print(f"Resume status: {response.status_code} - {response.text}")

if __name__ == "__main__":
    token = get_token()
    
    # pause_capacity(token)
    # resume_capacity(token)
