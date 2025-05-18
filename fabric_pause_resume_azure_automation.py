# This Python script allows you to pause and resume a Microsoft Fabric Capacity using Azure Automation.
#
# ATTENTION: This script was developed by community users and is provided as-is, without official Microsoft support. 
#            Be sure to test it thoroughly before using in production.
#
# To use this script, you must create the following variables in your Azure Automation account (case-sensitive names):
#
#   TENANT_ID         (string)
#   CLIENT_ID         (string)
#   CLIENT_SECRET     (string, secret)
#   SUBSCRIPTION_ID   (string)
#   RESOURCE_GROUP    (string)
#   CAPACITY_NAME     (string)
#
# For secrets, mark them as “encrypted” in the Automation Account.
# To add these variables: In the Automation Account, go to "Variables" under "Shared Resources".


import automationassets
import requests
import sys

TENANT_ID = automationassets.get_automation_variable("TENANT_ID")
CLIENT_ID = automationassets.get_automation_variable("CLIENT_ID")
CLIENT_SECRET = automationassets.get_automation_variable("CLIENT_SECRET")
SUBSCRIPTION_ID = automationassets.get_automation_variable("SUBSCRIPTION_ID")
RESOURCE_GROUP = automationassets.get_automation_variable("RESOURCE_GROUP")
CAPACITY_NAME = automationassets.get_automation_variable("CAPACITY_NAME")

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
    if len(sys.argv) < 2:
        print("Usage: python fabric_capacity_control.py [pause|resume]")
        sys.exit(1)

    token = get_token()
    if sys.argv[1].lower() == "pause":
        pause_capacity(token)
    elif sys.argv[1].lower() == "resume":
        resume_capacity(token)
    else:
        print("Invalid argument. Use 'pause' or 'resume'.")