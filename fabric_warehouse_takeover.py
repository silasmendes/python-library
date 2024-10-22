"""
Script to automate the takeover of ownership of a Fabric SQL Data Warehouse using the PBI REST API.

For more details, please refer to the official Microsoft documentation:
https://learn.microsoft.com/en-us/fabric/data-warehouse/change-ownership#take-ownership-of-warehouse

Prerequisites:
- An Azure Service Principal with the necessary permissions.
- The 'msal' and 'requests' Python libraries installed.

Usage:
- Replace the placeholder variables with your actual values.
- Run the script in your Python environment.
"""

import requests
import msal

# Replace these variables with your actual values
client_id = 'your_client_id'           # Application (client) ID of your Azure AD app
client_secret = 'your_client_secret'   # Client secret of your Azure AD app
tenant_id = 'your_tenant_id'           # Directory (tenant) ID
workspace_id = 'your_workspace_id'     # The workspace ID
warehouse_id = 'your_warehouse_id'     # The warehouse ID

# Authority URL
authority_url = f'https://login.microsoftonline.com/{tenant_id}'

# The scope required for Power BI REST API
scope = ['https://analysis.windows.net/powerbi/api/.default']

# Create a ConfidentialClientApplication instance
app = msal.ConfidentialClientApplication(
    client_id=client_id,
    client_credential=client_secret,
    authority=authority_url
)

# Acquire a token for the app
result = app.acquire_token_for_client(scopes=scope)

if 'access_token' in result:
    access_token = result['access_token']
    
    # Set up the request headers
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    
    # Construct the API URL
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datawarehouses/{warehouse_id}/takeover'
    
    # Make the POST request to invoke warehouse takeover
    response = requests.post(url, headers=headers)
    
    # Check the response
    if response.status_code in [200, 202]:
        print('Warehouse takeover initiated successfully.')
    else:
        print(f'Failed to initiate warehouse takeover. Status code: {response.status_code}')
        print(f'Response: {response.text}')
else:
    print('Failed to acquire access token.')
    print(f'Error: {result.get("error")}')
    print(f'Description: {result.get("error_description")}')
