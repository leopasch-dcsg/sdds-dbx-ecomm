import os
from os import access

import requests

client_id = os.environ["SP_CLIENT_ID"]
client_secret = os.environ["SP_CLIENT_SECRET"]
tenant_id = os.environ["SP_TENANT_ID"]
oauth_host = os.environ["SP_OAUTH_HOST"]
oauth_scope = os.environ["SP_OAUTH_SCOPE"]

token_url = f"{oauth_host}/{tenant_id}/oauth2/v2.0/token"

response = requests.post(
    token_url,
    data={
        "grant_type": "client_credentials",
        "scope": oauth_scope,
        "client_id": client_id,
        "client_secret": client_secret
    },
)

if not response.ok:
    raise RuntimeError(f"Failed to get access token: {response.text}")

access_token=response.json()["access_token"]
print(access_token)

#command to set token in terminal
#export JDBC_ACCESS_TOKEN=$(echo $(python devops/set_jdbc_access_token_devlocal.py))