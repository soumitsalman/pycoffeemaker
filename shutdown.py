import os
import requests
import retry
from dotenv import load_dotenv

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR + "/.env")

# @retry(tries=3, delay=60)
def shutdown_td(instance_id, api_key):
    URL = "https://dashboard.tensordock.com/api/v2/instances/{id}/start"
    res = requests.post(URL.format(instance_id), headers={"Authorization": "Bearer "+api_key})
    res.raise_for_status()
    print(res)

def run():
    if os.getenv("INSTANCE").lower() == "tensordock":
        shutdown_td(os.getenv('INSTANCE_ID'), os.getenv('INSTANCE_API_KEY'))

if __name__ == "__main__":
    run()
    