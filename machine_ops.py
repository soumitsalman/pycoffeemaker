import os
import requests
import retry
import argparse
from dotenv import load_dotenv

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR + "/.env")

# @retry(tries=3, delay=60)
def shutdown_td(instance_id, api_key):
    URL = "https://dashboard.tensordock.com/api/v2/instances/{id}/stop"
    res = requests.post(URL.format(instance_id), headers={"Authorization": "Bearer "+api_key})
    res.raise_for_status()
    print(res)

def start_td(instance_id, api_key):
    URL = "https://dashboard.tensordock.com/api/v2/instances/{id}/start"
    res = requests.post(URL.format(instance_id), headers={"Authorization": "Bearer "+api_key})
    res.raise_for_status()
    print(res)

def run(instance, action):
    if instance.lower() == "tensordock":
        id, api_key = os.getenv('TD_INSTANCE_ID'), os.getenv('TD_API_KEY')
        if action == "stop": shutdown_td(id, api_key)
        elif action == "start": start_td(id, api_key)

parser = argparse.ArgumentParser(description="Run the coffee maker application")
parser.add_argument("--action", type=str, help="The action to do")
parser.add_argument("--instance", type=str, help="The instance to act upon")

if __name__ == "__main__":
    args = parser.parse_args()
    run(instance=args.instance, action=args.action)
    