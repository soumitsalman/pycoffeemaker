import os
import requests
import argparse
from utils.env import load_coffeemaker_env

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_coffeemaker_env(CURR_DIR)

def shutdown_td(instance_id, api_key):
    url = f"https://dashboard.tensordock.com/api/v2/instances/{instance_id}/stop"
    res = requests.post(url, headers={"Authorization": f"Bearer {api_key}"})
    res.raise_for_status()
    print(res.text)

def shutdown_az(auth_url: str):
    res = requests.post(auth_url)
    res.raise_for_status()
    print(res.text)

def start_td(instance_id, api_key):
    url = f"https://dashboard.tensordock.com/api/v2/instances/{instance_id}/start"
    res = requests.post(url, headers={"Authorization": f"Bearer {api_key}"})
    res.raise_for_status()
    print(res.text)

def run(provider, action):
    match provider.lower():
        case "tensordock":
            id, api_key = os.getenv('TD_INSTANCE_ID'), os.getenv('TD_API_KEY')
            match action:
                case "stop": shutdown_td(id, api_key)
                case "start": start_td(id, api_key)
        case "azure":
            auth_url = os.getenv('AZ_AUTH_URL')
            match action:
                case "stop": shutdown_az(auth_url)

parser = argparse.ArgumentParser(description="Run the coffee maker application")
parser.add_argument("--action", type=str, required=True, help="The action to do")
parser.add_argument("--provider", type=str, default=None, help="The provider to act upon (default: GPU_PROVIDER)")

if __name__ == "__main__":
    args = parser.parse_args()
    provider = args.provider or os.getenv("GPU_PROVIDER")
    if not provider:
        parser.error("provider required via --provider or GPU_PROVIDER")
    run(provider=provider, action=args.action)
    