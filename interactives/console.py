import json
from icecream import ic
from .tools import InteractSession, InteractiveInputParser
from beanops.datamodels import *

DEFAULT_CTYPE_TO_WRITE="newsletter"
DEFAULT_LIMIT=10
DEFAULT_LAST_N_DAYS=15   
APP_NAME = "Espresso:"

from datetime import datetime as dt
def render(content, prefix: str = None):
    if not content:
        print(APP_NAME, "Nothing found")
    elif isinstance(content, str):
        print(APP_NAME, content)
    elif isinstance(content, dict):
        [render(val, key) for key, val in content.items()]
    elif isinstance(content[0], Nugget):
        print(APP_NAME, len(content), f"Highlights: {prefix or 'All'}")
        for nug in content:
            print("[", dt.fromtimestamp(nug.updated).strftime('%Y-%m-%d'), "] ", nug.trend_score, " | ", nug.digest())
    elif isinstance(content[0], Bean):
        print(APP_NAME, len(content), f"Beans: {prefix or 'All'}")
        for bean in content:
            print("[", dt.fromtimestamp(bean.updated).strftime('%Y-%m-%d'), "] ", bean.source, ":", bean.title)
            print(bean.summary, "\n")

def run_console(session: InteractSession):    
    parser = InteractiveInputParser()
    try:
        # for user_input in ["generative ai", "Donald Trump"]:
        while True:
            args = parser.parse(input("You: "))
            if args[0] == "exit":
                print("Exiting...")
                break
            elif args[0] == "/trending":
                resp = session.trending(args[1], args[2], args[3])
                render(resp)
            elif args[0] == "/lookfor":
                resp = session.search(args[1], args[2], args[3])
                render(resp)
            elif args[0] == "/write":
                for resp in session.write(args[1], args[2], args[3], stream=True):
                    render(resp)
            elif args[0] == "/settings":
                resp = session.configure(args[1], args[2], args[3])
                render(resp)
            else:
                print(APP_NAME, "WTF is this?")
                
    except KeyboardInterrupt:
        print("\nExiting...")
