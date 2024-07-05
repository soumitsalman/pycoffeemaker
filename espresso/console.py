## THIS IS MOSTLY AN EXAMPLE OF HOW TO USE INTERACTIVE ##
from icecream import ic

from .interactives import tools, settings
from beanops.datamodels import *

DEFAULT_CTYPE_TO_WRITE="newsletter"
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

def run_console(db_conn, llm_api_key, embedder_path): 
    tools.initiatize_tools(db_conn, llm_api_key, embedder_path)
    session = settings.Settings()

    try:
        # for user_input in ["generative ai", "Donald Trump"]:
        while True:
            args = tools.parse_prompt(input("You: "))
            if args[0] == "exit":
                print("Exiting...")
                break
            elif args[0] == "trending":
                resp = tools.highlights(args[1], args[2], args[3])
                render(resp)
            elif args[0] == "lookfor":
                resp = tools.search(args[1], args[2], args[3])
                render(resp)
            elif args[0] == "write":
                for resp in tools.write(args[1], args[2], args[3], stream=True):
                    render(resp)
            elif args[0] == "settings":
                resp = session.update(args[1], args[2], args[3])
                render(resp)
            else:
                print(APP_NAME, "WTF is this?")
                
    except KeyboardInterrupt:
        print("\nExiting...")

# if __name__ == "__main__":
#     run_console(db_conn, llm_api_key)