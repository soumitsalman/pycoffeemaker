from icecream import ic
import os
from dotenv import load_dotenv
from shared import utils

if __name__ == "__main__":
    load_dotenv()
    instance_mode = os.getenv("INSTANCE_MODE")
    db_conn = os.getenv('DB_CONNECTION_STRING')
    llm_api_key = os.getenv("DEEPINFRA_API_KEY")   
    
    logger = utils.create_logger("web UI")

from beanops.beansack import Beansack
from interactives.tools import InteractSession, InteractiveInputParser
from beanops.datamodels import *
from datetime import datetime as dt
import gradio as gr
from datetime import datetime as dt

def render(content, prefix: str = None):
    if not content:
        return "Nothing found"
    elif isinstance(content, str):
        return content
    elif isinstance(content, dict):
        return "\n\n".join([render(val, key) for key, val in content.items()]) 
    elif isinstance(content[0], Nugget):
        text = f"## {len(content)} highlights: {prefix or 'All'}\n"
        return text+ "\n".join([f"- 📅 {dt.fromtimestamp(nug.updated).strftime('%Y-%m-%d')} - {nug.digest()}" for nug in content])
    elif isinstance(content[0], Bean):
        text = f"## {len(content)} news/posts: {prefix or 'All'}\n"
        beanformat = lambda bean: f"📅 {dt.fromtimestamp(bean.updated).strftime('%Y-%m-%d')} [{bean.source}]({bean.url})\n### {bean.title}\n{bean.summary}\n"
        return text + "\n---\n".join([beanformat(bean) for bean in content])

def create_interface(db_conn, llm_api_key):
    session = InteractSession(Beansack(conn_str=db_conn), llm_api_key)
    parser = InteractiveInputParser()

    def process_prompt(prompt, thread):
        args = parser.parse(prompt)
        if args[0] == "/trending":
            resp = session.trending(args[1], args[2], args[3])
            return render(resp)
        elif args[0] == "/lookfor":
            resp = session.search(args[1], args[2], args[3])
            return  render(resp)
        elif args[0] == "/write":
            resp = session.write(args[1], args[2], args[3])
            return render(resp)
        elif args[0] == "/settings":
            resp = session.configure(args[1], args[2], args[3])
            return render(resp)
        else:
            return "WTF is this?"

    return gr.ChatInterface(
        fn = process_prompt,         
        textbox=gr.Textbox(placeholder="Tell me lies, tell me sweet little lies"),
        examples=["/trending -q \"spacex and rocket launch, GPU\"", "/lookfor -q \"cyber security breach and incidents\" -d 7", "/write -q \"generative AI safety\""],
        title="Espresso",
        theme="soft"
    )

 
if __name__ == "__main__":
    create_interface(db_conn, llm_api_key).launch(share=True)        

