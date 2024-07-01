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
        return text+ "\n".join([f"📅 {dt.fromtimestamp(nug.updated).strftime('%Y-%m-%d')} {nug.digest()}\n" for nug in content])
    elif isinstance(content[0], Bean):
        text = f"## {len(content)} news/posts: {prefix or 'All'}\n"
        beanformat = lambda bean: f"📅 {dt.fromtimestamp(bean.updated).strftime('%Y-%m-%d')} [{bean.source}]({bean.url})\n### {bean.title}\n{bean.summary}\n"
        return text + "\n---\n".join([beanformat(bean) for bean in content])

def create_interface(db_conn, llm_api_key):    
    parser = InteractiveInputParser()

    def process_prompt(prompt: str, session: InteractSession, ndays_slider):
        task, query, ctype, ndays, topn  = parser.parse(prompt)

        ndays = ndays or ndays_slider # the command line value takes precedence 
        # topn = topn or topn_slider

        if task == "trending":
            resp = session.trending(query, ctype, ndays)
            output = render(resp)
        elif task == "lookfor":
            resp = session.search(query, ctype, ndays)
            output = render(resp)
        elif task == "write":
            resp = session.write(query, ctype, ndays)
            output = render(resp)
        elif task == "settings":
            resp = session.configure(query, ctype, ndays)
            output = render(resp)
        else:
            output = "WTF is this?"
        return output, session

    return gr.Interface(
        fn = process_prompt,         
        inputs=[gr.Textbox(placeholder="Tell me lies, tell me sweet little lies"), gr.State(value=InteractSession(Beansack(conn_str=db_conn), llm_api_key))],
        outputs=[gr.Markdown(), gr.State()],
        examples=[
            ["trending -q \"spacex and rocket launch, GPU\"", 2], 
            ["trending -q robotics -t news -d 4", 1], 
            ["lookfor -q \"cyber security breach and incidents\" -d 7 -n 5", 15],
            ["write -q \"generative AI safety\"", 2]
        ],
        additional_inputs=[
            gr.Slider(minimum=1, maximum=30, value=2, label="Last N Days", key="last_n_days_slider")
        ],
        title="Espresso",
        theme="glass"
    )

 
if __name__ == "__main__":
    create_interface(db_conn, llm_api_key).launch(share=True)        

