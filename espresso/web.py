from beanops.datamodels import *
from espresso.custom_ui import *
from .interactives import tools, settings
from nicegui import ui
from icecream import ic
from datetime import datetime as dt


APP_NAME="Espresso by Cafecit.io"

EDITORIAL_PAGE = "Editorial"
TRENDING_PAGE = "Trending"
ENGAGEMENTS_PAGE = "Social Media"
CONSOLE_PAGE = "Console"
CHAT_PAGE = "(Beta) Chat"

EDITORIAL_HEADER = "Espresso Founder's Picks"
TRENDING_HEADER = "Trending News, Blogs & Posts"
ENGAGEMENTS_HEADER = "Your Social Media Stats"
CONSOLE_HEADER = "Espresso Console"
CHAT_HEADER = "(Beta) Chat with Espresso"

NO_ACTION = "I don't really do much. I just sit here and look pretty."
NOTHING_FOUND = "Couldn't find anything"
NOTHING_TRENDING = "Nothing Trending."

DEFAULT_LIMIT=10
DEFAULT_LAST_NDAYS=15

session_settings: settings.Settings = None

latest = lambda item: -item.updated
nugget_markdown = lambda nugget: f"**{nugget.keyphrase}**: {nugget.description}" if nugget else None


def get_editor_categories():
    return ["Cybersecurity", "Generative AI", "Robotics", "Space and Rockets", "Politics", "Yo Momma"]

def get_user_categories(user_id):
    return session_settings.topics or []

F_NAME = "header"
F_NUGGETS = "nuggets"
F_SELECTED = "selected"
F_BEANS = "beans"

def render_bean_as_card(bean: Bean):
    def banner_markdown():
        banners = []
        if bean.source:
            banners.append(f"🔗 [{bean.source}]({bean.url})")   
        if bean.created:
            banners.append(f"📅 {dt.fromtimestamp(bean.created).strftime('%a, %b %d')}") 
        if bean.author:
            banners.append(f"✍️ {bean.author}")
        if bean.noise and bean.noise.comments:
            banners.append(f"💬 {bean.noise.comments}")
        if bean.noise and bean.noise.likes:
            banners.append(f"👍 {bean.noise.likes}")
        return " ".join(banners)
    if bean:
        with ui.card() as card:
            ui.markdown(banner_markdown()).classes('text-caption')
            ui.markdown(f"**{bean.title}**")
            ui.markdown(bean.summary)
            if bean.tags:
                with ui.row().classes("gap-0"):
                    [ui.chip(word, on_click=lambda : ui.notify(NO_ACTION)).props('outline square') for word in bean.tags[:3]]
        return card

def render_nugget_as_card(nugget: Nugget):
    if nugget:
        with ui.card() as card:
            if nugget.urls:
                ui.markdown(f"🗞️ {len(nugget.urls)}").classes('text-caption')
            ui.markdown(nugget.digest())
            if "keywords" in nugget:
                with ui.row().classes("gap-0"):
                    [ui.chip(word, on_click=lambda : ui.notify(NO_ACTION)).props('outline square') for word in (nugget['keywords'] or [])[:3]]
        return card

def render_nuggets(category: dict):
    def on_select_nugget(nugget):
        category[F_SELECTED] = nugget
        category[F_BEANS] = sorted(tools.get_beans_for_nugget(session_settings, nugget_id=nugget['data'].id, content_type=settings.ContentType.NEWS), key = latest)
        # clear the other nuggets
        for nug in category.get(F_NUGGETS) or []:
            nug[F_SELECTED] = (nug == nugget)

    def render_nugget_as_timeline_item(nugget: dict):
        with HighlightableItem("background-color: lightblue; padding: 15px; border-radius: 4px;", on_click=lambda nugget=nugget: on_select_nugget(nugget)).props("clickable").classes("w-full").bind_highlight_from(nugget, F_SELECTED) as item:            
            ui.markdown(nugget_markdown(nugget['data']))
        return item

    return BindableTimeline(date_field=lambda nug: nug['data'].updated, header_field=lambda nug: nug['data'].keyphrase, item_render_func=render_nugget_as_timeline_item).props("side=right").bind_items_from(category, "nuggets")

def render_beans(category: dict):    
    return BindableList(category.get(F_BEANS, []), render_bean_as_card).bind_items_from(category, F_BEANS)

def category_tab(category: dict):
    with ui.tab(category[F_NAME], label=category[F_NAME]) as tab:                
        if "nuggets" not in category:
            nuggets = sorted(tools.trending(
                session_settings=session_settings, 
                topics=category["header"], 
                content_type=settings.ContentType.HIGHLIGHTS), key = latest)         
            category[F_NUGGETS] = [{'data': item} for item in nuggets]
        n_count = len(category.get(F_NUGGETS))       
        if n_count:
            ui.badge(str(n_count)).props("floating transparent")
    return tab

def category_panel(category: dict):
    with ui.tab_panel(category[F_NAME]) as panel:
        with ui.row().style('display: noflex; flex-wrap: nowrap;'):
            render_nuggets(category).classes("w-full").style('flex: 1;')           
            render_beans(category).classes("w-full").style('flex: 1;')
    return panel

def trending_page(viewmodel: dict):
    if not viewmodel.get("categories"):
        viewmodel["categories"] = {cat: {F_NAME:cat} for cat in get_user_categories(None)+get_editor_categories()}.values()

    with ui.tabs().bind_value(viewmodel, "selected") as tabs:    
        for category in viewmodel["categories"]:
            category_tab(category)

    with ui.tab_panels(tabs):
        for category in viewmodel["categories"]:
            category_panel(category)
        

EXAMPLE_OPTIONS = ["trending -t posts -q \"cyber security breches\"", "lookfor -q \"GPU vs LPU\"", "settings -d 7 -n 20"]   
PLACEHOLDER = "Tell me lies, sweet little lies"

def render_prompt_response(resp):
    if isinstance(resp, str):
        ui.markdown(resp)
    elif isinstance(resp, Bean):
        render_bean_as_card(resp) 
    elif isinstance(resp, Nugget):
        render_nugget_as_card(resp)
    
def console_page(viewmodel):
    def process_prompt():
        if viewmodel['prompt']:  
                    
            task, query, ctype, ndays, topn = tools.parse_prompt(viewmodel['prompt'])
            if task == "trending":
                resp = tools.trending(session_settings, query, ctype, ndays)                
            elif task == "lookfor":
                resp = tools.search(session_settings, query, ctype, ndays)
            elif task == "write":
                resp = tools.write(session_settings, query, ctype, ndays)
            elif task == "settings":
                resp = [session_settings.update(query, ctype, ndays, topn)]             
            else:
                # make it search all
                resp = tools.search_all(session_settings, viewmodel['prompt'])

            viewmodel['response_banner'] = f"{len(resp)} results found" if resp else NOTHING_FOUND
            viewmodel["last_response"] = resp 
            # viewmodel["prompt"] = None
        
    with ui.input(placeholder=PLACEHOLDER, autocomplete=EXAMPLE_OPTIONS).bind_value(viewmodel, "prompt") \
        .props('rounded outlined input-class=mx-3').classes('w-full self-center').on('keydown.enter', process_prompt) as prompt_input:
        ui.button(icon="send", on_click=process_prompt).bind_visibility_from(prompt_input, 'value').props("flat dense")
    ui.label("Examples: "+", ".join(EXAMPLE_OPTIONS)).classes('text-caption self-center')
    ui.label().bind_text_from(viewmodel, "response_banner").classes("text-bold")
    BindableGrid(render_prompt_response, columns=3).bind_items_from(viewmodel, "last_response")

def settings_panel(viewmodel):    
    with ui.list():
        ui.item_label('Search Settings').classes("text-subtitle1")
        with ui.item():
            with ui.item_section().bind_text_from(viewmodel, "last_ndays", lambda x: f"Last {x} days"):
                ui.slider(min=1, max=30, step=1).bind_value(viewmodel, "last_ndays")
        with ui.item():
            with ui.item_section().bind_text_from(viewmodel, "topn", lambda x: f"Top {x} results"):
                ui.slider(min=1, max=50, step=1).bind_value(viewmodel, "topn")
        with ui.item():
            with ui.item_section("Content Types"):
                ui.select(options=viewmodel['content_types'], multiple=True).bind_value(viewmodel, 'content_types').props("use-chips")
        with ui.item():
            with ui.item_section("Sources"):
                ui.select(options=viewmodel['sources'], with_input=True, multiple=True).bind_value(viewmodel, 'sources').props("use-chips")
    
    ui.separator()

    with ui.column(align_items="stretch"):
        ui.label('Connections').classes("text-subtitle1")
        ui.switch(text="Slack")
        ui.switch(text="Reddit")
        ui.switch(text="LinkedIn")


def run_web(db_conn, llm_api_key, embedder_path):
    tools.initiatize_tools(db_conn, llm_api_key, embedder_path)
    global session_settings
    session_settings = settings.Settings(topics=["Space and Rocket Launch", "Generative AI", "Tesla & Cybertruck", "Miami Events"], last_ndays=7, limit=5)

    ui.add_css(content="""
        @import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;700&display=swap');
            
        body {
            font-family: 'Open Sans', sans-serif;
            color: #1A1A1A;        
        }
    """)

    with ui.header().classes(replace="row items-center"):
        with ui.avatar():
            ui.image("espresso/images/cafecito.png")
        with ui.tabs() as page_tabs:
            trending_news_tab = ui.tab("Trending News", icon="trending_up")
            console_tab = ui.tab("Console", icon="terminal")
        ui.space()
        ui.label(APP_NAME).classes("text-h6")
        ui.space()
        ui.button(on_click=lambda: settings_drawer.toggle(), icon="settings").props('flat color=white').classes("self-right")

    with ui.tab_panels(page_tabs, value = trending_news_tab).classes("w-full"):
        with ui.tab_panel(trending_news_tab):
            trending_page({"selected": None, "categories": None})
        with ui.tab_panel(console_tab):
            console_page({"prompt": None})

    with ui.right_drawer(elevated=True, value=False) as settings_drawer:
        settings_panel({"last_ndays": session_settings.last_ndays, "topn": session_settings.limit, "content_types": ["news", "posts", "comments", "highlights"], "sources": ["All"]})

    ui.run(title=APP_NAME, favicon="espresso/images/cafecito-ico.png")
