from typing_extensions import Self
from nicegui import ui
from nicegui.binding import BindableProperty, bind_from
from typing import cast
from datetime import datetime as dt
from itertools import groupby
from icecream import ic

date_to_str = lambda date: dt.fromtimestamp(date).strftime('%a, %b %d')

def groupby_date(items: list, date_field):
    flatten_date = lambda item: dt.fromtimestamp(date_field(item)).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    return {date_val: list(group_items) for date_val, group_items in groupby(items, flatten_date)}

class BindableTimeline(ui.timeline):
    items = BindableProperty(on_change=lambda sender, value: cast(Self, sender)._render())

    def __init__(self, item_render_func, items: list|dict = None, date_field = lambda x: x['date'], header_field: str = lambda x: x["name"], groupby_time: bool = False):
        super().__init__()
        self.items = items
        self.date_field = date_field
        self.header_field = header_field
        self.render_item = item_render_func
        self.groupby_time = groupby_time
        self._render()
    
    def _render(self):    
        self.clear()
        items = self.items or []           
        with self:
            if not self.groupby_time:
                for item in items:
                    date = self.date_field(item)
                    with ui.timeline_entry(
                        title=self.header_field(item), 
                        subtitle=(date_to_str(date) if isinstance(date, (int, float)) else date)):
                        self.render_item(item)
            else:                
                for date, group in groupby_date(items, self.date_field).items():
                    with ui.timeline_entry(
                        title=", ".join(self.header_field(item) for item in group), 
                        subtitle=(date_to_str(date) if isinstance(date, (int, float)) else date)):
                        for item in group:
                            self.render_item(item)
        self.update()

    def bind_items_from(self, target_object, target_name: str = 'items', backward = lambda x: x) -> Self:
        bind_from(self, "items", target_object, target_name, backward)
        return self

class BindableList(ui.list):
    items = BindableProperty(on_change=lambda sender, value: cast(Self, sender)._render(value))

    def __init__(self, item_render_func, items: list = None):
        super().__init__()
        self.items = items or []
        self.render_item = item_render_func
        self._render(self.items)
    
    def _render(self, value):    
        self.clear()    
        with self:
            for item in (value or []):                    
                with ui.item():
                    self.render_item(item)
        self.update()

    def bind_items_from(self, target_object, target_name: str = 'items', backward = lambda x: x) -> Self:
        bind_from(self, "items", target_object, target_name, backward)
        return self
    
class BindableGrid(ui.grid):
    items = BindableProperty(
        on_change=lambda sender, value: cast(Self, sender)._render(value)
    )

    def __init__(self, item_render_func, items: list = None, rows: int = None, columns: int = None):     
        super().__init__(rows = rows, columns = columns)             
        self.items = items
        self.render_item = item_render_func    
        self._render(self.items)
   
    def _render(self, value):  
        self.clear() 
        with self:            
            for item in (value or []):
                self.render_item(item)
        self.update()

    def bind_items_from(self, target_object, target_name: str = 'items', backward = lambda x: x) -> Self:
        bind_from(self, "items", target_object, target_name, backward)
        return self
    

class HighlightableItem(ui.item):
    highlight = BindableProperty(
        on_change=lambda sender, value: cast(Self, sender)._render()
    )

    def __init__(self, highlight_style: str, highlight: bool = False, **kwargs):               
        self.highlight = highlight
        self.highlight_style = highlight_style
        super().__init__(**kwargs)        
        self._render()
   
    def _render(self):  
        # self.clear() 
        self.style(add=self.highlight_style) if self.highlight else self.style(remove=self.highlight_style)  

    def bind_highlight_from(self, target_object, target_name: str = 'highlight', backward = lambda x: x) -> Self:
        bind_from(self, "highlight", target_object, target_name, backward)
        return self