import gradio as gr

INTERACTIVE_MODEL = "gpt-3.5-turbo-0125"

def process_prompt(prompt, thread):
    return "Howdy "+prompt

chat = gr.ChatInterface(
    fn = process_prompt, 
    examples=["What is trending today?", "Create a summary of news on Cybersecurity"],
    title="Espresso")

chat.launch()