import json
from typing import Optional
from pydantic import BaseModel, Field
from .utils import *

M_GIST = "# GIST"
M_CATEGORIES = "# DOMAINS"
M_ENTITIES = "# ENTITIES"
M_TOPIC = "# TOPIC"
M_REGIONS = "# REGIONS"
M_SUMMARY = "# SUMMARY"
M_KEYPOINTS = "# KEY POINTS"
M_KEYEVENTS = "# KEY EVENTS"
M_DATAPOINTS = "# KEY POINTS"
M_INSIGHT = "# ACTIONABLE INSIGHT"
M_FIELDS = [M_GIST, M_CATEGORIES, M_ENTITIES, M_TOPIC, M_REGIONS, M_SUMMARY, M_KEYPOINTS, M_KEYEVENTS, M_DATAPOINTS, M_INSIGHT]
M_START = "```markdown"
M_END="```"
MARKDOWN_HEADERS = ["# ", "## ", "### ", "#### ", "**"]

C_KEYPOINTS = "P:"
C_KEYEVENTS = "E:"
C_DATAPOINTS = "D:"
C_REGIONS = "R:"
C_ENTITIES = "N:"
C_CATEGORIES = "C:"
C_SENTIMENTS = "S:"
COMPRESSED_FIELDS = [C_KEYPOINTS, C_KEYEVENTS, C_DATAPOINTS, C_REGIONS, C_ENTITIES, C_CATEGORIES, C_SENTIMENTS]
UNDETERMINED = ["n/a", "none", "undetermined", "not specified", "not mentioned"]

clean_up = lambda items: list(filter(lambda x: x.lower() not in UNDETERMINED, distinct_items(items)))

class Digest(BaseModel):
    raw: str
    # expr: Optional[str] = Field(default="")
    keypoints: Optional[list[str]] = Field(default=[])
    keyevents: Optional[list[str]] = Field(default=[])
    datapoints: Optional[list[str]] = Field(default=[])
    categories: Optional[list[str]] = Field(default=[])
    entities: Optional[list[str]] = Field(default=[])
    regions: Optional[list[str]] = Field(default=[])
    sentiments: Optional[list[str]] = Field(default=[])

    def parse_json(response: str):  
        try:      
            response = json.loads(response[response.find('{'):response.rfind('}')+1])
            return Digest(
                summary=response.get('summary'),
                highlights=response.get('keypoints'),
                title=response.get('gist'),
                names=distinct_items(split_parts(response.get('entities'))),
                domains=distinct_items(split_parts(response.get('categories'))),
            )
        except json.JSONDecodeError: return

    def parse_markdown(response: str):
        digest = Digest()
        response = response.strip().removeprefix(M_START).removesuffix(M_END).strip()
        last = None
        for line in response.splitlines():
            line = line.strip()
            if not line or line == UNDETERMINED: continue

            if any(field in line for field in M_FIELDS):
                last = line
            elif M_GIST in last:
                digest.gist = line
            elif M_CATEGORIES in last:
                digest.categories = split_parts(line)
            elif C_ENTITIES in last:
                digest.entities = split_parts(line)
            elif M_TOPIC in last:
                digest.topic = line 
            elif C_REGIONS in last:
                digest.regions = split_parts(line)
            elif M_SUMMARY in last:
                digest.summary = (digest.summary+"\n"+line) if digest.summary else line
            elif C_KEYPOINTS in last:
                if not digest.keypoints: digest.keypoints = []
                digest.keypoints.append(line.removeprefix("- ").removeprefix("* "))
            elif M_INSIGHT in last:
                digest.insight = line

        return digest   

    def parse_compressed(response: str):
        response = response.strip()
        if not response: return
        
        digest = Digest(raw = response)
        parts = [part.strip() for part in split_parts(response, r'[;\|\n]+') if part != UNDETERMINED]
        add_to = None
        for part in parts:
            prefix = next((field for field in COMPRESSED_FIELDS if part.startswith(field)), None)

            if prefix:
                part = part.removeprefix(prefix)
                add_to = prefix
            
            if add_to == C_REGIONS:
                if isalphaorspace(part): digest.regions.append(part)
            elif add_to == C_ENTITIES:
                digest.entities.append(part)
            elif add_to == C_CATEGORIES:
                if isalphaorspace(part): digest.categories.append(part)
            elif add_to == C_SENTIMENTS:
                if part.isalpha(): digest.sentiments.append(part)

        digest.regions = clean_up(digest.regions)
        digest.entities = clean_up(digest.entities)
        digest.categories = clean_up(digest.categories)
        digest.sentiments = clean_up(digest.sentiments)

        return digest

M_INTRODUCTION = ["# Introduction", "## Introduction"]
M_ANALYSIS = ["## Analysis"]
M_TAKEAWAYS = ["## Key Datapoints", "## Key Takeaways", "## Key Trends & Insights"]
M_VERDICT = ["## Verdict", "## Conclusion"]
M_KEYWORDS = ["## Keywords"]
class GeneratedArticle(BaseModel):
    raw: str
    title: str = Field(default=None)
    intro: list[str] = Field(default=[])
    analysis: list[str] = Field(default=[])
    insights: list[str] = Field(default=[])
    verdict: list[str] = Field(default=[])
    keywords: Optional[list[str]] = None

    def parse_markdown(text: str):
        text = text.strip().removeprefix(M_START).removesuffix(M_END).strip()
        response = GeneratedArticle(raw=text)
        
        lines = text.splitlines()
        response.title = remove_before(lines[0].removeprefix("##").removeprefix("#"), "Title:").strip()
        add_to = None
        for line in lines[1:]:
            line = line.strip()

            if line in M_INTRODUCTION: add_to = M_INTRODUCTION
            elif line in M_ANALYSIS: add_to = M_ANALYSIS
            elif line in M_TAKEAWAYS: add_to = M_TAKEAWAYS
            elif line in M_VERDICT: add_to = M_VERDICT
            elif line in M_KEYWORDS: add_to = M_KEYWORDS

            elif add_to == M_INTRODUCTION: response.intro.append(line)
            elif add_to == M_ANALYSIS: response.analysis.append(line)
            elif add_to == M_TAKEAWAYS: response.insights.append(line)
            elif add_to == M_VERDICT: response.verdict.append(line)
            elif add_to == M_KEYWORDS: response.keywords = [kw.strip().removesuffix('.') for kw in line.split(',')]

        return response   




# def cleanup_markdown(text: str) -> str:
#     # remove all \t with
#     text = text.replace("\t", "")
    
#     # removing the first line if it looks like a header
#     text = text.strip()
#     if any(text.startswith(tag) for tag in MARKDOWN_HEADERS):
#         text = remove_before(text, "\n") 

#     # replace remaining headers with "**"
#     text = re.sub(r"(#+ )(.*?)(\n|$)", replace_header_tag, text)
#     # Replace "\n(any number of spaces)\n" with "\n\n"
#     text = re.sub(r"\n\s*\n", "\n\n", text)
#     # # Remove any space after "\n"
#     # text = re.sub(r"\n\s+", "\n", text)
#     # Replace "\n\n\n" with "\n\n"
#     # text = re.sub(r"\n\n\n", "\n\n", text)
#     # # remove > right after \n
#     # text = re.sub(r"\n>", "\n", text)
#     # replace every single \n with \n\n
#     text = re.sub(r'(?<!\n)\n(?!\n)', '\n\n', text)
#     # Add a space after every "+" if there is no space
#     text = re.sub(r'\+(?!\s)', '+ ', text)

#     return text.strip()

# def replace_header_tag(match):
#     header_content = match.group(2).strip()  # The content after "# " or "## "
#     newline = match.group(3)  # Preserve the newline or end of string
#     return f"\n**{header_content}**{newline}"