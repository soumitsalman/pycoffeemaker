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
UNDETERMINED = "N/A"
MARKDOWN_HEADERS = ["# ", "## ", "### ", "#### ", "**"]

C_KEYPOINTS = "P:"
C_KEYEVENTS = "E:"
C_DATAPOINTS = "D:"
C_REGIONS = "R:"
C_ENTITIES = "N:"
C_CATEGORIES = "C:"
C_SENTIMENTS = "S:"
COMPRESSED_FIELDS = [C_KEYPOINTS, C_KEYEVENTS, C_DATAPOINTS, C_REGIONS, C_ENTITIES, C_CATEGORIES, C_SENTIMENTS]

class Digest(BaseModel):
    expr: str
    keypoints: Optional[list[str]] = Field(default=None)
    keyevents: Optional[list[str]] = Field(default=None)
    datapoints: Optional[list[str]] = Field(default=None)
    categories: Optional[list[str]] = Field(default=None)
    entities: Optional[list[str]] = Field(default=None)
    regions: Optional[list[str]] = Field(default=None)
    sentiments: Optional[list[str]] = Field(default=None)

    gist: Optional[str] = Field(default=None)
    topic: Optional[str] = Field(default=None)
    summary: Optional[str] = Field(default=None)
    insight: Optional[str] = Field(default=None)  

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

    def parse_compressed_digest(response: str):
        response = response.strip()
        if not response: return
        
        digest = Digest(expr = "")
        parts = [part.strip() for part in split_parts(response, r'[;\|\n]+') if part != UNDETERMINED]
        last = None
        for part in parts:
            prefix = next((field for field in COMPRESSED_FIELDS if part.startswith(field)), None)

            if prefix:
                part = part.removeprefix(prefix)
                last = prefix
                if part == UNDETERMINED: continue # skip
                digest.expr += f";{prefix+part}" if digest.expr else prefix+part
            else:
                digest.expr += f"|{part}"
            
            if last == C_REGIONS:
                if not digest.regions: digest.regions = []
                if isalphaorspace(part): digest.regions.append(part)
            elif last == C_ENTITIES:
                if not digest.entities: digest.entities = []
                digest.entities.append(part)
            elif last == C_CATEGORIES:
                if not digest.categories: digest.categories = []
                if isalphaorspace(part): digest.categories.append(part)
            elif last == C_SENTIMENTS:
                if not digest.sentiments: digest.sentiments = []
                if part.isalpha(): digest.sentiments.append(part)

        digest.regions = distinct_items(digest.regions)
        digest.entities = distinct_items(digest.entities)
        digest.categories = distinct_items(digest.categories)
        digest.sentiments = distinct_items(digest.sentiments)

        return digest

class TopicAttributes(BaseModel):
    frequency: Optional[int] = Field(default=None)
    keywords: Optional[list[str]] = Field(default=None)

class TopicsExtractionResponse(BaseModel):
    """Extracted topics of articles that can be written"""
    raw: str
    topics: dict[str, TopicAttributes] = Field(description="Dict<Topic,Dict<frequency:Int,keywords:List<String>>>")

    def parse_json(text: str):
        try: 
            topics = {k: TopicAttributes(**v) for k,v in json.loads(text).items()}
            if topics: return TopicsExtractionResponse(raw=text, topics = topics)
        except json.JSONDecodeError: return


M_VERDICT = "## Verdict"
class ArticleGenerationResponse(BaseModel):
    raw: str
    title: Optional[str] = None
    intro: Optional[str] = None
    analysis: Optional[str] = None
    takeaways: Optional[list[str]]
    verdict: Optional[str]
    markdown: Optional[str]

    def parse_markdown(text: str):
        text = text.strip().removeprefix(M_START).removesuffix(M_END).strip()
        response = ArticleGenerationResponse(raw=text)
        last = None
        for line in text.splitlines():
            line = line.strip()

            if any(field in line for field in M_FIELDS):
                last = line
            elif M_GIST in last:
                response.gist = line
            elif M_CATEGORIES in last:
                response.categories = split_parts(line)
            elif C_ENTITIES in last:
                response.entities = split_parts(line)
            elif M_TOPIC in last:
                response.topic = line 
            elif C_REGIONS in last:
                response.regions = split_parts(line)
            elif M_SUMMARY in last:
                response.summary = (response.summary+"\n"+line) if response.summary else line
            elif C_KEYPOINTS in last:
                if not response.keypoints: response.keypoints = []
                response.keypoints.append(line.removeprefix("- ").removeprefix("* "))
            elif M_INSIGHT in last:
                response.insight = line

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