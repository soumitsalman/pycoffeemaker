import sys
import os
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json

from datetime import datetime
from pathlib import Path
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.lancesack import Beansack
from coffeemaker.nlp import embedders

import os
import json
import re
from datetime import datetime
from typing import Optional
from bs4 import BeautifulSoup
import feedparser
from pathlib import Path

def extract_tldr_highlight(content: str) -> Optional[str]:
    """Extract the TLDR section from content."""
    soup = BeautifulSoup(content, 'html.parser')
    tldr_section = soup.find('h4', {'id': 'tldr'})
    
    if tldr_section and tldr_section.find_next('ul'):
        ul = tldr_section.find_next('ul')
        items = [li.get_text(strip=True) for li in ul.find_all('li')]
        return items
    return None

def extract_sips_from_content(content: str, link: str, pub_date: str) -> list:
    """Extract Sips (h3 sections) from content."""
    soup = BeautifulSoup(content, 'html.parser')
    sips = []
    
    h2_tags = soup.find_all('h2')
    
    for i, h2 in enumerate(h2_tags):
        h2_text = h2.get_text(strip=True)
        sip_id = f"{link}#{h2_text.lower().replace(' ', '-')}"
        
        # Collect content until next h2 or end
        content_text = []
        current = h2.find_next_sibling()
        
        while current:
            if current.name == 'h2':
                break
            if current.name in ['p', 'ul', 'ol', 'pre']:
                content_text.append(current.get_text(strip=True))
            current = current.find_next_sibling()
        
        sip_content = ' '.join(content_text)
        
        sips.append({
            'id': sip_id,
            'title': h2_text,
            'content': sip_content,
            'created': pub_date
        })
    
    return sips

def parse_rss_to_mugs_and_sips(feed_url: str) -> tuple:
    """Parse RSS feed and return Mugs and Sips."""
    feed = feedparser.parse(feed_url)
    mugs = []
    sips = []
    
    for entry in feed.entries:
        # Extract Mug data
        title = entry.get('title', '')
        link = entry.get('link', '')
        content = entry.get('content', [{}])[0].get('value', entry.get('summary', ''))
        pub_date = entry.get('published', datetime.now().isoformat())
        categories = entry.get('tags', [])
        
        # Parse pubDate
        try:
            from email.utils import parsedate_to_datetime
            if 'published_parsed' in entry:
                pub_datetime = datetime(*entry.published_parsed[:6])
                pub_date = pub_datetime.isoformat()
        except:
            pub_date = datetime.now().isoformat()
        
        # Extract highlight (TLDR section)
        highlight = extract_tldr_highlight(content)
        
        # Create Mug
        mug = {
            'id': link,
            'title': title,
            'content': content,
            'tags': [cat['term'] for cat in categories],
            'highlights': highlight,
            'sips': [],
            'created': pub_date
        }
        
        # Extract Sips
        entry_sips = extract_sips_from_content(content, link, pub_date)
        sips.extend(entry_sips)
        mug['sips'] = [sip['id'] for sip in entry_sips]
        
        mugs.append(mug)
    
    return mugs, sips

def import_espresso_rss():
    """Main function to fetch RSS and save JSON."""
    rss_url = 'https://espresso.cafecito.tech/rss/'
    
    # Create .tmp directory if it doesn't exist
    tmp_dir = Path('.tmp')
    tmp_dir.mkdir(exist_ok=True)
    
    # Parse RSS feed
    print(f"Fetching RSS from {rss_url}...")
    mugs, sips = parse_rss_to_mugs_and_sips(rss_url)
    
    # Prepare output structure
    output = {
        'mugs': mugs,
        'sips': sips,
        'metadata': {
            'source': rss_url,
            'fetched_at': datetime.now().isoformat(),
            'mug_count': len(mugs),
            'sip_count': len(sips)
        }
    }
    
    # Save to JSON file
    output_file = tmp_dir / 'espresso_content.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Successfully saved {len(mugs)} Mugs and {len(sips)} Sips to {output_file}")
    print(f"  File size: {output_file.stat().st_size / 1024:.2f} KB")
    return output

def load_espresso_to_cupboard(cupboard_data, db_path: str = ".beansack/cupboard/prod0/"):
    # Load JSON data
    if isinstance(cupboard_data, str):
        with open(cupboard_data, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = cupboard_data

    # Initialize CupboardDB
    db = Beansack(db_path)
    embedder = embedders.from_path(os.getenv("EMBEDDER_PATH"))
    
    def _embed(items: list):
        if not all([item.embedding for item in items]): return items

        get_content = lambda item: f"{item.title}\n\n{item.content}"
        contents = [get_content(item) for item in items]
        vecs = embedder.embed_documents(contents)
        for item, vec in zip(items, vecs):
            item.embedding = vec
        return items
    
    # Process Mugs
    mugs_to_insert = []
    for mug_data in data.get('mugs', []):
        mug = Mug(
            id=mug_data['id'],
            title=mug_data.get('title'),
            content=mug_data.get('content'),
            embedding=None,
            created=datetime.fromisoformat(mug_data['created']) if mug_data.get('created') else None,
            sips=mug_data.get('sips', []),
            highlights=mug_data.get('highlights', []),
            tags=mug_data.get('tags', [])
        )
        mugs_to_insert.append(mug)
    
    # Process Sips
    sips_to_insert = []
    for sip_data in data.get('sips', []):
        sip = Sip(
            id=sip_data['id'],
            title=sip_data.get('title'),
            content=sip_data.get('content'),
            created=datetime.fromisoformat(sip_data['created']) if sip_data.get('created') else None,
            past_sips=sip_data.get('past_sips', []),
            source_beans=sip_data.get('source_beans', [])
        )
        sips_to_insert.append(sip)
    
    # Insert into database
    if mugs_to_insert:
        print(f"Inserting {len(mugs_to_insert)} Mugs...")
        total = db.store_mugs(_embed(mugs_to_insert))
        print(f"✓ Successfully inserted {total} Mugs")

    if sips_to_insert:
        print(f"Inserting {len(sips_to_insert)} Sips...")
        total = db.store_sips(_embed(sips_to_insert))
        print(f"✓ Successfully inserted {total} Sips")
    
    print(f"\n✓ Complete! Database saved to {db_path}")
    return db

if __name__ == '__main__':
    json_path = 'tests/cupboard_data.json'
    db_path = '.beansack/lancesack'
        
    db = load_espresso_to_cupboard(import_espresso_rss(), db_path)