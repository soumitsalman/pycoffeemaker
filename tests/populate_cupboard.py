import sys
import os
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json

from datetime import datetime
from pathlib import Path
from coffeemaker.orchestrators.cupboarddb import CupboardDB, Mug, Sip

def load_espresso_to_cupboard(json_file: str, db_path: str = ".beansack/cupboard/prod0/"):
    """
    Load espresso_content.json into CupboardDB as Mugs and Sips.
    
    Args:
        json_file: Path to espresso_content.json
        db_path: Path to the cupboard database directory
    """
    # Ensure db_path exists
    Path(db_path).mkdir(parents=True, exist_ok=True)
    
    # Load JSON data
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Initialize CupboardDB
    cupboard = CupboardDB(db_path=db_path)
    
    # Process Mugs
    mugs_to_insert = []
    for mug_data in data.get('mugs', []):
        mug = Mug(
            id=mug_data['id'],
            title=mug_data.get('title'),
            content=mug_data.get('content'),
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
        cupboard.add(mugs_to_insert)
        print(f"✓ Successfully inserted {len(mugs_to_insert)} Mugs")
    
    if sips_to_insert:
        print(f"Inserting {len(sips_to_insert)} Sips...")
        cupboard.add(sips_to_insert)
        print(f"✓ Successfully inserted {len(sips_to_insert)} Sips")
    
    print(f"\n✓ Complete! Database saved to {db_path}")
    return cupboard

if __name__ == '__main__':
    json_path = 'tests/espresso_content.json'
    db_path = '.test/cupboard/prod0/'
    
    if not os.path.exists(json_path):
        print(f"Error: {json_path} not found")
        exit(1)
    
    cupboard = load_espresso_to_cupboard(json_path, db_path)