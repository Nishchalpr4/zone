from database import DatabaseManager
import os
from dotenv import load_dotenv

load_dotenv()

db = DatabaseManager()
print("Performing nuclear wipe...")
db.danger_full_wipe()
print("Re-initializing DB...")
db._init_db()
print("Seeding fresh ontology (no merge)...")
db.seed_ontology(merge_with_existing=False)
print("Reset complete.")
