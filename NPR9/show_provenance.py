import os
from dotenv import load_dotenv
from database import DatabaseManager

load_dotenv(override=True)

def show():
    print("--- ZONE PROVENANCE (Option A Audit Table) ---")
    db = DatabaseManager()
    conn = db._get_connection()
    try:
        cursor = db._get_cursor(conn)
        cursor.execute("SELECT zone_id, source_text, subject_id, timestamp FROM zone_provenance ORDER BY timestamp DESC LIMIT 50")
        rows = cursor.fetchall()
        
        if not rows:
            print("No provenance records found.")
            return

        print(f"{'ZONE':<15} | {'SUBJECT_ID':<20} | {'TEXT SNIPPET'}")
        print("-" * 80)
        for row in rows:
            snippet = row['source_text'][:50].replace('\n', ' ') + "..." if len(row['source_text']) > 50 else row['source_text']
            print(f"{row['zone_id']:<15} | {row['subject_id']:<20} | {snippet}")
            
    finally:
        db._release_connection(conn)

if __name__ == "__main__":
    show()
