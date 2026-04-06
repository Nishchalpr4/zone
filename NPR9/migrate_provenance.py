import os
from dotenv import load_dotenv
from database import DatabaseManager

load_dotenv(override=True)

def migrate():
    print("Starting migration of existing assertions to zone_provenance...")
    db = DatabaseManager()
    conn = db._get_connection()
    try:
        cursor = db._get_cursor(conn)
        
        # 1. Fetch all existing assertions that have a zone_id
        cursor.execute("""
            SELECT zone_id, source_text, subject_id, document_name, section_ref 
            FROM assertions 
            WHERE zone_id IS NOT NULL
        """)
        assertions = cursor.fetchall()
        print(f"Found {len(assertions)} assertions to migrate.")
        
        # 2. Insert into zone_provenance
        count = 0
        for ass in assertions:
            cursor.execute("""
                INSERT INTO zone_provenance (zone_id, source_text, subject_id, document_name, section_ref)
                VALUES (%s, %s, %s, %s, %s)
            """, (ass['zone_id'], ass['source_text'], ass['subject_id'], ass['document_name'], ass['section_ref']))
            count += 1
            
        conn.commit()
        print(f"Successfully migrated {count} records to zone_provenance.")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        conn.rollback()
    finally:
        db._release_connection(conn)

if __name__ == "__main__":
    migrate()
