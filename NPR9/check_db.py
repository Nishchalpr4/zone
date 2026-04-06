from database import DatabaseManager
import os
from dotenv import load_dotenv

load_dotenv()

db = DatabaseManager()
print("--- Database Stats ---")
with db.pool.getconn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM entity_master")
        print(f"Total Entities (entity_master): {cur.fetchone()[0]}")
        
        cur.execute("SELECT count(*) FROM relation_master")
        print(f"Total Relations (relation_master): {cur.fetchone()[0]}")
        
        cur.execute("SELECT count(*) FROM entity_zone_membership")
        print(f"Entities in entity_zone_membership: {cur.fetchone()[0]}")
        
        cur.execute("SELECT zone_id, count(*) FROM entity_zone_membership GROUP BY zone_id")
        for row in cur.fetchall():
            print(f"  Zone {row[0]}: {row[1]} entities")
            
        cur.execute("SELECT count(*) FROM relation_zone_membership")
        print(f"Relations in relation_zone_membership: {cur.fetchone()[0]}")

        cur.execute("SELECT zone_id, count(*) FROM relation_zone_membership GROUP BY zone_id")
        for row in cur.fetchall():
            print(f"  Zone {row[0]}: {row[1]} relations")
