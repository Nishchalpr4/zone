import os
import json
from dotenv import load_dotenv
from graph_store import GraphStore
from extraction import extract_knowledge_multistage
from models import ExtractionPayload

load_dotenv(override=True)

# Sample high-fidelity text
SAMPLE_TEXT = """
Apple Inc. (AAPL) reported strong financial results for Q1 2024. Total revenue reached $119.6 billion, representing a 2% increase year-over-year. The iPhone remains Apple's primary revenue driver, contributing $69.7 billion in the quarter. Services revenue, which includes iCloud, Apple Music, and the App Store, grew 11% to reach an all-time record of $23.1 billion. Wearables, Home and Accessories segment saw revenue of $11.9 billion. Operationally, Apple is expanding its manufacturing network in India and Vietnam to diversify its supply chain.
"""

def populate():
    print("Starting sample population...")
    store = GraphStore()
    
    # ── 1. EXTRACTION ──
    print(f"Extracting knowledge from sample text (via Multi-Stage Pipeline)...")
    payload = extract_knowledge_multistage(
        text=SAMPLE_TEXT,
        document_name="Apple Q1 2024 Earnings",
        document_id="apple_q1_2024"
    )
    
    # ── 2. INGESTION ──
    print(f"Ingesting extraction payload (Zone 1 + Zone 2)...")
    # Default to zone1_entity, but ingest_extraction will auto-handle zone2_data for quants.
    result = store.ingest_extraction(payload, metadata={"company_name": "Apple Inc.", "zone_id": "zone1_entity"})
    
    print(f"Population Complete: {result['entities_processed']} entities, {result['relations_processed']} relations.")
    print("Database is now ready for analysis.")

if __name__ == "__main__":
    populate()
