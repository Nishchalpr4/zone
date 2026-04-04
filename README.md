# Zone

Zone is a FastAPI and D3.js knowledge graph explorer built around a two-zone model:

- Zone 1: Entity Zone for company structure, products, geographies, roles, and business relationships
- Zone 2: Data Zone for metrics, values, periods, units, and other quantitative evidence

The current code lives under `NPR9/` and uses one canonical graph with zone-aware provenance instead of maintaining separate physical graphs.

## What The Data Zone Does

The Data Zone is the quantitative layer on top of the entity graph. It is designed to keep metrics grounded to real business entities while preserving source evidence.

Core ideas:

- Every extraction request can carry a `zone_id`
- Canonical entity IDs are shared across zones
- Zone membership is stored separately for entities and relations
- Quantitative facts are written under `zone2_data`
- Graph reads can be filtered by `zone1_entity`, `zone2_data`, or `all`
- The UI already exposes Entity Zone and Data Zone toggles

Current Data Zone contract in this repo includes:

- Metric and KPI-style quantitative capture
- Time period, currency, unit, driver, benchmark, and forecast-oriented ontology labels
- Zone-aware evidence storage through `assertions.zone_id`
- Zone-aware graph filtering using `entity_zone_membership` and `relation_zone_membership`

## Current Implementation Status

What is implemented now:

- `POST /api/extract` accepts `zone_id`
- extraction still runs through the existing multi-stage pipeline
- quant facts extracted in Stage 4 are stored as Data Zone facts
- graph reads support `zone1_entity`, `zone2_data`, and `all`
- frontend includes a Data Zone view toggle
- database schema includes zone master and membership tables

Important architecture note:

- This repo uses one canonical graph with zone-scoped provenance
- Zone 2 extends Zone 1; it does not replace it
- Metrics stay attached to entity nodes so the quantitative layer remains navigable

## Repository Layout

```text
allzones/
├─ README.md
├─ NPR9/
│  ├─ main.py
│  ├─ extraction.py
│  ├─ graph_store.py
│  ├─ database.py
│  ├─ validators.py
│  ├─ base_ontology.json
│  ├─ requirements.txt
│  └─ static/
└─ __MACOSX/   # local artifact, excluded from git
```

## API Overview

Main endpoints:

- `POST /api/extract` to run extraction and ingestion
- `GET /api/graph?zone=all|zone1_entity|zone2_data` to fetch graph state
- `GET /api/ontology` to inspect the active ontology
- `POST /api/admin/reseed` to refresh ontology rules
- `POST /api/reset` to clear the graph

Example extraction request:

```json
{
  "text": "Q4 revenue grew 12% year over year to $4.2B in North America.",
  "document_name": "Quarterly Report",
  "section_ref": "Q4 summary",
  "zone_id": "zone2_data",
  "metadata": {
    "document_id": "q4-report"
  }
}
```

## Local Development

From the project root:

```powershell
cd NPR9
python -m venv ..\.venv
..\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Required environment variables:

```env
DATABASE_URL=postgres://...
LLM_API_KEY=...
LLM_BASE_URL=https://openrouter.ai/api/v1
LLM_MODEL=openai/gpt-oss-120b:free
```

## Data Zone Design Notes

The repo includes a working build plan for Zone 2 in `NPR9/ZONE2_BUILD_PLAN.md`. The intended shape of the data model is:

- Entity -> HAS_METRIC -> Metric
- Metric -> HAS_VALUE -> DataPoint
- DataPoint -> HAS_UNIT / HAS_CURRENCY / HAS_TIME_PERIOD / HAS_SCENARIO
- Metric -> HAS_DRIVER -> Driver

The current codebase partially realizes that model by:

- storing zone-aware quantitative assertions
- attaching quant metrics to canonical entities
- exposing zone-scoped graph retrieval

## Tech Stack

- Python
- FastAPI
- Neon Postgres / PostgreSQL
- Pydantic
- D3.js
- LLM-backed multi-stage extraction

## Notes

- The workspace-level `.venv/` and `__MACOSX/` are local-only and are excluded from git
- The repo is intended to be pushed from this workspace root, not from the parent home-directory git root