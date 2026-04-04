# Zone 2 Build Plan (Step-by-Step)

## Goal
Append Zone 2 (Data Zone) to the existing graph without breaking Zone 1 (Entity Zone).

## Guiding Design
Use one canonical graph, with strict layering for data semantics:

- Entity -> HAS_METRIC -> Metric
- Metric -> HAS_VALUE -> DataPoint
- DataPoint -> HAS_UNIT / HAS_CURRENCY / HAS_TIME_PERIOD / HAS_SCENARIO
- Metric -> HAS_DRIVER -> Driver
- Forecast -> FORECAST_FOR -> TimePeriod or Scenario
- Claim/Data -> EVIDENCE_FOR -> Evidence (existing assertions layer)

## Zone 2 Contract

### data_entity_types
- Metric
- KPI
- Financial
- Revenue
- Cost
- Margin
- GrowthRate
- Volume
- Price
- Capacity
- Utilization
- MarketShare
- Forecast
- TimePeriod
- Scenario
- Benchmark
- DataPoint
- Currency
- Unit
- Assumption
- Driver

### data_relation_types
- HAS_METRIC
- HAS_KPI
- HAS_VALUE
- HAS_UNIT
- HAS_CURRENCY
- HAS_TIME_PERIOD
- HAS_SCENARIO
- HAS_DRIVER
- DERIVED_FROM
- COMPARED_TO
- BENCHMARKED_AGAINST
- FORECAST_FOR
- ASSOCIATED_WITH
- REPORTED_IN
- PART_OF
- MEASURED_AS

### data_extraction_rules
- METRIC CREATION: Every quantitative or quasi-quantitative mention MUST be converted into a Metric or DataPoint node.
- VALUE SEPARATION: Numeric values MUST be stored as separate DataPoint nodes, not embedded in Metric names.
- TIME BINDING: Every DataPoint MUST be linked to a TimePeriod if mentioned or inferable.
- UNIT NORMALIZATION: Units (%, USD, units, tons) MUST be explicitly extracted and linked.
- DRIVER LINKING: If a cause is mentioned, extract it as Driver and link via HAS_DRIVER.
- FORECAST DISTINCTION: Forward-looking values MUST be tagged as Forecast and linked to Scenario if applicable.
- NO DUPLICATE METRICS: Similar metric labels must map to one canonical Metric node.
- ATTACH TO ENTITY ZONE: Every Metric MUST connect to at least one Zone 1 entity node.
- GRANULARITY RULE: Prefer atomic metrics (Revenue, Volume) over aggregated phrases.
- EVIDENCE: Every DataPoint MUST retain verbatim source_text.

## Delivery Phases

1. Contract and API surface
- Add explicit `zone_id` in extraction request.
- Default to `zone1_entity` so current behavior is unchanged.

2. Ontology extension
- Add `data_entity_types`, `data_relation_types`, `data_extraction_rules` to ontology storage.
- Keep Zone 1 and Zone 2 namespaces separate.

3. DB provenance
- Add `zone_master`, `entity_zone_membership`, `relation_zone_membership`.
- Add optional `zone_id` on assertions.

4. Zone-aware ingestion
- Keep canonical entity resolution global.
- Record per-zone memberships while ingesting.

5. Zone-aware reads
- Add graph filtering by zone (`zone1_entity`, `zone2_data`, `all`).

6. UI support
- Add zone selector for extraction.
- Add graph filter toggles.

7. Validation gates
- DataPoint always tied to Metric.
- Metric always tied to at least one entity node.
- Evidence present for DataPoint.
- Deduped canonical metric labels.

## Current sprint
- Step 1: Add zone_id request contract and keep backward compatibility.
