import json
import logging
from typing import List, Dict, Any, Optional
from models import ExtractionPayload, EntityCandidate, RelationCandidate, OntologyDiscovery

logger = logging.getLogger(__name__)

def safe_json_loads(data: Any, default: Any = None) -> Any:
    if data is None: return default
    if isinstance(data, (dict, list)): return data
    if not isinstance(data, str) or not data.strip(): return default
    try:
        cleaned = data.strip()
        if "```json" in cleaned:
            cleaned = cleaned.split("```json")[-1].split("```")[0].strip()
        return json.loads(cleaned)
    except:
        import re
        try:
            dict_match = re.search(r'\{.*\}', cleaned, re.DOTALL)
            list_match = re.search(r'\[.*\]', cleaned, re.DOTALL)
            if dict_match and (not list_match or dict_match.start() < list_match.start()): return json.loads(dict_match.group())
            elif list_match: return json.loads(list_match.group())
        except: pass
        return default

def find_list_data(data: Any) -> List[Any]:
    if isinstance(data, list): return data
    if isinstance(data, dict):
        for key in ["entities", "relations", "facts", "data"]:
            if key in data and isinstance(data[key], list): return data[key]
    return []

class LogicGuard:
    def __init__(self, ontology: Dict[str, Any]):
        self.ontology = ontology

    def refine_payload(self, payload: ExtractionPayload) -> ExtractionPayload:
        """
        PLATINUM STANDARD HEALER (v2):
        1. Identifies Global Root (LegalEntity).
        2. Normalizes Multi-Portfolio Hierarchy (Domain > Portfolios > Lines).
        3. Supports Products vs Services segregation.
        4. Protects Strategy & Financial nodes from pruning.
        5. Forces 100% Root-to-Leaf connectivity using BFS.
        """
        def norm(t): return str(t).lower().replace(" ", "").replace("_", "")
        
        # --- PHASE 0: Pre-processing ---
        entity_map = {str(e.temp_id): e for e in payload.entities}
        
        # 1. Identify Root
        root = next((e for e in payload.entities if norm(e.entity_type) in ["legalentity", "company"]), None)
        if not root and payload.entities: root = payload.entities[0]
        if not root: return payload
        root_id = str(root.temp_id)

        # --- PHASE 1: Taxonomy Anchoring (The Spine) ---
        # Derive types dynamically from ontology or use safe defaults for the core spine
        ont_types = {norm(t) for t in self.ontology.get("entity_types", [])}
        
        # Core structural types (The Spine) - These are mostly for internal healing
        pl_types = {"productline", "product", "item", "brand", "digitalproduct"}
        ps_types = {"service", "subscription"} 
        pf_types = {"productfamily", "productportfolio", "businessunit"}
        sf_types = {"serviceportfolio"}
        pd_types = {"productdomain", "industry", "subindustry"}
        
        # BASE ontology types — these are never logged as discoveries (standard types)
        _BASE_ONTOLOGY_NORM = {norm(t) for t in [
            "LegalEntity", "BusinessUnit", "Sector", "Industry", "SubIndustry", "EndMarket",
            "Channel", "ProductDomain", "ProductFamily", "ProductLine", "Site", "Geography",
            "Person", "Role", "Technology", "Capability", "Brand", "Initiative", "Program",
            "Management", "CompetitorNetwork", "ProductPortfolio", "Market", "Manufacturer",
            "Strategy", "SupplyChain", "ManufacturingNetwork", "MarketForecast",
            "ProductionInsight", "ServicePortfolio"
        ]}
        # Structural/bridge types also suppressed from discovery
        _SUPPRESS_DISCOVERY = _BASE_ONTOLOGY_NORM | pl_types | ps_types | pf_types | sf_types | pd_types
        
        # Everything else is considered 'non-taxonomic' (Strategy, Financial, Competitor, etc.)
        # We define them as 'known' only if they are in the database ontology.
        known_types = ont_types | pl_types | ps_types | pf_types | sf_types | pd_types

        # 1. Identify existing or required taxonomic anchors
        all_families = [e for e in payload.entities if norm(e.entity_type) in pf_types]
        all_domains = [e for e in payload.entities if norm(e.entity_type) in pd_types]
        service_fams = [e for e in payload.entities if norm(e.entity_type) in sf_types]
        
        # Ensure Domain exists
        if not all_domains:
            primary_dom_id = "bridge_taxonomic_domain"
            payload.entities.append(EntityCandidate(
                temp_id=primary_dom_id, canonical_name="Core Operations", 
                entity_type="ProductDomain", short_info="Primary industry sector."
            ))
            entity_map[primary_dom_id] = payload.entities[-1]
        else:
            primary_dom_id = str(all_domains[0].temp_id)

        # Ensure Product Family exists
        if not all_families:
            primary_fam_id = "bridge_taxonomic_family"
            payload.entities.append(EntityCandidate(
                temp_id=primary_fam_id, canonical_name="Product Portfolio", 
                entity_type="ProductPortfolio", short_info="Core portfolio of products."
            ))
            entity_map[primary_fam_id] = payload.entities[-1]
        else:
            primary_fam_id = str(all_families[0].temp_id)

        # Ensure Service Family exists if services are present
        has_services = any(norm(e.entity_type) in ps_types or any(x in str(e.canonical_name).lower() for x in ["icloud", "music", "cloud", "service"]) for e in payload.entities)
        if has_services and not service_fams:
            service_fam_id = "bridge_service_portfolio"
            payload.entities.append(EntityCandidate(
                temp_id=service_fam_id, canonical_name="Service Portfolio", 
                entity_type="ServicePortfolio", short_info="Portfolio of digital services."
            ))
            entity_map[service_fam_id] = payload.entities[-1]
        elif service_fams:
            service_fam_id = str(service_fams[0].temp_id)
        else:
            service_fam_id = None

        # --- PHASE 2: Relation Re-anchoring ---
        final_rels = []
        # We start with ALL relations that are NOT purely taxonomic (we'll rebuild the spine)
        tax_rel_types = {"HAS_PRODUCT_DOMAIN", "HAS_FAMILY", "HAS_PRODUCT_FAMILY", "HAS_SERVICE_PORTFOLIO", "INCLUDES", "HAS_PRODUCTS"}
        tax_rel_types_norm = {norm(t) for t in tax_rel_types}  # Pre-normalize for correct comparison
        
        for r in payload.relations:
            src_id, tgt_id = str(r.source_temp_id), str(r.target_temp_id)
            if src_id not in entity_map or tgt_id not in entity_map: continue
            
            tgt_ent = entity_map[tgt_id]
            tgt_type = norm(tgt_ent.entity_type)
            tgt_name = str(tgt_ent.canonical_name).lower()

            # 1. Standard Taxonomic Re-anchoring (Line/Service -> Family)
            if tgt_type in pl_types or tgt_type in ps_types:
                if service_fam_id and (tgt_type in ps_types or any(x in tgt_name for x in ["icloud", "music", "cloud", "service"])):
                    r.source_temp_id = service_fam_id
                else:
                    r.source_temp_id = primary_fam_id
                r.relation_type = "INCLUDES"
            
            # 2. Family -> Domain
            elif tgt_type in pf_types and src_id == root_id and tgt_id != primary_fam_id:
                r.source_temp_id = primary_dom_id
                r.relation_type = "HAS_FAMILY"
            
            # 3. Discover New Relations
            rel_type = r.relation_type.upper().replace(" ", "_")
            ont_rels = {norm(rt) for rt in self.ontology.get('relation_types', [])}
            if norm(rel_type) not in ont_rels and norm(rel_type) not in tax_rel_types_norm:
                logger.info(f"Discovered new relation type: {rel_type}")
                payload.discoveries.append(OntologyDiscovery(
                    type='RELATION',
                    name=rel_type,
                    suggested_label=rel_type,
                    context=f"Link between {entity_map[src_id].entity_type} and {entity_map[tgt_id].entity_type}",
                    source_type=entity_map[src_id].entity_type,
                    target_type=entity_map[tgt_id].entity_type
                ))

            # Filter out existing spine relations to avoid duplicates before we add our own
            if r.relation_type in tax_rel_types and tgt_id in [primary_dom_id, primary_fam_id, service_fam_id]:
                continue
                
            final_rels.append(r)

        # Add the Hard Spine
        # Root -> Domain
        final_rels.append(RelationCandidate(source_temp_id=root_id, target_temp_id=primary_dom_id, relation_type="HAS_PRODUCT_DOMAIN"))
        # Domain -> Family
        final_rels.append(RelationCandidate(source_temp_id=primary_dom_id, target_temp_id=primary_fam_id, relation_type="HAS_FAMILY"))
        # Root -> Service Portfolio
        if service_fam_id:
            final_rels.append(RelationCandidate(source_temp_id=root_id, target_temp_id=service_fam_id, relation_type="HAS_SERVICE_PORTFOLIO"))

        # --- PHASE 3: Connectivity & Pruning ---
        keep_ids = {root_id, primary_dom_id, primary_fam_id}
        if service_fam_id: keep_ids.add(service_fam_id)
        
        # Get all allowed types from ontology (normalized)
        ont_types = {norm(t) for t in self.ontology.get('entity_types', [])}
        
        allowed_entities = []
        for e in payload.entities:
            eid = str(e.temp_id)
            etype_raw = e.entity_type
            etype = norm(etype_raw)
            
            # 1. Is it a core taxonomic anchor?
            is_anchor = eid in keep_ids
            
            # 2. Is it a known type in our current ontology?
            is_known = etype in known_types
            
            # CRITICAL DISCOVERY LOGIC:
            # Log discovery if NOT in the BASE/STRUCTURAL suppression set.
            # This catches both truly novel types AND types previously discovered
            # (which are now in extended ontology but not base ontology).
            is_base_type = etype in _SUPPRESS_DISCOVERY
            if not is_base_type:
                # 1. Log the discovery for the learning engine
                logger.info(f"Discovered new entity type: {etype_raw} for {e.canonical_name}")
                payload.discoveries.append(OntologyDiscovery(
                    type='ENTITY',
                    name=e.canonical_name, 
                    suggested_label=etype_raw,
                    context=f"Extracted from text: {e.short_info or e.description}"
                ))
            if not is_known:
                # 2. Keep the original suggested type and allow ingestion into KG
                e.entity_type = etype_raw
                allowed_entities.append(e)
            elif is_anchor or is_known:
                allowed_entities.append(e)
        
        payload.entities = allowed_entities
        allowed_ids = {str(e.temp_id) for e in payload.entities}

        # BFS for strict connectivity
        tree_rels = []
        visited = {root_id}
        queue = [root_id]
        
        # Filter rels to only include allowed IDs
        final_rels = [r for r in final_rels if str(r.source_temp_id) in allowed_ids and str(r.target_temp_id) in allowed_ids]
        
        while queue:
            curr = queue.pop(0)
            for r in final_rels:
                if str(r.source_temp_id) == curr:
                    tid = str(r.target_temp_id)
                    if tid not in visited:
                        tree_rels.append(r)
                        visited.add(tid)
                        queue.append(tid)
        
        # Orphan handling
        for e in payload.entities:
            eid = str(e.temp_id)
            if eid not in visited and eid != root_id:
                # strategy/financial nodes might be floating, anchor them to root or nearest
                tree_rels.append(RelationCandidate(
                    source_temp_id=root_id, 
                    target_temp_id=eid, 
                    relation_type="HAS_STRATEGY" if norm(e.entity_type) in ["strategy", "capability"] else "ASSOCIATED_WITH"
                ))
                visited.add(eid)

        payload.relations = tree_rels
        return payload

    def validate_extraction(self, payload: ExtractionPayload) -> List[str]:
        return []
