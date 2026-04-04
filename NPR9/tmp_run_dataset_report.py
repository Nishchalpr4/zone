import json
import time
import traceback
from pathlib import Path
from datetime import datetime

import requests

DATA_PATH = Path(r"c:\Users\nishc\OneDrive\Desktop\document_chunks_040426.json")
OUT_PATH = Path(r"c:\Users\nishc\OneDrive\Desktop\allzones\NPR9\dataset_test_report_040426.json")
BASE_URL = "http://127.0.0.1:8000"

# Keep live API cost/time bounded while still meaningful
SAMPLE_SIZE = 8
REQUEST_TIMEOUT = 320


def safe_get(d, key, default=None):
    return d.get(key, default) if isinstance(d, dict) else default


def parse_dataset(path: Path):
    text = path.read_text(encoding="utf-8", errors="replace")
    payload = json.loads(text)

    if isinstance(payload, dict):
        if isinstance(payload.get("chunks"), list):
            chunks = payload["chunks"]
        elif isinstance(payload.get("data"), list):
            chunks = payload["data"]
        else:
            # best effort for dict payloads
            chunks = [payload]
    elif isinstance(payload, list):
        chunks = payload
    else:
        chunks = []

    return payload, chunks


def health_check():
    r = requests.get(f"{BASE_URL}/api/health", timeout=10)
    r.raise_for_status()
    return r.json()


def reset_graph():
    r = requests.delete(f"{BASE_URL}/api/graph", timeout=30)
    r.raise_for_status()
    return r.json()


def get_zone_graph(zone: str):
    r = requests.get(f"{BASE_URL}/api/graph", params={"zone": zone}, timeout=40)
    r.raise_for_status()
    return r.json()


def chunk_text(chunk):
    # Handle common schema variants
    return (
        safe_get(chunk, "text")
        or safe_get(chunk, "chunk_text")
        or safe_get(chunk, "content")
        or safe_get(chunk, "body")
        or ""
    )


def chunk_doc_name(chunk, idx):
    return (
        safe_get(chunk, "document_name")
        or safe_get(chunk, "doc_name")
        or safe_get(chunk, "source")
        or f"chunk_{idx + 1}"
    )


def chunk_section_ref(chunk):
    return (
        safe_get(chunk, "section_ref")
        or safe_get(chunk, "section")
        or safe_get(chunk, "chunk_id")
        or "chunk"
    )


def integrity_stats(chunks):
    total = len(chunks)
    empty_text = 0
    short_text_lt_40 = 0
    missing_doc_name = 0
    unique_texts = set()
    duplicate_text_count = 0

    for i, c in enumerate(chunks):
        text = chunk_text(c)
        if not text or not str(text).strip():
            empty_text += 1
        if len(str(text).strip()) < 40:
            short_text_lt_40 += 1
        doc_name = chunk_doc_name(c, i)
        if not doc_name or not str(doc_name).strip():
            missing_doc_name += 1

        t = str(text).strip()
        if t in unique_texts:
            duplicate_text_count += 1
        else:
            unique_texts.add(t)

    return {
        "total_chunks": total,
        "empty_text_chunks": empty_text,
        "short_text_lt_40": short_text_lt_40,
        "missing_document_name": missing_doc_name,
        "duplicate_text_chunks": duplicate_text_count,
        "unique_text_chunks": len(unique_texts),
    }


def sample_indices(total, sample_size):
    if total == 0:
        return []
    if total <= sample_size:
        return list(range(total))

    # evenly spread sample across file
    step = max(1, total // sample_size)
    picks = list(range(0, total, step))[:sample_size]
    if len(picks) < sample_size:
        i = total - 1
        while len(picks) < sample_size and i >= 0:
            if i not in picks:
                picks.append(i)
            i -= 1
    return sorted(picks)


def run_extract(chunk, idx):
    text = str(chunk_text(chunk)).strip()
    payload = {
        "text": text,
        "document_name": chunk_doc_name(chunk, idx),
        "section_ref": str(chunk_section_ref(chunk)),
        "source_authority": 6,
    }

    started = time.time()
    r = requests.post(f"{BASE_URL}/api/extract", json=payload, timeout=REQUEST_TIMEOUT)
    elapsed = round(time.time() - started, 2)

    row = {
        "chunk_index": idx,
        "document_name": payload["document_name"],
        "section_ref": payload["section_ref"],
        "text_length": len(text),
        "elapsed_sec": elapsed,
        "http_status": r.status_code,
    }

    try:
        data = r.json()
    except Exception:
        row["error"] = f"Non-JSON response: {r.text[:300]}"
        return row

    if r.status_code != 200:
        row["error"] = data.get("detail", str(data))
        return row

    ext = data.get("extraction", {})
    diff = data.get("diff", {})
    row.update(
        {
            "success": True,
            "entities_extracted": ext.get("entities_extracted", 0),
            "relations_extracted": ext.get("relations_extracted", 0),
            "total_entities_after": diff.get("total_entities"),
            "total_relations_after": diff.get("total_relations"),
            "thought_process_preview": str(ext.get("thought_process", ""))[:240],
        }
    )
    return row


def main():
    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "dataset_path": str(DATA_PATH),
        "api_base": BASE_URL,
        "sample_size": SAMPLE_SIZE,
        "status": "started",
    }

    try:
        payload, chunks = parse_dataset(DATA_PATH)
        report["dataset_root_type"] = type(payload).__name__
        report["integrity"] = integrity_stats(chunks)

        # API preflight
        report["health_before"] = health_check()
        report["reset"] = reset_graph()

        picks = sample_indices(len(chunks), SAMPLE_SIZE)
        report["sample_indices"] = picks

        results = []
        ok = 0
        fail = 0
        total_entities = 0
        total_relations = 0

        for idx in picks:
            row = run_extract(chunks[idx], idx)
            results.append(row)
            if row.get("success"):
                ok += 1
                total_entities += int(row.get("entities_extracted", 0) or 0)
                total_relations += int(row.get("relations_extracted", 0) or 0)
            else:
                fail += 1

        report["sample_results"] = results
        report["sample_summary"] = {
            "successful": ok,
            "failed": fail,
            "success_rate": round((ok / len(picks)) * 100, 2) if picks else 0.0,
            "entities_extracted_total": total_entities,
            "relations_extracted_total": total_relations,
            "entities_per_success_avg": round(total_entities / ok, 2) if ok else 0.0,
            "relations_per_success_avg": round(total_relations / ok, 2) if ok else 0.0,
        }

        # Final zone snapshots
        zone_all = get_zone_graph("all")
        zone_e = get_zone_graph("zone1_entity")
        zone_d = get_zone_graph("zone2_data")

        def summarize_zone(data):
            nodes = data.get("nodes", [])
            links = data.get("links", [])
            node_types = {}
            quant_nodes = 0
            quant_metrics = 0
            for n in nodes:
                t = n.get("type", "Unknown")
                node_types[t] = node_types.get(t, 0) + 1
                q = n.get("quant_metrics", []) or []
                if q:
                    quant_nodes += 1
                    quant_metrics += len(q)
            return {
                "nodes": len(nodes),
                "links": len(links),
                "quant_nodes": quant_nodes,
                "quant_metrics": quant_metrics,
                "node_type_breakdown": dict(sorted(node_types.items(), key=lambda kv: kv[0])),
            }

        report["zone_snapshots"] = {
            "all": summarize_zone(zone_all),
            "zone1_entity": summarize_zone(zone_e),
            "zone2_data": summarize_zone(zone_d),
        }

        # Known behavior checks
        z1n = report["zone_snapshots"]["zone1_entity"]["nodes"]
        z2n = report["zone_snapshots"]["zone2_data"]["nodes"]
        z2q = report["zone_snapshots"]["zone2_data"]["quant_metrics"]
        report["checks"] = {
            "toggle_has_distinct_results": z1n != z2n,
            "data_zone_has_quant_metrics": z2q > 0,
            "data_zone_non_empty": z2n > 0,
        }

        report["status"] = "completed"

    except Exception as e:
        report["status"] = "failed"
        report["error"] = str(e)
        report["traceback"] = traceback.format_exc()

    OUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(str(OUT_PATH))
    print("STATUS:", report.get("status"))


if __name__ == "__main__":
    main()
