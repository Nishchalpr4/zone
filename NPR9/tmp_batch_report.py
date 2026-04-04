import json
import random
import statistics
import time
from collections import Counter, defaultdict
from pathlib import Path

import requests

DATA_PATH = Path(r"c:/Users/nishc/OneDrive/Desktop/document_chunks_040426.json")
API = "http://127.0.0.1:8000"
OUT_PATH = Path(r"c:/Users/nishc/OneDrive/Desktop/allzones/NPR9/batch_report_document_chunks_040426.json")

MAX_LIVE_TESTS = 8
SEED = 42
PER_REQUEST_TIMEOUT = 180


def summarize_dataset(items):
    required = ["chunk_id", "doc_id", "chunk_index", "content", "embedding", "metadata"]
    missing_key_counts = Counter()
    empty_content = 0
    content_lengths = []
    embedding_lengths = []
    docs = Counter()

    for it in items:
        for k in required:
            if k not in it:
                missing_key_counts[k] += 1
        c = it.get("content", "") or ""
        if not c.strip():
            empty_content += 1
        content_lengths.append(len(c))

        emb = it.get("embedding")
        if isinstance(emb, list):
            embedding_lengths.append(len(emb))
        else:
            embedding_lengths.append(0)

        docs[str(it.get("doc_id", "unknown"))] += 1

    content_lengths_sorted = sorted(content_lengths)
    emb_lengths_sorted = sorted(embedding_lengths)

    def pct(arr, p):
        if not arr:
            return 0
        idx = int((len(arr) - 1) * p)
        return arr[idx]

    return {
        "total_chunks": len(items),
        "unique_docs": len(docs),
        "top_docs_by_chunk_count": docs.most_common(20),
        "missing_required_key_counts": dict(missing_key_counts),
        "empty_content_chunks": empty_content,
        "content_length_stats": {
            "min": min(content_lengths) if content_lengths else 0,
            "max": max(content_lengths) if content_lengths else 0,
            "mean": round(statistics.mean(content_lengths), 2) if content_lengths else 0,
            "p50": pct(content_lengths_sorted, 0.50),
            "p90": pct(content_lengths_sorted, 0.90),
            "p99": pct(content_lengths_sorted, 0.99),
        },
        "embedding_length_stats": {
            "min": min(embedding_lengths) if embedding_lengths else 0,
            "max": max(embedding_lengths) if embedding_lengths else 0,
            "mean": round(statistics.mean(embedding_lengths), 2) if embedding_lengths else 0,
            "distinct_lengths": sorted(set(embedding_lengths))[:15],
        },
    }


def choose_sample(items, max_tests=MAX_LIVE_TESTS, seed=SEED):
    # 1) Include first chunk from as many docs as possible for coverage
    by_doc_first = {}
    for it in items:
        d = str(it.get("doc_id", "unknown"))
        idx = int(it.get("chunk_index", 0) or 0)
        if d not in by_doc_first or idx < int(by_doc_first[d].get("chunk_index", 0) or 0):
            by_doc_first[d] = it

    base = list(by_doc_first.values())

    # 2) If too many docs, sample docs deterministically
    rnd = random.Random(seed)
    if len(base) > max_tests:
        base = rnd.sample(base, max_tests)

    # 3) If too few, top up with random chunks not already chosen
    chosen_ids = {x.get("chunk_id") for x in base}
    if len(base) < max_tests:
        rest = [x for x in items if x.get("chunk_id") not in chosen_ids and (x.get("content") or "").strip()]
        need = min(max_tests - len(base), len(rest))
        if need > 0:
            base.extend(rnd.sample(rest, need))

    # Keep stable order for readability
    return sorted(base, key=lambda x: (str(x.get("doc_id", "")), int(x.get("chunk_index", 0) or 0)))


def api_get(path, timeout=30):
    r = requests.get(API + path, timeout=timeout)
    return r.status_code, r.json()


def api_reset_graph():
    r = requests.delete(API + "/api/graph", timeout=60)
    return r.status_code, r.text[:500]


def live_test(sample):
    status, health = api_get("/api/health", timeout=20)
    if status != 200:
        raise RuntimeError(f"Health failed: {status} {health}")

    reset_status, reset_body = api_reset_graph()

    runs = []
    for i, it in enumerate(sample, start=1):
        text = (it.get("content") or "").strip()
        payload = {
            "text": text,
            "document_name": str(it.get("doc_id", "Unknown Document")),
            "section_ref": f"chunk_{it.get('chunk_index', i)}",
            "source_authority": 7,
            "metadata": {
                "document_id": str(it.get("doc_id", "unknown")),
                "chunk_id": str(it.get("chunk_id", f"sample_{i}")),
            },
        }

        start = time.time()
        row = {
            "i": i,
            "chunk_id": it.get("chunk_id"),
            "doc_id": it.get("doc_id"),
            "chunk_index": it.get("chunk_index"),
            "content_len": len(text),
        }
        try:
            r = requests.post(API + "/api/extract", json=payload, timeout=PER_REQUEST_TIMEOUT)
            row["http_status"] = r.status_code
            row["duration_sec"] = round(time.time() - start, 2)
            if r.status_code == 200:
                data = r.json()
                ext = data.get("extraction", {})
                row["entities_extracted"] = ext.get("entities_extracted")
                row["relations_extracted"] = ext.get("relations_extracted")
                row["new_entities"] = len(data.get("diff", {}).get("new_entities", []))
            else:
                row["error"] = r.text[:1000]
        except Exception as e:
            row["http_status"] = "exception"
            row["duration_sec"] = round(time.time() - start, 2)
            row["error"] = str(e)
        runs.append(row)

    zone_snapshots = {}
    for zone in ["all", "zone1_entity", "zone2_data"]:
        s, d = api_get(f"/api/graph?zone={zone}", timeout=45)
        snap = {
            "http_status": s,
            "node_count": len(d.get("nodes", [])) if isinstance(d, dict) else None,
            "link_count": len(d.get("links", [])) if isinstance(d, dict) else None,
            "stats": d.get("stats", {}) if isinstance(d, dict) else {},
        }
        if isinstance(d, dict) and isinstance(d.get("nodes"), list):
            with_metrics = []
            for n in d["nodes"]:
                q = n.get("quant_metrics") or []
                if q:
                    with_metrics.append({
                        "id": n.get("id"),
                        "label": n.get("label"),
                        "type": n.get("type"),
                        "quant_metrics": q,
                    })
            snap["nodes_with_quant_metrics"] = with_metrics
        zone_snapshots[zone] = snap

    ok_runs = [r for r in runs if r.get("http_status") == 200]
    err_runs = [r for r in runs if r.get("http_status") != 200]

    summary = {
        "sample_size": len(sample),
        "success_count": len(ok_runs),
        "failure_count": len(err_runs),
        "avg_duration_sec": round(statistics.mean([r["duration_sec"] for r in runs]), 2) if runs else 0,
        "avg_entities_extracted": round(statistics.mean([r.get("entities_extracted", 0) for r in ok_runs]), 2) if ok_runs else 0,
        "avg_relations_extracted": round(statistics.mean([r.get("relations_extracted", 0) for r in ok_runs]), 2) if ok_runs else 0,
        "reset_status": reset_status,
        "reset_body": reset_body,
    }

    return {
        "summary": summary,
        "per_chunk_results": runs,
        "zone_snapshots": zone_snapshots,
    }


def main():
    raw = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("Expected top-level list in dataset")

    dataset_summary = summarize_dataset(raw)
    sample = choose_sample(raw)
    sample_manifest = [
        {
            "chunk_id": s.get("chunk_id"),
            "doc_id": s.get("doc_id"),
            "chunk_index": s.get("chunk_index"),
            "content_len": len((s.get("content") or "")),
        }
        for s in sample
    ]

    live = live_test(sample)

    report = {
        "report_generated_at_epoch": time.time(),
        "input_file": str(DATA_PATH),
        "config": {
            "max_live_tests": MAX_LIVE_TESTS,
            "seed": SEED,
            "request_timeout_sec": PER_REQUEST_TIMEOUT,
        },
        "dataset_summary": dataset_summary,
        "live_test": live,
        "sample_manifest": sample_manifest,
    }

    OUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"WROTE_REPORT {OUT_PATH}")
    print("SUMMARY", json.dumps(live["summary"], indent=2))
    for z, snap in live["zone_snapshots"].items():
        print("ZONE", z, "nodes", snap.get("node_count"), "links", snap.get("link_count"))


if __name__ == "__main__":
    main()
