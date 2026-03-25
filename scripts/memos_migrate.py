#!/usr/bin/env python3
"""
Migrate MemOS data (Neo4j Memory nodes + Qdrant vectors) between servers.

Usage:
  # Full migration: local → NAS
  python3 memos_migrate.py --src http://127.0.0.1 --dst http://10.10.10.66

  # Dry run (count only, no write)
  python3 memos_migrate.py --src http://127.0.0.1 --dst http://10.10.10.66 --dry-run

  # Custom ports
  python3 memos_migrate.py --src http://127.0.0.1 --dst http://10.10.10.66 \
    --src-neo4j-port 7474 --src-neo4j-auth neo4j:12345678 \
    --dst-neo4j-port 7474 --dst-neo4j-auth neo4j:openclaw2026 \
    --src-qdrant-port 6333 --dst-qdrant-port 6333

Migrates:
  Phase 1: Qdrant vectors (dedup by point ID)
  Phase 2: Neo4j Memory nodes via HTTP API (dedup by node ID)
  Phase 3: Fix stringified list fields in imported nodes

Requirements:
  - Both Neo4j instances must have HTTP API enabled (port 7474)
  - Both Qdrant instances must be accessible (port 6333)
  - No special libraries needed (stdlib only)
"""
import argparse
import base64
import json
import sys
import time
import urllib.request
import urllib.error

QDRANT_COLLECTION = "neo4j_vec_db"
BATCH_SIZE = 100
IMPORT_BATCH = 20
LIST_FIELDS = ["evolve_to", "history", "tags", "usage", "file_ids", "sources"]


def neo4j_query(base_url, auth, statement, params=None):
    """Execute a Cypher query via Neo4j HTTP API."""
    url = f"{base_url}/db/neo4j/tx/commit"
    body = {"statements": [{"statement": statement}]}
    if params:
        body["statements"][0]["parameters"] = params
    data = json.dumps(body).encode()
    auth_header = base64.b64encode(auth.encode()).decode()
    req = urllib.request.Request(url, data=data, headers={
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth_header}"
    })
    with urllib.request.urlopen(req, timeout=120) as r:
        result = json.loads(r.read())
        if result.get("errors"):
            raise Exception(f"Neo4j error: {result['errors']}")
        return result


def qdrant_scroll(base_url, collection, limit=100, offset=None):
    """Scroll through Qdrant points."""
    url = f"{base_url}/collections/{collection}/points/scroll"
    body = {"limit": limit, "with_payload": True, "with_vectors": True}
    if offset is not None:
        body["offset"] = offset
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def qdrant_get_ids(base_url, collection, ids):
    """Check which point IDs exist in Qdrant."""
    url = f"{base_url}/collections/{collection}/points"
    body = {"ids": ids, "with_payload": False, "with_vectors": False}
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            result = json.loads(r.read())
            return {str(p["id"]) for p in result.get("result", [])}
    except Exception:
        return set()


def qdrant_upsert(base_url, collection, points):
    """Upsert points into Qdrant."""
    url = f"{base_url}/collections/{collection}/points?wait=true"
    body = {"points": points}
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="PUT",
                                headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())


def migrate_qdrant(src_qdrant, dst_qdrant, dry_run=False):
    """Phase 1: Migrate Qdrant vectors with dedup."""
    print("=" * 60)
    print("Phase 1: Qdrant vector migration")
    print(f"  {src_qdrant} → {dst_qdrant}")
    print("=" * 60)

    # Scan all source points
    print("\nScanning source Qdrant points...")
    all_points = []
    offset = None
    while True:
        resp = qdrant_scroll(src_qdrant, QDRANT_COLLECTION, limit=BATCH_SIZE, offset=offset)
        points = resp.get("result", {}).get("points", [])
        if not points:
            break
        all_points.extend(points)
        next_offset = resp.get("result", {}).get("next_page_offset")
        if next_offset is None:
            break
        offset = next_offset
        print(f"  Scanned {len(all_points)} points...", flush=True)
    print(f"Total source points: {len(all_points)}")

    # Check which exist on destination
    print("\nChecking existing IDs on destination...")
    src_ids = [str(p["id"]) for p in all_points]
    existing = set()
    for i in range(0, len(src_ids), BATCH_SIZE):
        batch_ids = src_ids[i:i + BATCH_SIZE]
        existing.update(qdrant_get_ids(dst_qdrant, QDRANT_COLLECTION, batch_ids))

    new_points = [p for p in all_points if str(p["id"]) not in existing]
    print(f"Already on destination: {len(existing)}")
    print(f"New points to upload: {len(new_points)}")

    if dry_run:
        print("(dry run — skipping upload)")
        return len(new_points)

    if not new_points:
        return 0

    # Upload new points
    print("\nUploading new points...")
    uploaded = 0
    for i in range(0, len(new_points), BATCH_SIZE):
        batch = new_points[i:i + BATCH_SIZE]
        clean_batch = [{"id": p["id"], "vector": p["vector"], "payload": p.get("payload", {})}
                       for p in batch]
        try:
            qdrant_upsert(dst_qdrant, QDRANT_COLLECTION, clean_batch)
            uploaded += len(batch)
            if uploaded % 500 == 0 or uploaded == len(new_points):
                print(f"  Uploaded {uploaded}/{len(new_points)}", flush=True)
        except Exception as e:
            print(f"  Error at batch {i}: {e}", flush=True)

    print(f"\nQdrant: {uploaded} new vectors uploaded")
    return uploaded


def migrate_neo4j(src_neo4j, src_auth, dst_neo4j, dst_auth, dry_run=False):
    """Phase 2: Migrate Neo4j Memory nodes with dedup."""
    print("\n" + "=" * 60)
    print("Phase 2: Neo4j Memory node migration")
    print(f"  {src_neo4j} → {dst_neo4j}")
    print("=" * 60)

    # Export all source nodes
    print("\nExporting source Memory nodes...")
    offset = 0
    all_nodes = []
    while True:
        result = neo4j_query(src_neo4j, src_auth,
                             f"MATCH (n:Memory) RETURN properties(n) AS props SKIP {offset} LIMIT 200")
        rows = result["results"][0]["data"]
        if not rows:
            break
        for row in rows:
            all_nodes.append(row["row"][0])
        offset += 200
        print(f"  Exported {len(all_nodes)} nodes...", flush=True)
    print(f"Total source nodes: {len(all_nodes)}")

    # Get destination existing IDs
    print("\nGetting destination existing Memory IDs...")
    result = neo4j_query(dst_neo4j, dst_auth, "MATCH (n:Memory) RETURN n.id AS id")
    dst_ids = {row["row"][0] for row in result["results"][0]["data"]}
    print(f"Destination has {len(dst_ids)} existing nodes")

    # Filter new nodes
    new_nodes = [n for n in all_nodes if n.get("id") not in dst_ids]
    print(f"New nodes to create: {len(new_nodes)}")
    print(f"Already on destination: {len(all_nodes) - len(new_nodes)}")

    if dry_run:
        print("(dry run — skipping create)")
        return len(new_nodes)

    if not new_nodes:
        return 0

    # Import in batches using parameterized UNWIND
    created = 0
    errors = 0
    for i in range(0, len(new_nodes), IMPORT_BATCH):
        batch = new_nodes[i:i + IMPORT_BATCH]
        # Clean properties: convert lists/dicts to JSON strings for transport
        clean_batch = []
        for node in batch:
            clean = {}
            for k, v in node.items():
                if v is None:
                    continue
                if isinstance(v, (list, dict)):
                    clean[k] = json.dumps(v, ensure_ascii=False)
                else:
                    clean[k] = str(v)
            clean_batch.append(clean)
        try:
            result = neo4j_query(dst_neo4j, dst_auth,
                                 "UNWIND $nodes AS props CREATE (n:Memory) SET n = props RETURN count(n) AS cnt",
                                 {"nodes": clean_batch})
            cnt = result["results"][0]["data"][0]["row"][0]
            created += cnt
            if (i // IMPORT_BATCH) % 10 == 0 or i + IMPORT_BATCH >= len(new_nodes):
                print(f"  Created {created}/{len(new_nodes)} nodes", flush=True)
        except Exception as e:
            print(f"  Error at batch {i}: {e}", flush=True)
            errors += 1
        time.sleep(0.2)

    print(f"\nNeo4j: {created} nodes created, {errors} errors")
    return created


def fix_stringified_lists(dst_neo4j, dst_auth, dry_run=False):
    """Phase 3: Fix list fields that were stored as JSON strings during import."""
    print("\n" + "=" * 60)
    print("Phase 3: Fix stringified list fields")
    print("=" * 60)

    total_fixed = 0
    for field in LIST_FIELDS:
        result = neo4j_query(dst_neo4j, dst_auth, f"""
        MATCH (n:Memory) 
        WHERE n.{field} IS NOT NULL AND n.{field} STARTS WITH '['
        RETURN n.id AS id, n.{field} AS val
        LIMIT 5000
        """)
        rows = result["results"][0]["data"]

        if not rows:
            continue

        print(f"\n  {field}: {len(rows)} nodes with stringified lists")

        if dry_run:
            total_fixed += len(rows)
            continue

        # Parse JSON strings back to lists and update
        BATCH = 50
        fixed = 0
        for i in range(0, len(rows), BATCH):
            batch = rows[i:i + BATCH]
            updates = []
            for row in batch:
                node_id = row["row"][0]
                val_str = row["row"][1]
                try:
                    parsed = json.loads(val_str)
                    if isinstance(parsed, list):
                        updates.append({"id": node_id, "val": parsed})
                except Exception:
                    updates.append({"id": node_id, "val": []})
            if updates:
                neo4j_query(dst_neo4j, dst_auth, f"""
                UNWIND $updates AS u
                MATCH (n:Memory {{id: u.id}})
                SET n.{field} = u.val
                """, {"updates": updates})
                fixed += len(updates)
        total_fixed += fixed
        print(f"    Fixed {fixed} nodes")

    # Fix datetime fields stored as strings
    for field in ["created_at", "updated_at"]:
        result = neo4j_query(dst_neo4j, dst_auth, f"""
        MATCH (n:Memory) 
        WHERE n.{field} IS NOT NULL AND n.{field} STARTS WITH '2026'
        RETURN count(n) AS cnt
        """)
        cnt = result["results"][0]["data"][0]["row"][0]
        if cnt > 0:
            print(f"\n  {field}: {cnt} nodes with string datetime")
            if not dry_run:
                neo4j_query(dst_neo4j, dst_auth, f"""
                MATCH (n:Memory) 
                WHERE n.{field} IS NOT NULL AND n.{field} STARTS WITH '20'
                SET n.{field} = datetime(n.{field})
                """)
                print(f"    Fixed {cnt} nodes")
            total_fixed += cnt

    print(f"\nTotal fixes applied: {total_fixed}")
    return total_fixed


def main():
    parser = argparse.ArgumentParser(description="Migrate MemOS data between servers")
    parser.add_argument("--src", required=True, help="Source host (e.g. http://127.0.0.1)")
    parser.add_argument("--dst", required=True, help="Destination host (e.g. http://10.10.10.66)")
    parser.add_argument("--src-neo4j-port", type=int, default=7474, help="Source Neo4j HTTP port")
    parser.add_argument("--dst-neo4j-port", type=int, default=7474, help="Dest Neo4j HTTP port")
    parser.add_argument("--src-neo4j-auth", default="neo4j:12345678", help="Source Neo4j auth (user:pass)")
    parser.add_argument("--dst-neo4j-auth", default="neo4j:openclaw2026", help="Dest Neo4j auth (user:pass)")
    parser.add_argument("--src-qdrant-port", type=int, default=6333, help="Source Qdrant port")
    parser.add_argument("--dst-qdrant-port", type=int, default=6333, help="Dest Qdrant port")
    parser.add_argument("--dry-run", action="store_true", help="Count only, no writes")
    parser.add_argument("--skip-qdrant", action="store_true", help="Skip Qdrant migration")
    parser.add_argument("--skip-neo4j", action="store_true", help="Skip Neo4j migration")
    parser.add_argument("--skip-fix", action="store_true", help="Skip list field fix")

    args = parser.parse_args()

    src_neo4j = f"{args.src}:{args.src_neo4j_port}"
    dst_neo4j = f"{args.dst}:{args.dst_neo4j_port}"
    src_qdrant = f"{args.src}:{args.src_qdrant_port}"
    dst_qdrant = f"{args.dst}:{args.dst_qdrant_port}"

    print("MemOS Data Migration")
    print(f"  Source:      {args.src}")
    print(f"  Destination: {args.dst}")
    if args.dry_run:
        print("  Mode: DRY RUN (no writes)")
    print()

    results = {}

    if not args.skip_qdrant:
        results["qdrant"] = migrate_qdrant(src_qdrant, dst_qdrant, args.dry_run)

    if not args.skip_neo4j:
        results["neo4j"] = migrate_neo4j(src_neo4j, args.src_neo4j_auth,
                                          dst_neo4j, args.dst_neo4j_auth, args.dry_run)

    if not args.skip_fix and not args.dry_run:
        results["fixes"] = fix_stringified_lists(dst_neo4j, args.dst_neo4j_auth, args.dry_run)

    # Final summary
    print("\n" + "=" * 60)
    print("Migration Summary")
    print("=" * 60)
    for phase, count in results.items():
        print(f"  {phase}: {count}")

    # Verify final counts
    if not args.dry_run:
        print("\nFinal destination counts:")
        try:
            result = neo4j_query(dst_neo4j, args.dst_neo4j_auth,
                                 "MATCH (n:Memory) RETURN count(n) AS cnt")
            print(f"  Neo4j Memory nodes: {result['results'][0]['data'][0]['row'][0]}")
        except Exception as e:
            print(f"  Neo4j count failed: {e}")

        try:
            url = f"{dst_qdrant}/collections/{QDRANT_COLLECTION}"
            with urllib.request.urlopen(url, timeout=10) as r:
                d = json.loads(r.read())
                print(f"  Qdrant points: {d['result']['points_count']}")
        except Exception as e:
            print(f"  Qdrant count failed: {e}")

    print("\n⚠️  After migration, restart MemOS API to clear caches:")
    print(f"   docker restart <memos-api-container>")
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
