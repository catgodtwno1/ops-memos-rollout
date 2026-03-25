---
name: ops-memos-rollout
description: Deploy, repair, and verify MemOS memory server on macOS for OpenClaw. Use when setting up MemOS from scratch, troubleshooting MemOS add/search failures, onboarding Mac minis as clients to a shared MemOS server, or diagnosing empty search results. Triggers on "MemOS", "memos server", "memory server", "memos deploy", "memos search empty", "memos not working".
---

# MemOS Rollout

## Architecture

MemOS is a REST API memory service (Docker: neo4j + qdrant + API server) providing structured long-term memory with auto-extraction of facts, preferences, and skills from conversations.

- Default port: **8765**
- Docker socket (macOS): `unix:///Users/scott/.colima/default/docker.sock`
- Provider: SiliconFlow (Qwen2.5-72B LLM + bge-m3 embedding)

## ⚠️ Critical: API Format

The #1 failure mode is wrong write format. **`/product/add` requires chat message array, NOT plain text.**

### Correct write format

```json
{
  "user_id": "openclaw",
  "session_id": "session-001",
  "async_mode": "sync",
  "messages": [
    {"role": "user", "content": "Memory content here"},
    {"role": "assistant", "content": "Acknowledged."}
  ]
}
```

### Wrong formats (silent failure — 200 but nothing stored)

- `{"text": "content"}` — field does not exist in API schema
- `{"messages": "string content"}` — string messages not supported

### Search format

```json
{"query": "search terms", "user_id": "openclaw"}
```

`user_id` is **required** for both add and search.

## Deploy New Server

```bash
bash scripts/memos_server_up.sh
```

Starts 3 containers: `memos-neo4j`, `memos-qdrant`, `memos-api`. Reads config from `~/.openclaw/services/memos-server/.env`.

If `.env` doesn't exist, generate it from Cognee's SiliconFlow config:

```bash
python3 scripts/configure_memos_server.py
```

## Verify

```bash
python3 scripts/memos_client_smoke_test.py --base-url http://127.0.0.1:8765 --user-id test
```

Performs real add+search cycle. Success = memory written AND recalled.

## Health Check

No `/health` endpoint. Use `/docs` (Swagger UI) or real business endpoints.

## Onboard Client Mac Mini

```bash
bash scripts/onboard_memos_client.sh --base-url http://SERVER_IP:8765 --user-id CLIENT_ID
```

## Install Autostart

```bash
bash scripts/install_memos_launchagent.sh
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Add 200 but search empty | Wrong write format (`text` field) | Use `messages` array with `{role, content}` objects |
| Add 200 but search empty | Missing `user_id` in search | Add `user_id` to search payload |
| `/health` returns 404 | No health route in this API version | Use `/docs` or business endpoints |
| Container won't start | Colima not running | `colima start` first |
| Embedding fails | SiliconFlow token invalid | Check `.env` `EMBEDDING_API_KEY` |
| Add timeout after ~130 writes | Neo4j O(N) scan, missing indexes | Apply Neo4j indexes (see Known Issues) |
| P99 latency >10s on add | Neo4j transaction backlog | Restart memos-api, apply indexes |
| Add takes 7-15s (NAS) | ASYNC_MODE=sync → fine mode → LLM call | Patch core.py to force fast mode (see NAS Deployment) |
| Container DNS resolution fails | Default bridge network | Use custom Docker network `oc-memory` |
| Patches lost on restart | Container filesystem is ephemeral | Bind mount patched files + docker commit |

## NAS Deployment (QNAP/Synology)

### Docker network for container DNS

Default Docker bridge network does **NOT** support container-name DNS resolution. Containers using hostnames like `oc-neo4j` or `oc-qdrant` will fail with `Name or service not known`.

**Fix:** Create a custom Docker network and attach all related containers:

```bash
DOCKER=/path/to/docker  # QNAP: /share/CACHEDEV1_DATA/.qpkg/container-station/bin/docker

$DOCKER network create oc-memory
$DOCKER network connect oc-memory oc-neo4j
$DOCKER network connect oc-memory oc-qdrant

# When creating memos-api, use --network oc-memory
$DOCKER run -d --name oc-memos-api --network oc-memory ...
```

### ASYNC_MODE=sync causes LLM bottleneck (fixed 2026-03-25)

**Symptom:** `/product/add` takes 7-15 seconds per request on NAS, even with very few nodes.

**Root cause:** `ASYNC_MODE=sync` → `core.py` selects `mode="fine"` → every add calls `mem_reader.get_memory(mode="fine")` → invokes MiniMax LLM for structured memory extraction per chat window. The LLM round-trip (~10-15s) is the bottleneck, NOT Neo4j.

**Fix:** Patch `core.py` to force fast mode (skips LLM, does text split + embedding only):

```python
# In /app/src/memos/mem_os/core.py, line ~763
# Change: mode="fast" if sync_mode == "async" else "fine"
# To:     mode="fast"
```

**Result:** 7,000-15,000ms → 320-725ms (~20x speedup)

### Persisting patches across container restarts

Patches inside Docker containers are lost on restart. Two strategies:

**Strategy 1: Bind mount patched files**
```bash
# Copy patched file out of container
$DOCKER cp oc-memos-api:/app/src/memos/mem_os/core.py /path/to/core_patched.py

# Add bind mount on container create
-v /path/to/core_patched.py:/app/src/memos/mem_os/core.py
```

**Strategy 2: Docker commit (backup)**
```bash
$DOCKER commit -m "Patches: fast mode + neo4j indexes" oc-memos-api local/memos-api:patched-YYYYMMDD
$DOCKER tag local/memos-api:patched-YYYYMMDD local/memos-api:latest
```

**Recommended:** Use both — bind mount for persistence, committed image as backup.

### Complete NAS container creation example

```bash
DOCKER=/share/CACHEDEV1_DATA/.qpkg/container-station/bin/docker

$DOCKER run -d \
  --name oc-memos-api \
  --restart unless-stopped \
  --network oc-memory \
  -p 8765:8000 \
  -e ASYNC_MODE=sync \
  -e NEO4J_BACKEND=neo4j-community \
  -e NEO4J_URI=bolt://oc-neo4j:7687 \
  -e NEO4J_USER=neo4j \
  -e NEO4J_PASSWORD=YOUR_PASSWORD \
  -e QDRANT_HOST=oc-qdrant \
  -e QDRANT_PORT=6333 \
  -e MOS_EMBEDDER_BACKEND=universal_api \
  -e MOS_EMBEDDER_PROVIDER=openai \
  -e MOS_EMBEDDER_MODEL=BAAI/bge-m3 \
  -e MOS_EMBEDDER_API_BASE=https://api.siliconflow.cn/v1 \
  -e MOS_EMBEDDER_API_KEY=YOUR_KEY \
  -e EMBEDDING_DIMENSION=1024 \
  -e PYTHONPATH=/app/src \
  -e TZ=Asia/Shanghai \
  -v /path/to/memos-data:/app/data \
  -v /path/to/neo4j_community_patched.py:/app/src/memos/graph_dbs/neo4j_community.py \
  -v /path/to/core_patched.py:/app/src/memos/mem_os/core.py \
  local/memos-api:latest \
  uvicorn memos.main:app --host 0.0.0.0 --port 8000 --workers 4
```

> **`--workers 4`**: Multi-worker uvicorn prevents a single slow Neo4j write from blocking all other requests. Especially important for NAS deployments where single-worker is the default.

## Multi-Machine Deployment (多台 Mac Mini 共用)

### User ID 策略

MemOS 用 `user_id` 做數據隔離（搜索時按 `cube_id` 過濾）。

| 場景 | 建議 | 原因 |
|------|------|------|
| 同一個人的多台機器 | **共用同一個 `user_id`**（如 `scott`） | 記憶互通，任何一台存的記憶其他台都能搜到 |
| 不同人共用 NAS MemOS | **各用各的 `user_id`** | 數據隔離，互不干擾 |
| 壓測/測試 | **用獨立 `user_id`**（如 `stress-test`） | 避免測試數據污染正式記憶 |

### OpenClaw 配置（每台機器的 openclaw.json）

```json
{
  "plugins": {
    "entries": {
      "memos": {
        "config": {
          "baseUrl": "http://10.10.10.66:8765",
          "userId": "scott"
        }
      }
    }
  }
}
```

**⚠️ 關鍵規則：**
- 四台 Mac Mini 都指向同一個 NAS MemOS（`http://10.10.10.66:8765`）
- 都用同一個 `userId: "scott"`（同一人的記憶應互通）
- **不要**在壓測時用正式 `userId`，用 `stress-test` 或其他測試 ID
- 壓測後記得清理測試數據（`--cleanup` 參數）

### 避免並發衝突

多台機器同時寫入同一個 NAS MemOS 時，Neo4j 可能出現鎖競爭：
- **日常使用**（稀疏寫入）：完全沒問題
- **批量導入/壓測**：同一時間只能一台在跑，否則 timeout 雪崩
- **壓測前**：先清理舊測試數據，減少 Neo4j 節點數

## Known issues (2026-03-24)

### neo4j write bottleneck under sustained load

**Symptom:** `/product/add` returns 200 for ~40 consecutive writes, then starts timing out (10s+). Search remains stable.

**Root cause:** `add_nodes_batch` in neo4j blocks during graph node creation. Under sustained write pressure, the graph computation queue backs up.

**Impact:** Normal usage is fine (writes are sparse). Only affects burst scenarios like batch imports or stress tests.

**Mitigation:**
- Increase timeout if doing batch imports
- Batch multiple facts into a single `messages` array rather than one-per-call
- If neo4j hangs completely: `docker restart memos-api`
- Use `--workers 4` in uvicorn for multi-worker concurrency (NAS especially)

### Neo4j O(N) full-scan bottleneck (fixed 2026-03-25)

**Symptom:** After ~130 consecutive writes, `/product/add` starts timing out at 15s+. P50 stays normal (~90ms) but P99 explodes.

**Root cause:** Two O(N) patterns in MemOS code:
1. `get_all_memory_items(scope="WorkingMemory")` in `tree.py:122` — full `MATCH (n:Memory) RETURN n` scan on every add
2. `remove_oldest_memory()` uses `ORDER BY ... SKIP N LIMIT 1` — O(N) with large node counts

**Fix applied:**
1. Created composite index: `CREATE INDEX memory_type_user_index FOR (n:Memory) ON (n.memory_type, n.user_name)`
2. Created range index: `CREATE RANGE INDEX memory_updated_at_index FOR (n:Memory) ON (n.updated_at)`
3. Created unique constraint: `CREATE CONSTRAINT memory_id_unique FOR (n:Memory) REQUIRE n.id IS UNIQUE`
4. Neo4j memory tuning: heap 512m, pagecache 512M, transaction limits 64m/256m

**Verification (post-fix):**
- 100 rounds (indexed, few nodes): P50=89ms, P95=124ms, Max=176ms, 0 errors ✅
- 500 rounds (1400+ nodes, 200ms throttle): **Still fails** — O(N) scan is in application code (`get_all_memory_items`), not purely a DB index issue. Indexes help with small node counts but can't fix the Python-level full scan + in-memory dedup.

**Post-patch verification (2026-03-25):**

After patching `core.py` to force `mode="fast"`, the O(N) bottleneck is effectively mitigated for normal usage:

| 環境 | 輪次 | 成功 | Avg | P50 | P95 | P99 | Max | 衰退 |
|------|------|------|-----|-----|-----|-----|-----|------|
| 本機 | 500 | 500/0 | 19ms | — | — | — | — | — |
| NAS | 500 | 500/0 | 92ms | 56ms | 280ms | 348ms | 421ms | 0.9x ✅ |

**Conclusion:** Patch 生效後 500 輪零錯誤、無衰退。日常使用完全沒問題。

### 獨立壓測腳本

```bash
# 本機測試
python3 scripts/memos_stress_test.py --rounds 500

# NAS 測試
python3 scripts/memos_stress_test.py --url http://10.10.10.66:8765 --rounds 500

# 測試完清理數據
python3 scripts/memos_stress_test.py --url http://10.10.10.66:8765 --rounds 100 --cleanup
```

判定標準：
- ✅ PASS: 零錯誤 + 衰退 ≤ 1.5x
- ⚠️ WARN: 零錯誤但衰退 > 1.5x（patch 可能部分失效）
- ❌ FAIL: 有錯誤（timeout/HTTP error）

**To apply indexes on a new deployment:**
```bash
docker exec memos-neo4j cypher-shell -u neo4j -p password "
CREATE INDEX memory_type_user_index IF NOT EXISTS FOR (n:Memory) ON (n.memory_type, n.user_name);
CREATE RANGE INDEX memory_updated_at_index IF NOT EXISTS FOR (n:Memory) ON (n.updated_at);
CREATE CONSTRAINT memory_id_unique IF NOT EXISTS FOR (n:Memory) REQUIRE n.id IS UNIQUE;
"
```

### SimpleStruct MemReader warning

**Symptom:** Logs show `SimpleStruct MemReader does not support str message data now, your messages contains [None], skipping`

**Root cause:** Some `/product/add` calls pass content as plain string instead of chat message array. The API returns 200 but nothing is actually stored.

**Fix:** Always use the correct message format (see "Critical: API Format" above).

## Data Migration (跨機器遷移)

### When to migrate

- Moving from local Docker to NAS
- Consolidating multiple machines' memory into one server
- Disaster recovery from backup

### Migration script

```bash
# Full migration: local Mac → NAS (with dedup)
python3 scripts/memos_migrate.py \
  --src http://127.0.0.1 \
  --dst http://10.10.10.66 \
  --src-neo4j-auth neo4j:12345678 \
  --dst-neo4j-auth neo4j:openclaw2026

# Dry run first (count only, no writes)
python3 scripts/memos_migrate.py \
  --src http://127.0.0.1 \
  --dst http://10.10.10.66 \
  --dry-run

# Skip Qdrant (only migrate Neo4j nodes)
python3 scripts/memos_migrate.py \
  --src http://127.0.0.1 \
  --dst http://10.10.10.66 \
  --skip-qdrant

# Custom ports
python3 scripts/memos_migrate.py \
  --src http://192.168.1.100 \
  --dst http://192.168.1.200 \
  --src-neo4j-port 7474 --dst-neo4j-port 7474 \
  --src-qdrant-port 6333 --dst-qdrant-port 6333
```

### What it migrates

| Phase | Data | Method | Dedup |
|-------|------|--------|-------|
| 1 | Qdrant vectors | Scroll API → Upsert | By point UUID |
| 2 | Neo4j Memory nodes | HTTP API + parameterized UNWIND | By node ID |
| 3 | Fix stringified lists | Auto-repair imported nodes | — |

### Known pitfalls

1. **List fields become strings**: Neo4j HTTP API imports `[]` as `"[]"` (string). Phase 3 auto-fixes this. Without the fix, MemOS Pydantic validation fails and search returns empty results.

2. **Don't use cypher-shell for import**: Multi-language text (Chinese, apostrophes, etc.) breaks cypher string escaping. Always use HTTP API with parameterized queries.

3. **NAS /tmp is tiny**: QNAP `/tmp` is a 64MB tmpfs. Don't try to export snapshots there. Use rsync or HTTP API streaming.

4. **Restart MemOS after migration**: The API caches some state. Always restart the API container after migration to pick up new data.

5. **Concurrent writes during migration**: Don't run MemOS writes from other clients during migration — Neo4j transaction conflicts can cause timeout cascades.

### Prerequisites

- Both Neo4j instances must have HTTP API enabled (port 7474 by default)
- Both Qdrant instances must be accessible (port 6333)
- Python 3 (stdlib only, no pip install needed)
- Network connectivity between source and destination

### Post-migration verification

```bash
# Restart destination MemOS API
docker restart <memos-api-container>

# Run smoke test against destination
python3 scripts/memos_client_smoke_test.py --base-url http://DEST_IP:8765 --user-id openclaw

# Run stress test to confirm stability
python3 scripts/memos_stress_test.py --url http://DEST_IP:8765 --rounds 100
```

## Scripts

| Script | Purpose |
|--------|---------|
| `scripts/configure_memos_server.py` | Generate .env from Cognee SiliconFlow config |
| `scripts/memos_server_up.sh` | Start Docker containers |
| `scripts/memos_server_status.sh` | Check container status |
| `scripts/memos_server_down.sh` | Stop containers |
| `scripts/install_memos_launchagent.sh` | macOS autostart |
| `scripts/memos_client_smoke_test.py` | Real add+search validation |
| `scripts/onboard_memos_client.sh` | One-command client onboard |
| `scripts/memos_stress_test.py` | 500 輪壓測（驗證 O(N) patch） |
| `scripts/memos_migrate.py` | 跨機器數據遷移（Qdrant + Neo4j，自動去重） |
