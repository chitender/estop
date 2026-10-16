# estop

**htop for Elasticsearch search performance.** A single-file, stdlib-only, read-only
terminal monitor that shows what is actually making your searches slow — thread-pool
saturation, fetch-phase bloat, page-cache starvation, deleted-doc debt, alias fan-out,
hot shards, JVM misconfiguration — as live rates, not lifetime counters.

Built while diagnosing a production cluster where per-shard query latency had drifted
to 20–77 ms and search queues were rejecting ~30 requests/s per node. Every rule in
the alert and audit tables corresponds to something that actually hurt.

```
 ▶ estop  prod-search  ·  ES 7.17.28  ·  14:02:11  ·  5s                  search health  31 ████▌·······
cluster GREEN   nodes 17 (data 12)  shards 1242/621 pri   unassigned 0  reloc 0  init 0  recov 0  pending 0   data 11.2 TB
 1 overview  2 nodes  3 indices  4 hot shards  5 alerts  6 config audit    ⚠ 4 critical  8 warning    sort:severity
── SEARCH ───────────────────────────────────────────────────────────────────────────────────────────────────────────
 shard-queries/s   1119.5    ▂▃▃▄▅▆▇█▇▆▅▅▄▄▃▃▂▂▂▃▄▅▆▇      RESOURCES
 avg query ms         3.1    ▁▁▁▁▂▂▃▇█▇▃▂▁▁▁▁▁▁▁▁▁▁▁▁      avg cpu %          42  ▃▄▄▅▆▅▄▃▃▄▅▅▆▆▅▄
 fetch share %         22    ▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂      page-cache cov     3%
 rejections/s        0.00    ▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁      disk read MB/s    187  ▅▆▇█▇▆▅▅▆▇██▇▆▅▅
 search pool        3/588    ·······················   1%
```

## Install

```bash
curl -O https://raw.githubusercontent.com/chitender/estop/main/estop.py
chmod +x estop.py
```

Python 3.8+. No dependencies. Uses `curses` when available and falls back to a plain
ANSI renderer (`--plain`) otherwise. Needs the `monitor` cluster privilege only.

## Run

```bash
ES_URL=https://es.example.com:9200 ES_USER=monitor ES_PASS='...' ./estop.py -i 5
ES_URL=https://es:9200 ES_API_KEY='...' INSECURE=1 ./estop.py --index-prefix pd_
ES_URL=... ./estop.py --once            # two samples, plain-text report, exit — paste into Slack
```

| Flag | Meaning |
|---|---|
| `-i N` | sample interval in seconds (default 5) |
| `-n N` | rows in indices / hot-shards views (default 25) |
| `--index-prefix pd_` | restrict index and shard views to one prefix (`ES_INDEX_PREFIX`) |
| `--hot-every N` | sample `_nodes/hot_threads` every N ticks (default 6, i.e. 30 s at `-i 5`) |
| `--no-hot` | never call `hot_threads` |
| `--node-strip 'org-es-prod-,-vm'` | substrings removed from node names for display (`ES_NODE_STRIP`) |
| `--no-tasks` | never call the `_tasks` API (view 7) |
| `--show-queries` | show search request bodies in view 7 — off by default because bodies can contain PHI/PII |
| `--plain` | force the ANSI renderer |
| `--once` | one-shot plain-text report including the config audit |
| `--once --json` | the same data as one JSON document (incl. raw `hot_threads`) to attach to a ticket or incident |

Keys: `1`–`7` switch views · `s` cycle sort column · `p` pause · `h` sample hot threads now · `d` dump a JSON snapshot to `./estop-<timestamp>.json` · `+`/`-` interval · `q` quit.
Polling runs on a background thread, so a slow cluster never freezes the keyboard or the clock.

## Views

1. **Overview** — 0–100 *search health* score (penalised by live alerts), 60-sample sparklines for
   shard-q/s, query ms, fetch ms, fetch share, rejections, queue depth, CPU, heap, disk read,
   indexing; search-pool gauge; hot-threads panel showing what fraction of busy search threads
   are **off-CPU** (the direct disk-bound vs CPU-bound answer); top alerts.
2. **Nodes** — per data node: CPU, load/core, heap, old-GC/s, **page-cache coverage**
   `(RAM − heap) ÷ indexed bytes`, disk read MB/s and IOPS, shards hosted, search pool
   active/max, queue, rejections/s, shard-q/s, query ms, fetch/s, fetch ms, fetch share,
   query- and request-cache hit %, merges, open search contexts. Coordinators and masters in a
   separate block.
3. **Indices** — pri/rep, docs, deleted %, GB/shard, segments/shard, live and lifetime query
   cost, fetch share, **hot-shard skew** (hottest shard ÷ mean, shard and node named),
   `refresh_interval`, `max_result_window`, mapped-field and text-field counts, codec;
   alias fan-out footer (primary shards touched per query through each alias).
4. **Hot shards** — every shard ranked by search-thread-seconds per wall-second
   (1.00 = one thread fully consumed), plus search-load share per node to spot imbalance.
5. **Alerts** — ~40 rate-based rules, most severe first.
6. **Config audit** — static PASS/WARN/FAIL table refreshed every 5 minutes: compressed oops,
   heap ≤ 31 GB and ≤ 50 % RAM, dedicated coordinating nodes, recovery concurrency and
   throttle, search queue size, swap, shards per node, page-cache coverage, and per hot index:
   replicas, `max_result_window`, `refresh_interval`, field count and text ratio, nested fields,
   codec, slowlog enabled, shard size, alias fan-out.

7. **Live tasks** — *which query is hurting the cluster right now*: every in-flight coordinating
   search ordered by running time (node, indices, search type, `size`, whether it aggregates, and the
   task id to paste into `POST _tasks/<id>/_cancel`), plus background work that steals I/O from search
   (`forcemerge`, `reindex`, `delete_by_query`, snapshot). When shards are unassigned it shows the
   `_cluster/allocation/explain` reason and the top decider; when cluster-state tasks are pending it
   shows the head of `_cluster/pending_tasks`. Searches running ≥ 10 s / 30 s raise alerts.

Also new in the nodes view and alerts: write-pool queue and rejections (bulk indexers getting 429),
fielddata heap, and background-task alerts.

## What it reads

`_cluster/health`, `_cluster/settings`, `_nodes/stats`, `_nodes/<info>`, `_cat/indices`,
`_cat/shards`, `_cat/aliases`, `_cat/recovery`, `<index>/_settings`, `<index>/_mapping`,
`_tasks` (unless `--no-tasks`), `_cluster/allocation/explain` and `_cluster/pending_tasks` (only while
shards are unassigned / tasks pending) and, unless `--no-hot`, `_nodes/hot_threads`. Everything is a GET. The only call with
non-trivial cost on the cluster is `hot_threads` (a 500 ms sampling window per node);
the 30 s default is fine, do not set `--hot-every 1`.

## Thresholds

All thresholds live in the `T` dict at the top of `estop.py`. The disk-throughput warning
(`read_mbps_warn/crit = 150/240`) is tuned for Azure P50 managed disks (~250 MB/s cap);
adjust for your storage.

## Known limits

- Page-cache coverage assumes one data node per VM. Co-located nodes will over-count.
- Hot-shard skew is computed from one interval's deltas. On a near-idle cluster a single
  slow fan-out query that straddles the sample boundary can make three random shards look
  "hot". Treat skew as a prompt to check `_cat/tasks?actions=*search*&detailed`, not a verdict.
- Node-level `fs.io_stats` is Linux-only; the disk-read columns are zero elsewhere.
- View 7 only sees tasks alive at the instant of the poll; a 2 s query can fall between 5 s ticks. It is
  for stuck or long queries, not a replacement for the search slow log.
- Developed and verified against ES 7.17; 8.x is expected to work but has had less testing.

## Test without a cluster

```bash
python3 tests/mock_es.py &
ES_URL=http://127.0.0.1:9299 ./estop.py -i 2 --index-prefix pd_ --node-strip mock-
```

The mock is deliberately broken (32 GB heap with compressed oops off, refresh disabled,
`max_result_window` 100000, 1200-field mappings, yellow health, saturated pools) so every
rule has something to fire on.

## License

MIT
