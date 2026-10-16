#!/usr/bin/env python3
"""Fake Elasticsearch for exercising estop without a cluster.

    python3 tests/mock_es.py &              # listens on 127.0.0.1:9299
    ES_URL=http://127.0.0.1:9299 python3 estop.py -i 2 --index-prefix pd_ --node-strip 'mock-'

Serves the subset of _cluster / _nodes / _cat endpoints estop reads, with counters
that grow over time so deltas are non-zero. Deliberately misconfigured (32g heap,
compressed oops off on data-0, refresh -1, max_result_window 100000, 600-field
mapping, yellow health) so every alert and audit rule has something to say.
"""
import json, random, sys, time
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9299
t0 = time.time()
NODES = [f"mock-data-{i}" for i in range(4)] + ["mock-coord-0", "mock-master-0"]


def el():
    return time.time() - t0


def roles(n):
    return ["data"] if "data" in n else (["ingest"] if "coord" in n else ["master"])


def nodestats():
    out = {}
    for i, n in enumerate(NODES):
        data = "data" in n; e = el(); f = 1 + i * 0.2
        out[n] = {"name": n, "roles": roles(n),
                  "os": {"cpu": {"percent": random.randint(30, 95), "load_average": {"1m": random.uniform(3, 14)}},
                         "mem": {"total_in_bytes": 64e9, "free_in_bytes": 1e9}, "swap": {"used_in_bytes": 0}},
                  "jvm": {"mem": {"heap_used_percent": random.randint(30, 88), "heap_max_in_bytes": 32 * 2**30},
                          "gc": {"collectors": {"young": {"collection_count": int(e), "collection_time_in_millis": int(e * 50)},
                                                "old": {"collection_count": int(e // 40), "collection_time_in_millis": int(e * 5)}}}},
                  "fs": {"total": {"total_in_bytes": 4e12, "available_in_bytes": 1e12},
                         "io_stats": {"total": {"read_kilobytes": int(e * 200 * 1024 * f), "write_kilobytes": int(e * 20 * 1024),
                                                "read_operations": int(e * 1200), "write_operations": int(e * 100)}}},
                  "thread_pool": {"search": {"active": 13 if data else 0, "queue": random.randint(0, 300) if data else 0,
                                             "rejected": int(e * 20) if data else 0, "largest": 13},
                                  "write": {"queue": 0}, "get": {"queue": 0}},
                  "breaker": {"parent": {"tripped": 0}}, "http": {"current_open": 300 if not data else 2},
                  "indices": {"store": {"size_in_bytes": 1e12 if data else 0},
                              "search": {"query_total": int(e * 120 * f), "query_time_in_millis": int(e * 120 * 45 * f),
                                         "fetch_total": int(e * 20), "fetch_time_in_millis": int(e * 20 * 180),
                                         "scroll_current": 3, "open_contexts": 40},
                              "indexing": {"index_total": int(e * 3)},
                              "merges": {"current": i % 2, "total_size_in_bytes": int(e * 1e6)},
                              "query_cache": {"hit_count": int(e * 5), "miss_count": int(e * 95), "evictions": int(e * 30)},
                              "request_cache": {"hit_count": int(e * 40), "miss_count": int(e * 60), "evictions": int(e * 2)},
                              "fielddata": {"memory_size_in_bytes": 1e6}, "segments": {"memory_in_bytes": 1e8}}}
    return out


def nodeinfo():
    return {n: {"name": n, "roles": roles(n), "version": "7.17.28",
                "os": {"allocated_processors": 8, "mem": {"total_in_bytes": 64e9}},
                "jvm": {"version": "22", "mem": {"heap_max_in_bytes": 32 * 2**30},
                        "using_compressed_ordinary_object_pointers": "false" if n == "mock-data-0" else "true"},
                "settings": {}} for n in NODES}


HOT = """::: {mock-data-0}{abc}{127.0.0.1}
   Hot threads at 2026-10-01T10:00:00Z, interval=500ms, busiestThreads=6, ignoreIdleThreads=true:

   100.0% [cpu=8.0%, other=92.0%] (500ms out of 500ms) cpu usage by thread 'elasticsearch[mock-data-0][search][T#3]'
     3/10 snapshots sharing following 20 elements
       sun.nio.ch.FileDispatcherImpl.pread0(Native Method)
   100.0% [cpu=97.0%, other=3.0%] (500ms out of 500ms) cpu usage by thread 'elasticsearch[mock-data-0][search][T#7]'
   100.0% [cpu=12.0%, other=88.0%] (500ms out of 500ms) cpu usage by thread 'elasticsearch[mock-data-0][search][T#1]'
   60.0% [cpu=60.0%, other=0.0%] (300ms out of 500ms) cpu usage by thread 'elasticsearch[mock-data-0][[pd_idx_1][3]: Lucene Merge Thread #12]'
::: {mock-data-1}{def}{127.0.0.2}
   100.0% [cpu=15.0%, other=85.0%] (500ms out of 500ms) cpu usage by thread 'elasticsearch[mock-data-1][search][T#2]'
   40.0% [cpu=40.0%, other=0.0%] (200ms out of 500ms) cpu usage by thread 'elasticsearch[mock-data-1][transport_worker][T#4]'
"""


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        p = self.path; e = el()
        if p.startswith("/_cluster/health"):
            b = {"cluster_name": "mock", "status": "yellow", "number_of_nodes": 6, "number_of_data_nodes": 4,
                 "active_shards": 100, "active_primary_shards": 60, "unassigned_shards": 2, "relocating_shards": 0,
                 "initializing_shards": 0, "number_of_pending_tasks": 0}
        elif p.startswith("/_cluster/settings"):
            b = {"persistent": {"cluster.routing.allocation.node_concurrent_recoveries": "8",
                                "indices.recovery.max_bytes_per_sec": "200mb"},
                 "transient": {}, "defaults": {"thread_pool.search.queue_size": "1000"}}
        elif p.startswith("/_nodes/stats"):
            b = {"nodes": nodestats()}
        elif p.startswith("/_nodes/hot_threads"):
            self.send_response(200); self.send_header("Content-Type", "text/plain"); self.end_headers()
            self.wfile.write(HOT.encode()); return
        elif p.startswith("/_nodes/"):
            b = {"nodes": nodeinfo()}
        elif p.startswith("/_cat/indices"):
            b = [{"index": f"pd_idx_{j}", "health": "green", "pri": "40", "rep": "1" if j else "0",
                  "docs.count": "500000000", "docs.deleted": str(int(1e8 * (j + 1) * 0.5)),
                  "pri.store.size": str(int(9e11)), "store.size": str(int(1.8e12)), "segments.count": "2500",
                  "search.query_total": str(int(e * 40 * (j + 1))), "search.query_time": f"{e * 40 * (j + 1) * 0.05:.1f}s",
                  "search.fetch_total": str(int(e * 6)), "search.fetch_time": f"{e * 6 * 0.2:.1f}s",
                  "indexing.index_total": "0", "merges.current": "0", "refresh.total": "1", "refresh.time": "1s",
                  "search.scroll_current": "0"} for j in range(4)]
        elif p.startswith("/_cat/shards"):
            b = [{"index": f"pd_idx_{j}", "shard": str(s), "prirep": pr, "state": "STARTED", "node": NODES[(s + j) % 4],
                  "docs": "12000000", "store": str(int(2.2e10)), "segments.count": "30",
                  "search.query_total": str(int(e * (j + 1) * (5 if s != 7 else 25))),
                  "search.query_time": f"{e * (j + 1) * (0.25 if s != 7 else 2.0):.1f}s",
                  "search.fetch_time": f"{e * 0.1:.1f}s", "merges.current": "0"}
                 for j in range(4) for s in range(40) for pr in ("p", "r") if not (j == 0 and pr == "r")]
        elif p.startswith("/_cat/aliases"):
            b = [{"alias": "pd_activity", "index": f"pd_idx_{j}"} for j in range(4)]
        elif p.startswith("/_cat/recovery"):
            b = []
        elif "/_settings" in p:
            b = {f"pd_idx_{j}": {"settings": {"index.refresh_interval": "-1" if j == 0 else "60s", "index.max_result_window": "100000"},
                                 "defaults": {"index.codec": "default", "index.search.slowlog.threshold.query.warn": "-1"}} for j in range(4)}
        elif "/_mapping" in p:
            b = {f"pd_idx_{j}": {"mappings": {"properties": {f"f{k}": {"type": "text" if k % 2 else "keyword",
                                                                       "fields": {"keyword": {"type": "keyword"}}} for k in range(600)}}} for j in range(4)}
        else:
            self.send_response(404); self.end_headers(); return
        d = json.dumps(b).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(d)


if __name__ == "__main__":
    print(f"mock ES on http://127.0.0.1:{PORT}", file=sys.stderr)
    HTTPServer(("127.0.0.1", PORT), H).serve_forever()
