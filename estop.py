#!/usr/bin/env python3
"""
estop v2 - realtime Elasticsearch search-performance monitor (htop for ES).

Stdlib only. Read-only. ES 7.x / 8.x.

  ES_URL=https://es:9200 ES_USER=... ES_PASS='...' INSECURE=1 ./estop.py [-i 5] [--index-prefix pd_]
  ES_API_KEY=...  ./estop.py
  --plain         force ANSI renderer (no curses)     --once   one snapshot to stdout
  --no-hot        skip hot_threads sampling           --hot-every N   sample hot threads every N ticks (default 6)
  --node-strip 'prefix-,-suffix'   shorten node names for display (or ES_NODE_STRIP)
  --no-tasks      skip the _tasks API         --show-queries  show request bodies in view 7   --once --json   machine-readable snapshot

Views   1 overview  2 nodes  3 indices  4 hot shards  5 alerts  6 config audit  7 live tasks
Keys    s sort · p pause · h sample hot threads now · d dump JSON snapshot · + / - interval · q quit

Colours  green ok · yellow watch · red problem · magenta critical
"""

import argparse
import base64
import collections
import json
import os
import re
import select
import shutil
import ssl
import sys
import threading
import time
import unicodedata
import urllib.error
import urllib.request

try:
    import curses  # noqa
    HAVE_CURSES = True
except ImportError:
    HAVE_CURSES = False
    curses = None

__version__ = "2.1.0"

# ───────────────────────── thresholds ─────────────────────────
T = dict(
    heap_warn=75, heap_crit=85,
    cpu_warn=70, cpu_crit=90,
    load_warn=1.0, load_crit=1.5,              # load_1m / cores
    disk_warn=80, disk_crit=90,
    queue_warn=50, queue_crit=200,
    rej_warn=0.1, rej_crit=5.0,                # /s
    qms_warn=50, qms_crit=150,                 # avg ms per shard query phase
    fms_warn=100, fms_crit=250,                # avg ms per shard fetch phase
    fetch_share_warn=40, fetch_share_crit=60,  # % of search time in fetch
    del_warn=15, del_crit=25,                  # deleted docs %
    seg_warn=30, seg_crit=50,                  # segments / shard
    shard_gb_warn=50, shard_gb_crit=100,
    oldgc_warn=0.05, oldgc_crit=0.5,           # old-gen GCs / s
    alias_shards_warn=60, alias_shards_crit=150,
    cache_cov_warn=15, cache_cov_crit=5,       # % of indexed bytes that fit in (RAM - heap)   (lower is worse)
    io_bound_warn=40, io_bound_crit=70,        # % of hot search threads off-CPU (I/O / wait)
    qcache_hit_warn=10,                        # % (lower is worse)  informational
    shards_per_node_warn=600, shards_per_node_crit=1000,
    fields_warn=500, fields_crit=1000,         # mapped fields per index
    text_ratio_warn=30,                        # % of fields that are text
    skew_warn=2.0, skew_crit=4.0,              # hottest shard query-time / mean, within an index
    mrw_warn=10000,                            # max_result_window above default
    read_mbps_warn=150, read_mbps_crit=240,    # per node disk read MB/s (Azure P50 cap ~250)
    ctx_warn=500, ctx_crit=2000,               # open search contexts per node
    task_warn_s=10, task_crit_s=30,            # a single search task running this long (seconds)
    fielddata_warn_mb=1024,                    # fielddata heap per node
    recovery_mbps_warn=100,                    # indices.recovery.max_bytes_per_sec above this (MB/s)
)

OK, WARN, CRIT, FATAL, DIM, HEAD, TITLE, ACC = 1, 2, 3, 4, 5, 6, 7, 8
SEV = {FATAL: "FATAL", CRIT: "CRIT ", WARN: "WARN "}
SPARK = "▁▂▃▄▅▆▇█"
HIST = 60
NODE_STRIP = []  # substrings removed from node names for display (--node-strip / ES_NODE_STRIP)


def short_node(name, width=26):
    for sub in NODE_STRIP:
        name = name.replace(sub, "")
    return name[:width]


# ───────────────────────── HTTP ─────────────────────────
class ES:
    def __init__(self):
        self.url = os.environ.get("ES_URL", "http://localhost:9200").rstrip("/")
        self.headers = {"Accept": "application/json"}
        key, user = os.environ.get("ES_API_KEY"), os.environ.get("ES_USER")
        if key:
            self.headers["Authorization"] = f"ApiKey {key}"
        elif user:
            tok = base64.b64encode(f"{user}:{os.environ.get('ES_PASS', '')}".encode()).decode()
            self.headers["Authorization"] = f"Basic {tok}"
        self.ctx = None
        if self.url.startswith("https"):
            self.ctx = ssl.create_default_context()
            if os.environ.get("INSECURE") == "1":
                self.ctx.check_hostname = False
                self.ctx.verify_mode = ssl.CERT_NONE

    def get(self, path, timeout=25, raw=False):
        req = urllib.request.Request(self.url + path, headers=self.headers if not raw else {})
        if raw:
            for k, v in self.headers.items():
                if k != "Accept":
                    req.add_header(k, v)
        with urllib.request.urlopen(req, timeout=timeout, context=self.ctx) as r:
            data = r.read().decode()
            return data if raw else json.loads(data)


def num(x, d=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def dur_ms(s):
    if s is None:
        return 0.0
    s = str(s)
    for suf, mul in (("micros", 0.001), ("nanos", 1e-6), ("ms", 1), ("s", 1000), ("m", 60000), ("h", 3600000), ("d", 86400000)):
        if s.endswith(suf):
            return num(s[: -len(suf)]) * mul
    return num(s)


def parse_bytes(s, default=0.0):
    """'200mb' / '2GB' / '40m' / '1073741824' -> bytes."""
    m = re.match(r"\s*([\d.]+)\s*([kmgtp]?)b?\s*$", str(s).lower())
    if not m:
        return default
    return float(m.group(1)) * 1024 ** ("", "k", "m", "g", "t", "p").index(m.group(2))


def clean(text):
    """Replace control characters (ESC, CSI 0x9b, ...) in server-controlled text so it cannot inject terminal escapes."""
    return "".join("?" if unicodedata.category(ch).startswith("C") else ch for ch in str(text))


def gb(b):
    return num(b) / 2**30


def human_bytes(b):
    b = num(b)
    for u in ("B", "K", "M", "G", "T"):
        if b < 1024 or u == "T":
            return f"{b:.0f}{u}" if u in ("B", "K") else f"{b:.1f}{u}"
        b /= 1024.0


def dget(d, *path, default=0):
    for p in path:
        d = d.get(p, {}) if isinstance(d, dict) else {}
    return d if isinstance(d, (int, float)) else default


# ───────────────────────── model ─────────────────────────
class Model:
    CAT_IDX_COLS = ("index,health,pri,rep,docs.count,docs.deleted,pri.store.size,store.size,segments.count,"
                    "search.query_total,search.query_time,search.fetch_total,search.fetch_time,"
                    "indexing.index_total,merges.current,refresh.total,refresh.time,search.scroll_current")
    CAT_SHARD_COLS = "index,shard,prirep,state,node,docs,store,segments.count,search.query_total,search.query_time,search.fetch_time,merges.current"

    # tasks that compete with search for CPU / disk when they run during peak
    BG_ACTIONS = ("indices:data/write/reindex", "indices:data/write/delete/byquery", "indices:data/write/update/byquery",
                  "indices:admin/forcemerge", "cluster:admin/snapshot/create")
    TASK_ACTIONS = ",".join(("indices:data/read/search",) + BG_ACTIONS)

    def __init__(self, es, index_prefix, hot_every, no_hot, no_tasks=False, show_queries=False):
        self.es = es
        self.pfx = index_prefix or ""
        self.hot_every = max(hot_every, 1)
        self.no_hot = no_hot
        self.no_tasks = no_tasks
        self.show_queries = show_queries
        self.tick = 0
        self.tasks = []          # long-running search tasks, longest first
        self.bg_tasks = []       # reindex / by-query / forcemerge / snapshot
        self.explain = None      # why the first unassigned shard is unassigned
        self.pending_top = []    # head of the cluster-state queue
        self.hot_raw = ""        # last raw hot_threads text (for the support bundle)
        self.task_err = None
        self.err = None
        self.prev = None
        self.cur = None
        # static
        self.info = {}
        self.cluster_settings = {}
        self.idx_settings = {}
        self.idx_mapping = {}
        self.alias_shards = {}
        self.version = "?"
        # computed
        self.cluster = {}
        self.rows_nodes = []
        self.rows_other = []
        self.rows_idx = []
        self.rows_shards = []
        self.alerts = []
        self.checks = []
        self.hot = dict(ts=0, total=0, search=0, io_bound=0, kinds=collections.Counter(), top=[])
        self.hist = collections.defaultdict(lambda: collections.deque(maxlen=HIST))
        self.score = 100
        self._static()

    # ---------- static collection (every ~5 min)
    def _static(self):
        info = self.es.get("/_nodes/_all/os,jvm,roles,settings,version")["nodes"]
        self.info = {}
        for k, v in info.items():
            roles = set(v.get("roles", []))
            self.info[k] = dict(
                name=v.get("name", k), roles=roles,
                is_data=any(r.startswith("data") for r in roles),
                cores=v.get("os", {}).get("allocated_processors") or v.get("os", {}).get("available_processors") or 1,
                ram=num(v.get("os", {}).get("mem", {}).get("total_in_bytes", 0)) if isinstance(v.get("os", {}).get("mem"), dict) else 0,
                heap_max=num(dget(v, "jvm", "mem", "heap_max_in_bytes")),
                coops=v.get("jvm", {}).get("using_compressed_ordinary_object_pointers"),
                jvm=v.get("jvm", {}).get("version", "?"),
                settings=v.get("settings", {}),
            )
            self.version = v.get("version", self.version)
        try:
            cs = self.es.get("/_cluster/settings?include_defaults=true&flat_settings=true")
            flat = {}
            for sec in ("defaults", "persistent", "transient"):
                flat.update(cs.get(sec, {}))
            self.cluster_settings = flat
        except Exception:  # noqa
            self.cluster_settings = {}

    def _static_indices(self, top_names):
        if not top_names:
            return
        names = ",".join(top_names)
        try:
            st = self.es.get(f"/{names}/_settings?include_defaults=true&flat_settings=true")
            for i, v in st.items():
                s = {}
                s.update(v.get("defaults", {}))
                s.update(v.get("settings", {}))
                self.idx_settings[i] = s
        except Exception:  # noqa
            pass
        try:
            mp = self.es.get(f"/{names}/_mapping")
            for i, v in mp.items():
                props = v.get("mappings", {})
                cnt = collections.Counter()
                self._walk(props, cnt)
                self.idx_mapping[i] = dict(fields=cnt["all"], text=cnt["text"], nested=cnt["nested"], fielddata=cnt["fielddata"])
        except Exception:  # noqa
            pass

    def _walk(self, node, cnt):
        if not isinstance(node, dict):
            return
        for k, v in node.items():
            if k in ("properties", "fields") and isinstance(v, dict):
                for fname, fdef in v.items():
                    if isinstance(fdef, dict):
                        if "type" in fdef or "properties" in fdef:
                            cnt["all"] += 1
                            if fdef.get("type") == "text":
                                cnt["text"] += 1
                            if fdef.get("type") == "nested":
                                cnt["nested"] += 1
                            if fdef.get("fielddata") is True:
                                cnt["fielddata"] += 1
                        self._walk(fdef, cnt)
            elif isinstance(v, dict):
                self._walk(v, cnt)

    # ---------- per-tick
    def refresh(self):
        try:
            s = dict(t=time.time())
            s["health"] = self.es.get("/_cluster/health")
            s["nodes"] = self.es.get("/_nodes/stats/indices,os,jvm,fs,thread_pool,breaker,process,transport,http")["nodes"]
            pat = f"{self.pfx}*" if self.pfx else "*"
            # time=ms: without it ES prints '1.3h' / '59.5m', far too coarse to diff between ticks
            s["indices"] = self.es.get(f"/_cat/indices/{pat}?format=json&bytes=b&time=ms&h={self.CAT_IDX_COLS}")
            s["shards"] = self.es.get(f"/_cat/shards/{pat}?format=json&bytes=b&time=ms&h={self.CAT_SHARD_COLS}")
            s["recovery"] = self.es.get("/_cat/recovery?format=json&active_only=true&h=index,shard,type,stage,source_node,target_node,bytes_percent")
            if self.tick % 12 == 0:
                s["aliases"] = self.es.get("/_cat/aliases?format=json&h=alias,index")
                # alias fan-out must count every index behind the alias, not only those matching --index-prefix
                s["all_pri"] = (self.es.get("/_cat/indices?format=json&h=index,pri") if self.pfx else s["indices"])
            else:
                s["aliases"] = s["all_pri"] = None
            self.err = None
        except Exception as e:  # noqa
            self.err = f"{type(e).__name__}: {e}"
            return
        if s["aliases"] is not None:
            pri_by_idx = {r["index"]: int(num(r.get("pri"))) for r in s["all_pri"]}
            al = collections.Counter()
            for r in s["aliases"]:
                al[r["alias"]] += pri_by_idx.get(r["index"], 0)
            self.alias_shards = dict(al)
        if self.tick % 60 == 0:
            if self.tick:
                try:
                    self._static()
                except Exception:  # noqa
                    pass
            top = [r["index"] for r in sorted(s["indices"], key=lambda r: -num(r.get("search.query_total")))[:12]]
            self._static_indices(top)
        self.prev, self.cur = self.cur, s
        self._compute()
        self._diagnose()
        if self.tick % 60 == 0:
            self._checks()
        if not self.no_hot and self.tick % self.hot_every == 1 % self.hot_every:
            self.sample_hot()
        self._alerts()
        self.tick += 1

    # ---------- "what is running right now / why is it broken" (tasks, allocation explain, pending queue)
    def _diagnose(self):
        c = self.cluster
        if not self.no_tasks:
            try:
                t = self.es.get(f"/_tasks?actions={self.TASK_ACTIONS}&detailed=true&group_by=none", timeout=10)
                self._parse_tasks(t.get("tasks", []))
                self.task_err = None
            except Exception as e:  # noqa
                self.tasks, self.bg_tasks = [], []
                self.task_err = f"{type(e).__name__}: {e}"
        if c.get("unassigned") and self.tick % 6 == 0:
            try:
                self.explain = self.es.get("/_cluster/allocation/explain", timeout=10)
            except Exception:  # noqa
                self.explain = None
        elif not c.get("unassigned"):
            self.explain = None
        if c.get("pending") and self.tick % 3 == 0:
            try:
                pt = self.es.get("/_cluster/pending_tasks", timeout=10).get("tasks", [])
                self.pending_top = [(x.get("priority", "?"), x.get("time_in_queue_millis", 0), x.get("source", "")[:90]) for x in pt[:5]]
            except Exception:  # noqa
                self.pending_top = []
        elif not c.get("pending"):
            self.pending_top = []

    def _parse_tasks(self, raw):
        searches, bg = [], []
        for t in raw:
            if t.get("parent_task_id"):
                continue  # shard-level child of a coordinating task
            action = t.get("action", "")
            desc = t.get("description", "") or ""
            node = self.info.get(t.get("node", ""), {}).get("name") or t.get("node", "?")
            row = dict(node=node, action=action, run_s=num(t.get("running_time_in_nanos")) / 1e9,
                       cancellable=bool(t.get("cancellable")), id=f"{t.get('node', '?')}:{t.get('id', '?')}")
            if action == "indices:data/read/search":
                mi = re.search(r"indices\[(.*?)\]", desc)
                ms = re.search(r"search_type\[(\w+)\]", desc)
                src = desc.split("source[", 1)[1].rstrip("]") if "source[" in desc else ""
                row.update(indices=(mi.group(1) if mi else "?"), stype=(ms.group(1) if ms else ""),
                           source=(re.sub(r"\s+", " ", src) if self.show_queries else ""),
                           size=(int(m.group(1)) if (m := re.search(r'"size"\s*:\s*(\d+)', src)) else None),
                           aggs=('"aggregations"' in src or '"aggs"' in src))
                searches.append(row)
            else:
                if self.show_queries:
                    row["indices"] = desc[:80]
                else:
                    # reindex / by-query descriptions embed query bodies, scripts and remote host/username:
                    # keep only bracket groups that look like plain index names (no '=', '{', spaces)
                    names = [g for g in re.findall(r"\[([^\]]{1,80})\]", desc) if re.fullmatch(r"[\w.,*:+\-]+", g) and g != "_doc"]
                    row["indices"] = ",".join(names[:3])[:80]
                bg.append(row)
        self.tasks = sorted(searches, key=lambda r: -r["run_s"])[:40]
        self.bg_tasks = sorted(bg, key=lambda r: -r["run_s"])

    def sample_hot(self):
        try:
            txt = self.es.get("/_nodes/hot_threads?threads=6&interval=500ms&type=cpu&ignore_idle_threads=true", raw=True)
        except Exception:  # noqa
            return
        self.hot_raw = txt
        total = search = io = 0
        kinds = collections.Counter()
        top = []
        node = "?"
        for line in txt.splitlines():
            if line.startswith("::: {"):
                node = line[5:].split("}")[0]
                continue
            m = re.match(r"\s+([\d.]+)% \[cpu=([\d.]+)%, (?:idle=[\d.]+%, )?other=([\d.]+)%\] .*? cpu usage by thread '([^']+)'", line)
            if not m:
                m2 = re.match(r"\s+([\d.]+)% \(([\d.]+)(micros|ms|s) out of ([\d.]+)(ms|s)\) cpu usage by thread '([^']+)'", line)
                if not m2:
                    continue
                pct, tname = num(m2.group(1)), m2.group(6)
                cpu_pct, other_pct = pct, 0.0
            else:
                pct, cpu_pct, other_pct, tname = num(m.group(1)), num(m.group(2)), num(m.group(3)), m.group(4)
            total += 1
            kind = "other"
            tail = tname.split("]", 1)[1] if tname.startswith("elasticsearch[") and "]" in tname else tname
            tl = tail.lower()
            if "merge thread" in tl:
                kind = "merge"
            elif tl.startswith("[gc") or "g1 " in tl or "gc thread" in tl or "concurrent mark" in tl:
                kind = "GC"
            else:
                for k in ("search_coordination", "search", "write", "refresh", "flush", "management", "transport", "generic", "snapshot", "warmer", "get", "force_merge"):
                    if f"[{k}]" in tl:
                        kind = k
                        break
            kinds[kind] += 1
            if kind == "search":
                search += 1
                if other_pct >= 50 or (cpu_pct and cpu_pct < 30 and other_pct > cpu_pct):
                    io += 1
            top.append((cpu_pct if m else pct, node.split("}")[0][:28], kind, tname[:40]))
        self.hot = dict(ts=time.time(), total=total, search=search, io_bound=(100.0 * io / search) if search else 0.0,
                        kinds=kinds, top=sorted(top, reverse=True)[:8])

    # ---------- compute
    def _compute(self):
        c, p = self.cur, self.prev
        dt = (c["t"] - p["t"]) if p else None
        h = c["health"]
        cl = dict(name=h.get("cluster_name"), status=h.get("status"), nodes=h.get("number_of_nodes"),
                  data_nodes=h.get("number_of_data_nodes"), shards=h.get("active_shards"), pri=h.get("active_primary_shards"),
                  unassigned=h.get("unassigned_shards"), relocating=h.get("relocating_shards"), initializing=h.get("initializing_shards"),
                  pending=h.get("number_of_pending_tasks"), dt=dt, recoveries=len(c.get("recovery") or []))
        rows, others = [], []
        tot = collections.Counter()
        shards_per_node = collections.Counter(r.get("node") for r in c["shards"])
        for nid, n in c["nodes"].items():
            inf = self.info.get(nid, dict(name=n.get("name", nid), roles=set(), is_data=True, cores=1, ram=0, heap_max=0))
            pn = p["nodes"].get(nid) if p else None
            ind = n.get("indices", {})
            srch = ind.get("search", {})
            tp = n.get("thread_pool", {})
            tps = tp.get("search", {})
            jvm = n.get("jvm", {})
            osd = n.get("os", {})
            fs = n.get("fs", {}).get("total", {})
            io = n.get("fs", {}).get("io_stats", {}).get("total", {})
            gc = jvm.get("gc", {}).get("collectors", {})
            cores = inf["cores"]
            ram = inf["ram"] or num(dget(osd, "mem", "total_in_bytes"))
            heap_max = inf["heap_max"] or num(dget(jvm, "mem", "heap_max_in_bytes"))
            store = num(dget(ind, "store", "size_in_bytes"))
            qc, rc = ind.get("query_cache", {}), ind.get("request_cache", {})
            row = dict(
                id=nid, name=inf["name"], short=short_node(inf["name"]),
                roles="".join(sorted({r[0] for r in inf["roles"]})) or "?", is_data=inf["is_data"],
                cpu=dget(osd, "cpu", "percent"), load=num(dget(osd, "cpu", "load_average", "1m")) / cores, cores=cores,
                heap=dget(jvm, "mem", "heap_used_percent"), heap_max_gb=heap_max / 2**30,
                disk=100.0 * (1 - num(fs.get("available_in_bytes")) / max(num(fs.get("total_in_bytes")), 1)),
                store_gb=store / 2**30,
                cache_cov=(100.0 * (ram - heap_max) / store) if (store and ram) else 0.0,
                mem_free_gb=num(dget(osd, "mem", "free_in_bytes")) / 2**30,
                swap_gb=num(dget(osd, "swap", "used_in_bytes")) / 2**30,
                active=tps.get("active", 0), queue=tps.get("queue", 0), max=max(tps.get("largest", 0), tps.get("active", 0)),
                write_q=tp.get("write", {}).get("queue", 0), get_q=tp.get("get", {}).get("queue", 0),
                merges=dget(ind, "merges", "current"), ctx=srch.get("open_contexts", 0), scroll=srch.get("scroll_current", 0),
                breaker=sum(b.get("tripped", 0) for b in n.get("breaker", {}).values()),
                fielddata_mb=num(dget(ind, "fielddata", "memory_size_in_bytes")) / 2**20,
                segmem_mb=num(dget(ind, "segments", "memory_in_bytes")) / 2**20,
                qc_hit=(100.0 * qc.get("hit_count", 0) / max(qc.get("hit_count", 0) + qc.get("miss_count", 0), 1)),
                rc_hit=(100.0 * rc.get("hit_count", 0) / max(rc.get("hit_count", 0) + rc.get("miss_count", 0), 1)),
                # indices.shard_stats is cluster-wide truth; _cat/shards is narrowed by --index-prefix
                shards=int(dget(ind, "shard_stats", "total_count") or shards_per_node.get(inf["name"], 0)),
                write_rej_rate=0.0, write_active=tp.get("write", {}).get("active", 0),
                http_open=dget(n, "http", "current_open"),
                qps=0.0, qms=0.0, fps=0.0, fms=0.0, rej_rate=0.0, ips=0.0, oldgc=0.0, young_ms=0.0, fetch_share=0.0,
                read_mbps=0.0, write_mbps=0.0, iops=0.0, qc_evict=0.0, rc_evict=0.0, merge_mbps=0.0,
            )
            if pn and dt:
                dq = max(dget(ind, "search", "query_total") - dget(pn, "indices", "search", "query_total"), 0)
                dqt = max(dget(ind, "search", "query_time_in_millis") - dget(pn, "indices", "search", "query_time_in_millis"), 0)
                df = max(dget(ind, "search", "fetch_total") - dget(pn, "indices", "search", "fetch_total"), 0)
                dft = max(dget(ind, "search", "fetch_time_in_millis") - dget(pn, "indices", "search", "fetch_time_in_millis"), 0)
                drej = max(tps.get("rejected", 0) - dget(pn, "thread_pool", "search", "rejected"), 0)
                didx = max(dget(ind, "indexing", "index_total") - dget(pn, "indices", "indexing", "index_total"), 0)
                pgc = pn.get("jvm", {}).get("gc", {}).get("collectors", {})
                dold = max(dget(gc, "old", "collection_count") - dget(pgc, "old", "collection_count"), 0)
                dyoung_ms = max(dget(gc, "young", "collection_time_in_millis") - dget(pgc, "young", "collection_time_in_millis"), 0)
                pio = pn.get("fs", {}).get("io_stats", {}).get("total", {})
                drd = max(num(io.get("read_kilobytes")) - num(pio.get("read_kilobytes")), 0)
                dwr = max(num(io.get("write_kilobytes")) - num(pio.get("write_kilobytes")), 0)
                dops = max(num(io.get("read_operations")) + num(io.get("write_operations")) - num(pio.get("read_operations")) - num(pio.get("write_operations")), 0)
                dmerge = max(dget(ind, "merges", "total_size_in_bytes") - dget(pn, "indices", "merges", "total_size_in_bytes"), 0)
                dwrej = max(tp.get("write", {}).get("rejected", 0) - dget(pn, "thread_pool", "write", "rejected"), 0)
                row.update(write_rej_rate=dwrej / dt)
                row.update(qps=dq / dt, qms=(dqt / dq) if dq else 0.0, fps=df / dt, fms=(dft / df) if df else 0.0,
                           rej_rate=drej / dt, ips=didx / dt, oldgc=dold / dt, young_ms=100.0 * dyoung_ms / (dt * 1000),
                           fetch_share=(100.0 * dft / (dqt + dft)) if (dqt + dft) else 0.0,
                           read_mbps=drd / 1024 / dt, write_mbps=dwr / 1024 / dt, iops=dops / dt, merge_mbps=dmerge / 2**20 / dt,
                           qc_evict=max(qc.get("evictions", 0) - dget(pn, "indices", "query_cache", "evictions"), 0) / dt,
                           rc_evict=max(rc.get("evictions", 0) - dget(pn, "indices", "request_cache", "evictions"), 0) / dt)
                if row["is_data"]:
                    tot.update(q=dq, qt=dqt, f=df, ft=dft, rej=drej, idx=didx, rd=drd, wr=dwr)
            (rows if row["is_data"] else others).append(row)
        self.rows_nodes, self.rows_other = rows, others
        if dt:  # zero defaults so alerts/overview never KeyError when no node is classified as data
            cl.update(qps=0.0, qms=0.0, fps=0.0, fms=0.0, rej_rate=0.0, ips=0.0, fetch_share=0.0, queue=0, active=0, pool=0,
                      cpu=0.0, heap_max=0.0, load=0.0, cache_cov=0.0, read_mbps=0.0, write_mbps=0.0, oldgc=0.0, breakers=0,
                      ctx=0, scroll=0, qc_hit=0.0, rc_hit=0.0, store_tb=0.0, write_rej_rate=0.0)
        if dt and rows:
            cl.update(
                write_rej_rate=sum(r["write_rej_rate"] for r in rows),
                qps=tot["q"] / dt, qms=(tot["qt"] / tot["q"]) if tot["q"] else 0.0, fps=tot["f"] / dt, fms=(tot["ft"] / tot["f"]) if tot["f"] else 0.0,
                rej_rate=tot["rej"] / dt, ips=tot["idx"] / dt,
                fetch_share=(100.0 * tot["ft"] / (tot["qt"] + tot["ft"])) if (tot["qt"] + tot["ft"]) else 0.0,
                queue=sum(r["queue"] for r in rows), active=sum(r["active"] for r in rows), pool=sum(r["max"] for r in rows),
                cpu=sum(r["cpu"] for r in rows) / len(rows), heap_max=max(r["heap"] for r in rows), load=max(r["load"] for r in rows),
                cache_cov=min(r["cache_cov"] for r in rows) if all(r["cache_cov"] for r in rows) else 0.0,
                read_mbps=tot["rd"] / 1024 / dt, write_mbps=tot["wr"] / 1024 / dt,
                oldgc=sum(r["oldgc"] for r in rows), breakers=sum(r["breaker"] for r in rows),
                ctx=sum(r["ctx"] for r in rows), scroll=sum(r["scroll"] for r in rows),
                qc_hit=sum(r["qc_hit"] for r in rows) / len(rows), rc_hit=sum(r["rc_hit"] for r in rows) / len(rows),
                store_tb=sum(r["store_gb"] for r in rows) / 1024,
            )
            for k in ("qps", "qms", "fms", "rej_rate", "fetch_share", "cpu", "heap_max", "read_mbps", "queue", "ips"):
                self.hist[k].append(cl[k])
            self.hist["busy"].append(100.0 * cl["active"] / max(cl["pool"], 1))
        self.cluster = cl

        # ---- indices
        prev_idx = {r["index"]: r for r in p["indices"]} if p else {}
        irows = []
        for r in c["indices"]:
            pr = prev_idx.get(r["index"])
            docs, dele = num(r.get("docs.count")), num(r.get("docs.deleted"))
            pri, rep = int(num(r.get("pri"), 1)), int(num(r.get("rep")))
            segs = num(r.get("segments.count"))
            qt_l, ft_l, qn_l = dur_ms(r.get("search.query_time")), dur_ms(r.get("search.fetch_time")), num(r.get("search.query_total"))
            st = self.idx_settings.get(r["index"], {})
            mp = self.idx_mapping.get(r["index"], {})
            row = dict(index=r["index"], health=r.get("health"), pri=pri, rep=rep, docs=docs,
                       del_pct=(100.0 * dele / (docs + dele)) if (docs + dele) else 0.0,
                       shard_gb=gb(r.get("pri.store.size")) / max(pri, 1), store_gb=gb(r.get("store.size")),
                       seg_per_shard=segs / max(pri * (1 + rep), 1), merges=int(num(r.get("merges.current"))),
                       scroll=int(num(r.get("search.scroll_current"))), qtotal=qn_l,
                       qms_life=(qt_l / qn_l) if qn_l else 0.0, fshare_life=(100.0 * ft_l / (qt_l + ft_l)) if (qt_l + ft_l) else 0.0,
                       refresh=st.get("index.refresh_interval", "1s"), mrw=int(num(st.get("index.max_result_window", 10000), 10000)),
                       codec=st.get("index.codec", "default"), sort=bool(st.get("index.sort.field")),
                       routing_req=st.get("index.routing_partition_size"),
                       slowlog=st.get("index.search.slowlog.threshold.query.warn", "-1"),
                       fields=mp.get("fields", 0), text=mp.get("text", 0), nested=mp.get("nested", 0),
                       qps=0.0, qms=0.0, fms=0.0, fetch_share=0.0, ips=0.0, skew=0.0, hot_shard="")
            if pr and dt:
                dq = max(qn_l - num(pr.get("search.query_total")), 0)
                dqt = max(qt_l - dur_ms(pr.get("search.query_time")), 0)
                df = max(num(r.get("search.fetch_total")) - num(pr.get("search.fetch_total")), 0)
                dft = max(ft_l - dur_ms(pr.get("search.fetch_time")), 0)
                di = max(num(r.get("indexing.index_total")) - num(pr.get("indexing.index_total")), 0)
                row.update(qps=dq / dt, qms=(dqt / dq) if dq else 0.0, fms=(dft / df) if df else 0.0,
                           fetch_share=(100.0 * dft / (dqt + dft)) if (dqt + dft) else 0.0, ips=di / dt)
            irows.append(row)
        self.rows_idx = irows

        # ---- shards (hot shard detection)
        srows = []
        if p and dt:
            prev_sh = {(r["index"], r["shard"], r["prirep"], r.get("node")): r for r in p["shards"]}
            per_index = collections.defaultdict(list)
            for r in c["shards"]:
                k = (r["index"], r["shard"], r["prirep"], r.get("node"))
                pr = prev_sh.get(k)
                if not pr or r.get("state") != "STARTED":
                    continue
                dq = max(num(r.get("search.query_total")) - num(pr.get("search.query_total")), 0)
                dqt = max(dur_ms(r.get("search.query_time")) - dur_ms(pr.get("search.query_time")), 0)
                dft = max(dur_ms(r.get("search.fetch_time")) - dur_ms(pr.get("search.fetch_time")), 0)
                row = dict(index=r["index"], shard=int(num(r["shard"])), prirep=r["prirep"], node=(r.get("node") or "?"),
                           qps=dq / dt, qtime=dqt / 1000.0 / dt, ftime=dft / 1000.0 / dt, qms=(dqt / dq) if dq else 0.0,
                           docs=num(r.get("docs")), gb=gb(r.get("store")), segs=int(num(r.get("segments.count"))),
                           merges=int(num(r.get("merges.current"))))
                srows.append(row)
                per_index[r["index"]].append(row)
            by_idx = {r["index"]: r for r in self.rows_idx}
            for i, lst in per_index.items():
                times = [x["qtime"] for x in lst]
                mean = sum(times) / len(times) if times else 0
                if mean > 0:
                    hot = max(lst, key=lambda x: x["qtime"])
                    if i in by_idx:
                        by_idx[i]["skew"] = hot["qtime"] / mean
                        by_idx[i]["hot_shard"] = f"{hot['shard']}{hot['prirep']}@{short_node(hot['node'], 14)}"
        self.rows_shards = sorted(srows, key=lambda x: -x["qtime"])

    # ---------- checks (config audit, static)
    def _checks(self):
        ck = []
        cs = self.cluster_settings

        def add(sev, area, name, value, hint):
            ck.append((sev, area, name, value, hint))

        datas = [v for v in self.info.values() if v["is_data"]]
        coord = [v for v in self.info.values() if not v["is_data"] and not ({"master"} & v["roles"])]
        # JVM
        bad_oops = [v["name"] for v in datas if str(v["coops"]).lower() == "false"]
        add(CRIT if bad_oops else OK, "JVM", "compressed oops", f"{len(datas) - len(bad_oops)}/{len(datas)} nodes", "heap must be < 32 GB (use ≤ 31g); " + (", ".join(bad_oops)[:60] if bad_oops else "ok"))
        big = [v["name"] for v in datas if v["heap_max"] > 31.5 * 2**30]
        add(CRIT if big else OK, "JVM", "heap ≤ 31 GB", f"{len(big)} over", "-Xmx32g disables compressed oops" if big else "ok")
        over_half = [v["name"] for v in datas if v["ram"] and v["heap_max"] > 0.55 * v["ram"]]
        add(WARN if over_half else OK, "JVM", "heap ≤ 50% RAM", f"{len(over_half)} over", "leave RAM for page cache; search-heavy clusters often want 25–40%" if over_half else "ok")
        add(OK if coord else WARN, "Topology", "coordinating nodes", str(len(coord)), "dedicated coord nodes keep result-merging off data nodes" if not coord else "ok")
        # recovery
        ncr = int(num(cs.get("cluster.routing.allocation.node_concurrent_recoveries", 2), 2))
        rmb = cs.get("indices.recovery.max_bytes_per_sec", "40mb")
        add(WARN if ncr > 2 else OK, "Recovery", "node_concurrent_recoveries", str(ncr), "recovery I/O competes with search; keep ≤ 2 on search-heavy clusters" if ncr > 2 else "ok")
        add(WARN if parse_bytes(rmb) > T["recovery_mbps_warn"] * 2**20 else OK, "Recovery", "recovery max_bytes_per_sec", str(rmb), "high values saturate throughput-capped disks during rebalancing")
        # search settings
        mcs = cs.get("search.max_buckets", "65536")
        add(OK, "Search", "search.max_buckets", str(mcs), "informational")
        q = cs.get("thread_pool.search.queue_size", "1000")
        add(WARN if int(num(q, 1000)) > 1000 else OK, "Search", "thread_pool.search.queue_size", str(q), "bigger queue = same throughput, longer waits" if int(num(q, 1000)) > 1000 else "ok")
        # swap
        sw = [r["short"] for r in self.rows_nodes if r["swap_gb"] > 0.5]
        add(CRIT if sw else OK, "OS", "swap in use", f"{len(sw)} nodes", "disable swap / bootstrap.memory_lock" if sw else "ok")
        # shards per node
        mx = max((r["shards"] for r in self.rows_nodes), default=0)
        add(level(mx, T["shards_per_node_warn"], T["shards_per_node_crit"]), "Shards", "max shards per data node", str(mx), "each shard costs heap/CPU on every query touching it")
        # page cache
        cov = min((r["cache_cov"] for r in self.rows_nodes if r["cache_cov"]), default=0)
        add(level(cov, T["cache_cov_warn"], T["cache_cov_crit"], invert=True) if cov else DIM, "Memory", "page-cache coverage (min node)", f"{cov:.0f}% of indexed bytes", "(RAM − heap) / on-disk index; low = disk-bound searches")
        # per-index
        for r in sorted(self.rows_idx, key=lambda r: -r["qtotal"])[:12]:
            i = r["index"][:38]
            if r["rep"] == 0:
                add(CRIT, "Index", f"{i} replicas", "0", "no redundancy and no read scaling")
            if r["mrw"] > T["mrw_warn"]:
                add(WARN, "Index", f"{i} max_result_window", f"{r['mrw']:,}", "deep from/size pagination; use search_after")
            rf = str(r["refresh"])
            if rf == "-1":
                add(WARN, "Index", f"{i} refresh_interval", rf, "refresh disabled - bulk-load setting left on?")
            elif rf.endswith("ms") or rf in ("1s",) and r["ips"] > 0:
                add(WARN, "Index", f"{i} refresh_interval", rf, "frequent refresh creates small segments under indexing; 30s+ for batch-loaded indices")
            if r["fields"] >= T["fields_warn"]:
                add(level(r["fields"], T["fields_warn"], T["fields_crit"]), "Mapping", f"{i} mapped fields", f"{r['fields']} ({r['text']} text)", "wide mappings bloat _source and fetch; keyword/index:false for codes & IDs")
            if r["fields"] and 100.0 * r["text"] / r["fields"] >= T["text_ratio_warn"]:
                add(WARN, "Mapping", f"{i} text field ratio", f"{100.0 * r['text'] / r['fields']:.0f}%", "clinical codes/IDs rarely need analyzed text")
            if r["nested"]:
                add(WARN, "Mapping", f"{i} nested fields", str(r["nested"]), "nested queries multiply per-doc cost")
            if r["codec"] == "default" and r["store_gb"] > 500:
                add(WARN, "Index", f"{i} codec", "default", "best_compression shrinks _source on disk-bound clusters")
            if str(r["slowlog"]) in ("-1", "") and r["qtotal"] > 0:
                add(WARN, "Index", f"{i} search slowlog", "off", "set query.warn ≈ 2s to catch expensive queries")
            if r["shard_gb"] >= T["shard_gb_warn"]:
                add(level(r["shard_gb"], T["shard_gb_warn"], T["shard_gb_crit"]), "Index", f"{i} avg shard size", f"{r['shard_gb']:.0f} GB", "target 20–50 GB")
        for al, sh in sorted(self.alias_shards.items(), key=lambda kv: -kv[1])[:8]:
            if sh >= T["alias_shards_warn"]:
                add(level(sh, T["alias_shards_warn"], T["alias_shards_crit"]), "Alias", f"{al} fan-out", f"{sh} primaries/query", "narrow aliases or custom _routing")
        self.checks = sorted(ck, key=lambda x: {FATAL: 0, CRIT: 1, WARN: 2, OK: 3, DIM: 4}[x[0]])

    # ---------- alerts
    def _alerts(self):
        a = []
        cl = self.cluster
        if cl.get("status") == "red":
            a.append((FATAL, f"cluster RED - {cl.get('unassigned')} unassigned shards"))
        elif cl.get("status") == "yellow":
            a.append((WARN, f"cluster YELLOW - {cl.get('unassigned')} unassigned shards"))
        if cl.get("relocating") or cl.get("initializing") or cl.get("recoveries"):
            a.append((WARN, f"{cl.get('relocating', 0)} relocating / {cl.get('initializing', 0)} initializing / {cl.get('recoveries', 0)} active recoveries - I/O competes with search"))
        if cl.get("unassigned") and self.explain:
            ex = self.explain
            why = (ex.get("allocate_explanation") or ex.get("unassigned_info", {}).get("reason") or "").strip()
            deciders = [d.get("explanation", "") for nd in ex.get("node_allocation_decisions", [])[:1] for d in nd.get("deciders", [])]
            a.append((WARN, f"unassigned {ex.get('index', '?')}[{ex.get('shard', '?')}{'p' if ex.get('primary') else 'r'}]: {why[:90]} {deciders[0][:110] if deciders else ''}".rstrip()))
        if cl.get("pending"):
            top = f" - head of queue: [{self.pending_top[0][0]}] {self.pending_top[0][2][:60]} ({self.pending_top[0][1] / 1000:.1f}s)" if self.pending_top else ""
            a.append((WARN, f"{cl['pending']} pending cluster-state tasks{top}"))
        for t in self.tasks[:5]:
            if t["run_s"] >= T["task_warn_s"]:
                what = f"{'agg ' if t['aggs'] else ''}{('size=' + str(t['size']) + ' ') if t['size'] is not None else ''}"
                a.append((CRIT if t["run_s"] >= T["task_crit_s"] else WARN,
                          f"search running {t['run_s']:.0f}s on {short_node(t['node'])} [{t['indices'][:50]}] {what}- view 7; cancel: POST _tasks/{t['id']}/_cancel"))
        for t in self.bg_tasks[:4]:
            a.append((WARN, f"{t['action'].split('/')[-1]} running {t['run_s']:.0f}s on {short_node(t['node'])} - competes with search for I/O"))
        if cl.get("write_rej_rate", 0) >= T["rej_warn"]:
            a.append((CRIT, f"write rejections {cl['write_rej_rate']:.2f}/s - bulk indexers are getting 429"))
        if cl.get("dt"):
            if cl["rej_rate"] >= T["rej_crit"]:
                a.append((FATAL, f"search rejections {cl['rej_rate']:.1f}/s - clients get partial results / 429"))
            elif cl["rej_rate"] >= T["rej_warn"]:
                a.append((CRIT, f"search rejections {cl['rej_rate']:.2f}/s"))
            busy = 100.0 * cl["active"] / max(cl["pool"], 1)
            if busy >= 90:
                a.append((CRIT, f"search pools {busy:.0f}% busy ({cl['active']}/{cl['pool']}) - queueing"))
            elif busy >= 70:
                a.append((WARN, f"search pools {busy:.0f}% busy"))
            if cl["fetch_share"] >= T["fetch_share_crit"] and cl["qps"] > 1:
                a.append((CRIT, f"fetch phase {cl['fetch_share']:.0f}% of search time - cap size, filter _source, search_after"))
            elif cl["fetch_share"] >= T["fetch_share_warn"] and cl["qps"] > 1:
                a.append((WARN, f"fetch phase {cl['fetch_share']:.0f}% of search time"))
            if cl["qms"] >= T["qms_crit"] and cl["qps"] > 1:
                a.append((CRIT, f"avg shard query {cl['qms']:.0f} ms cluster-wide"))
            if cl.get("cache_cov") and cl["cache_cov"] <= T["cache_cov_crit"]:
                a.append((CRIT, f"page cache covers {cl['cache_cov']:.0f}% of index - searches are disk-bound; more RAM or smaller heap"))
            elif cl.get("cache_cov") and cl["cache_cov"] <= T["cache_cov_warn"]:
                a.append((WARN, f"page cache covers {cl['cache_cov']:.0f}% of index"))
            if cl.get("breakers"):
                a.append((CRIT, f"circuit breakers tripped {cl['breakers']}x"))
        if self.hot["search"] >= 3 and self.hot["io_bound"] >= T["io_bound_warn"]:
            a.append((CRIT if self.hot["io_bound"] >= T["io_bound_crit"] else WARN, f"{self.hot['io_bound']:.0f}% of hot search threads are off-CPU (I/O wait) - page-cache misses"))
        if self.hot["kinds"].get("merge", 0) >= 3:
            a.append((WARN, f"{self.hot['kinds']['merge']} hot merge threads competing with search"))
        for r in self.rows_nodes:
            n = r["short"]
            if r["rej_rate"] >= T["rej_warn"]:
                a.append((CRIT if r["rej_rate"] < T["rej_crit"] else FATAL, f"{n}: search rejected {r['rej_rate']:.2f}/s, queue {r['queue']}"))
            elif r["queue"] >= T["queue_warn"]:
                a.append((WARN if r["queue"] < T["queue_crit"] else CRIT, f"{n}: search queue {r['queue']} ({r['active']}/{r['max']} active)"))
            if r["heap"] >= T["heap_warn"]:
                a.append((WARN if r["heap"] < T["heap_crit"] else CRIT, f"{n}: heap {r['heap']}%"))
            if r["oldgc"] >= T["oldgc_warn"]:
                a.append((WARN if r["oldgc"] < T["oldgc_crit"] else CRIT, f"{n}: old-gen GC {r['oldgc']:.2f}/s"))
            if r["young_ms"] >= 10:
                a.append((WARN, f"{n}: {r['young_ms']:.0f}% of wall time in young GC"))
            if r["disk"] >= T["disk_warn"]:
                a.append((WARN if r["disk"] < T["disk_crit"] else CRIT, f"{n}: disk {r['disk']:.0f}% used"))
            if r["load"] >= T["load_warn"]:
                a.append((WARN if r["load"] < T["load_crit"] else CRIT, f"{n}: load/core {r['load']:.2f}"))
            if r["read_mbps"] >= T["read_mbps_warn"]:
                a.append((WARN if r["read_mbps"] < T["read_mbps_crit"] else CRIT, f"{n}: disk read {r['read_mbps']:.0f} MB/s - near throughput cap?"))
            if r["breaker"]:
                a.append((CRIT, f"{n}: circuit breaker tripped {r['breaker']}x"))
            if r["ctx"] >= T["ctx_warn"]:
                a.append((WARN if r["ctx"] < T["ctx_crit"] else CRIT, f"{n}: {r['ctx']} open search contexts ({r['scroll']} scrolls)"))
            if r["swap_gb"] > 0.5:
                a.append((CRIT, f"{n}: {r['swap_gb']:.1f} GB swap in use"))
            if r["fielddata_mb"] >= T["fielddata_warn_mb"]:
                a.append((WARN, f"{n}: fielddata {r['fielddata_mb']:.0f} MB on heap - text field sorted/aggregated? use keyword or doc_values"))
            if r["write_q"] >= T["queue_warn"]:
                a.append((WARN, f"{n}: write queue {r['write_q']} - indexing backlog competes with search"))
            if r["is_data"] and r["qps"] > 5 and r["qc_hit"] < T["qcache_hit_warn"] and r["qc_evict"] > 1:
                a.append((WARN, f"{n}: query cache hit {r['qc_hit']:.0f}%, evicting {r['qc_evict']:.0f}/s - filters not reused / put predicates in bool.filter"))
        for r in self.rows_other:
            if r["cpu"] >= T["cpu_crit"]:
                a.append((WARN, f"{r['short']} ({r['roles']}): cpu {r['cpu']}% - coordinator merging results"))
            if r["heap"] >= T["heap_crit"]:
                a.append((CRIT, f"{r['short']} ({r['roles']}): heap {r['heap']}%"))
        hot_idx = sorted(self.rows_idx, key=lambda x: -x["qps"])[:15]
        for r in hot_idx:
            i = r["index"]
            if r["rep"] == 0 and r["qtotal"] > 0:
                a.append((CRIT, f"{i}: replicas=0"))
            if r["del_pct"] >= T["del_warn"]:
                a.append((WARN if r["del_pct"] < T["del_crit"] else CRIT, f"{i}: {r['del_pct']:.0f}% deleted docs - forcemerge only_expunge_deletes off-peak"))
            if r["shard_gb"] >= T["shard_gb_warn"]:
                a.append((WARN if r["shard_gb"] < T["shard_gb_crit"] else CRIT, f"{i}: {r['shard_gb']:.0f} GB/shard"))
            if r["seg_per_shard"] >= T["seg_warn"]:
                a.append((WARN if r["seg_per_shard"] < T["seg_crit"] else CRIT, f"{i}: {r['seg_per_shard']:.0f} segments/shard"))
            if r["qps"] > 0 and r["qms"] >= T["qms_warn"]:
                a.append((WARN if r["qms"] < T["qms_crit"] else CRIT, f"{i}: avg {r['qms']:.0f} ms/shard query"))
            if r["qps"] > 0 and r["fetch_share"] >= T["fetch_share_crit"]:
                a.append((WARN, f"{i}: fetch {r['fetch_share']:.0f}% of its search time"))
            if r["skew"] >= T["skew_warn"] and r["qps"] > 1:
                a.append((WARN if r["skew"] < T["skew_crit"] else CRIT, f"{i}: hot shard {r['hot_shard']} at {r['skew']:.1f}× mean - routing hotspot or uneven data"))
            if r["scroll"] >= 50:
                a.append((WARN, f"{i}: {r['scroll']} open scroll contexts"))
        for al, sh in sorted(self.alias_shards.items(), key=lambda kv: -kv[1]):
            if sh >= T["alias_shards_warn"]:
                a.append((WARN if sh < T["alias_shards_crit"] else CRIT, f"alias {al} -> {sh} primary shards per query"))
        self.alerts = a
        pen = sum({FATAL: 25, CRIT: 10, WARN: 3}.get(lv, 0) for lv, _ in a)
        self.score = max(0, 100 - pen)
        self.hist["score"].append(self.score)


# ───────────────────────── UI helpers ─────────────────────────
def level(v, warn, crit, invert=False):
    if invert:
        return OK if v > warn else (WARN if v > crit else CRIT)
    return CRIT if v >= crit else (WARN if v >= warn else OK)


def status_attr(s):
    return {"green": OK, "yellow": WARN, "red": FATAL}.get(s, DIM)


def spark(vals, width=24, vmax=None):
    vals = list(vals)[-width:]
    if not vals:
        return " " * width
    hi = vmax if vmax is not None else max(vals) or 1.0
    out = "".join(SPARK[min(len(SPARK) - 1, int(v / hi * (len(SPARK) - 1) + 0.5))] if hi else SPARK[0] for v in vals)
    return out.rjust(width)


def gauge(pct, width=20):
    pct = max(0.0, min(100.0, pct))
    full = int(width * pct / 100)
    part = (width * pct / 100) - full
    blocks = "█" * full + ("▌" if part >= 0.5 and full < width else "") 
    return blocks.ljust(width, "·")


# ───────────────────────── ANSI fallback ─────────────────────────
class _Pair:
    A_BOLD = 1 << 24
    KEY_RESIZE = -2
    COLOR_GREEN = COLOR_YELLOW = COLOR_RED = COLOR_MAGENTA = COLOR_WHITE = COLOR_BLACK = COLOR_CYAN = COLOR_BLUE = 0
    error = Exception

    def color_pair(self, n): return n
    def init_pair(self, *a): pass
    def start_color(self): pass
    def use_default_colors(self): pass
    def curs_set(self, n): pass


class AnsiScreen:
    FG = {OK: "32", WARN: "33", CRIT: "31", FATAL: "35", DIM: "0", HEAD: "30;46", TITLE: "36", ACC: "34"}

    def __init__(self):
        self.lines = {}
        self._tty = sys.stdin.isatty()
        if self._tty:
            import termios, tty  # noqa
            self._termios = termios
            self._old = termios.tcgetattr(sys.stdin)
            tty.setcbreak(sys.stdin.fileno())
        sys.stdout.write("\x1b[?25l\x1b[2J")

    def restore(self):
        sys.stdout.write("\x1b[?25h\x1b[0m\n"); sys.stdout.flush()
        if self._tty:
            self._termios.tcsetattr(sys.stdin, self._termios.TCSADRAIN, self._old)

    def getmaxyx(self):
        c = shutil.get_terminal_size((170, 48)); return c.lines, c.columns

    def nodelay(self, flag): pass
    def erase(self): self.lines = {}

    def addstr(self, y, x, text, attr=0):
        pair, bold = attr & 0xFFFF, bool(attr & _Pair.A_BOLD)
        code = self.FG.get(pair, "0") + (";1" if bold else "")
        text = clean(text)
        w = self.getmaxyx()[1]
        row = self.lines.setdefault(y, [(" ", "0")] * w)
        for i, ch in enumerate(text):
            if 0 <= x + i < w - 1:
                row[x + i] = (ch, code)

    def refresh(self):
        h, w = self.getmaxyx(); out = ["\x1b[H"]
        for y in range(h - 1):
            row = self.lines.get(y)
            if row is None:
                out.append("\x1b[K\n"); continue
            buf, cur = [], None
            for ch, code in row[: w - 1]:
                if code != cur:
                    buf.append(f"\x1b[0;{code}m"); cur = code
                buf.append(ch)
            out.append("".join(buf) + "\x1b[0m\x1b[K\n")
        sys.stdout.write("".join(out)); sys.stdout.flush()

    def getch(self):
        if not self._tty:
            return -1
        r, _, _ = select.select([sys.stdin], [], [], 0)
        if r:
            b = os.read(sys.stdin.fileno(), 1)  # raw read: sys.stdin.read() is buffered and starves select()
            return b[0] if b else -1
        return -1


def ansi_wrapper(fn):
    scr = AnsiScreen()
    try:
        fn(scr)
    finally:
        scr.restore()


# ───────────────────────── UI ─────────────────────────
class UI:
    SORT_NODES = [("qps", "q/s"), ("qms", "q ms"), ("fms", "f ms"), ("rej_rate", "rej/s"), ("queue", "queue"), ("cpu", "cpu"), ("heap", "heap"), ("load", "load"), ("read_mbps", "rd MB/s"), ("cache_cov", "cache"), ("name", "name")]
    SORT_IDX = [("qps", "q/s"), ("qms", "q ms"), ("fms", "f ms"), ("fetch_share", "fetch%"), ("del_pct", "del%"), ("shard_gb", "GB/shd"), ("seg_per_shard", "seg/shd"), ("skew", "skew"), ("fields", "fields"), ("index", "name")]
    SORT_SH = [("qtime", "q time"), ("qps", "q/s"), ("qms", "q ms"), ("gb", "GB"), ("segs", "segs")]
    VIEWS = [(1, "overview"), (2, "nodes"), (3, "indices"), (4, "hot shards"), (5, "alerts"), (6, "config audit"), (7, "live tasks")]

    def __init__(self, stdscr, model, interval, top_n):
        self.s, self.m, self.interval, self.top_n = stdscr, model, interval, top_n
        self.view, self.sort_n, self.sort_i, self.sort_s, self.paused = 1, 0, 0, 0, False
        self.worker, self.dump_msg = None, ""
        self.c = _Pair() if isinstance(stdscr, AnsiScreen) else curses
        self.c.curs_set(0); self.c.start_color(); self.c.use_default_colors()
        self.c.init_pair(OK, self.c.COLOR_GREEN, -1)
        self.c.init_pair(WARN, self.c.COLOR_YELLOW, -1)
        self.c.init_pair(CRIT, self.c.COLOR_RED, -1)
        self.c.init_pair(FATAL, self.c.COLOR_MAGENTA, -1)
        self.c.init_pair(DIM, self.c.COLOR_WHITE, -1)
        self.c.init_pair(HEAD, self.c.COLOR_BLACK, self.c.COLOR_CYAN)
        self.c.init_pair(TITLE, self.c.COLOR_CYAN, -1)
        self.c.init_pair(ACC, self.c.COLOR_BLUE, -1)
        self.s.nodelay(True)

    # ---- primitives
    def put(self, y, x, text, pair=DIM, bold=False):
        h, w = self.s.getmaxyx()
        if y < 0 or y >= h or x >= w:
            return
        text = clean(text)[: max(w - x - 1, 0)]
        try:
            self.s.addstr(y, x, text, self.c.color_pair(pair) | (self.c.A_BOLD if bold else 0))
        except Exception:
            pass

    def cell(self, y, x, text, pair):
        self.put(y, x, text, pair, bold=(pair in (CRIT, FATAL)))

    def hline(self, y, text=""):
        h, w = self.s.getmaxyx()
        self.put(y, 0, ("─" * 2 + (f" {text} " if text else "") + "─" * w)[: w - 1], TITLE)

    def width(self):
        return self.s.getmaxyx()[1]

    # ---- header (all views)
    def draw_header(self):
        m, c = self.m, self.m.cluster
        h, w = self.s.getmaxyx()
        self.put(0, 0, " " * (w - 1), HEAD)
        self.put(0, 0, f" ▶ estop  {c.get('name', '?')}  ·  ES {m.version}  ·  {time.strftime('%H:%M:%S')}  ·  {self.interval}s{'  ⏸ PAUSED' if self.paused else ''}", HEAD, bold=True)
        sc = m.score
        self.put(0, max(w - 36, 60), f" search health {sc:3d} {gauge(sc, 12)} ", HEAD, bold=True)
        x = 0
        self.put(1, x, "cluster ", DIM); x += 8
        self.cell(1, x, (c.get("status") or "?").upper(), status_attr(c.get("status"))); x += 8
        seg = f"nodes {c.get('nodes')} (data {c.get('data_nodes')})  shards {c.get('shards')}/{c.get('pri')} pri  "
        self.put(1, x, seg, DIM); x += len(seg)
        self.cell(1, x, f"unassigned {c.get('unassigned', 0)}", OK if not c.get("unassigned") else CRIT); x += 16
        self.cell(1, x, f"reloc {c.get('relocating', 0)}  init {c.get('initializing', 0)}  recov {c.get('recoveries', 0)}  pending {c.get('pending', 0)}", WARN if (c.get("relocating") or c.get("initializing") or c.get("recoveries") or c.get("pending")) else DIM)
        if c.get("store_tb") is not None:
            self.put(1, x + 46, f"data {c['store_tb']:.1f} TB", DIM)
        x = 0
        for v, label in self.VIEWS:
            txt = f" {v} {label} "
            self.put(2, x, txt, HEAD if v == self.view else TITLE, bold=(v == self.view)); x += len(txt) + 1
        nc = sum(1 for lv, _ in m.alerts if lv in (CRIT, FATAL)); nw = sum(1 for lv, _ in m.alerts if lv == WARN)
        self.cell(2, x + 1, f"⚠ {nc} critical", CRIT if nc else OK); self.cell(2, x + 15, f"{nw} warning", WARN if nw else OK)
        sort_lbl = {2: self.SORT_NODES[self.sort_n][1], 3: self.SORT_IDX[self.sort_i][1], 4: self.SORT_SH[self.sort_s][1]}.get(self.view, "severity")
        self.put(2, x + 28, f"sort:{sort_lbl}  [s]ort [p]ause [h]ot-threads [d]ump [+/-] [q]uit", DIM)
        if m.err:
            self.cell(3, 0, f"✖ {m.err}", FATAL)
        elif self.dump_msg:
            self.cell(3, 0, self.dump_msg, OK)
        return 4

    # ---- 1 overview
    def draw_overview(self, y):
        m, c = self.m, self.m.cluster
        if not c.get("dt"):
            self.put(y, 0, "collecting first delta…", DIM); return
        H = m.hist
        self.hline(y, "SEARCH"); y += 1
        rows = [
            ("shard-queries/s", f"{c['qps']:9.1f}", spark(H["qps"]), DIM),
            ("avg query ms   ", f"{c['qms']:9.1f}", spark(H["qms"]), level(c["qms"], T["qms_warn"], T["qms_crit"])),
            ("fetch/s        ", f"{c['fps']:9.1f}", "", DIM),
            ("avg fetch ms   ", f"{c['fms']:9.1f}", spark(H["fms"]), level(c["fms"], T["fms_warn"], T["fms_crit"])),
            ("fetch share %  ", f"{c['fetch_share']:9.0f}", spark(H["fetch_share"], vmax=100), level(c["fetch_share"], T["fetch_share_warn"], T["fetch_share_crit"])),
            ("rejections/s   ", f"{c['rej_rate']:9.2f}", spark(H["rej_rate"]), level(c["rej_rate"], T["rej_warn"], T["rej_crit"])),
            ("queue depth    ", f"{c['queue']:9d}", spark(H["queue"]), level(c["queue"], T["queue_warn"], T["queue_crit"])),
        ]
        busy = 100.0 * c["active"] / max(c["pool"], 1)
        for label, val, sp, lv in rows:
            self.put(y, 1, label, DIM); self.cell(y, 18, val, lv); self.cell(y, 29, sp, lv); y += 1
        self.put(y, 1, "search pool    ", DIM); self.cell(y, 18, f"{c['active']:4d}/{c['pool']:<5d} {gauge(busy, 24)} {busy:3.0f}%", level(busy, 70, 90)); y += 1
        # right column: resources
        ry = y - 9
        xr = 60
        self.put(ry, xr, "RESOURCES", TITLE, bold=True); ry += 1
        res = [
            ("avg cpu %      ", f"{c['cpu']:6.0f}", spark(H["cpu"], 16, 100), level(c["cpu"], T["cpu_warn"], T["cpu_crit"])),
            ("max heap %     ", f"{c['heap_max']:6.0f}", spark(H["heap_max"], 16, 100), level(c["heap_max"], T["heap_warn"], T["heap_crit"])),
            ("max load/core  ", f"{c['load']:6.2f}", "", level(c["load"], T["load_warn"], T["load_crit"])),
            ("page-cache cov ", f"{c['cache_cov']:5.0f}%", "", level(c["cache_cov"], T["cache_cov_warn"], T["cache_cov_crit"], invert=True) if c["cache_cov"] else DIM),
            ("disk read MB/s ", f"{c['read_mbps']:6.0f}", spark(H["read_mbps"], 16), DIM),
            ("disk write MB/s", f"{c['write_mbps']:6.0f}", "", DIM),
            ("old GC /s      ", f"{c['oldgc']:6.2f}", "", level(c["oldgc"], T["oldgc_warn"], T["oldgc_crit"])),
            ("qcache / rcache", f"{c['qc_hit']:3.0f}%/{c['rc_hit']:3.0f}%", "", DIM),
            ("open ctx/scroll", f"{c['ctx']:4d}/{c['scroll']:<4d}", "", level(c["ctx"], T["ctx_warn"] * len(m.rows_nodes), T["ctx_crit"] * len(m.rows_nodes))),
            ("indexing docs/s", f"{c['ips']:6.0f}", spark(H["ips"], 16), DIM),
        ]
        for label, val, sp, lv in res:
            self.put(ry, xr, label, DIM); self.cell(ry, xr + 16, val, lv); self.cell(ry, xr + 26, sp, lv); ry += 1
        y = max(y, ry) + 1
        # hot threads
        hot = m.hot
        self.hline(y, f"HOT THREADS  (sampled {int(time.time() - hot['ts'])}s ago)" if hot["ts"] else "HOT THREADS  (press h)"); y += 1
        if hot["ts"]:
            kinds = ", ".join(f"{k} {v}" for k, v in hot["kinds"].most_common(6))
            self.put(y, 1, f"hot threads {hot['total']}  →  {kinds}", DIM); y += 1
            io = hot["io_bound"]
            self.put(y, 1, "search threads off-CPU (I/O wait) ", DIM)
            self.cell(y, 36, f"{gauge(io, 24)} {io:3.0f}%", level(io, T["io_bound_warn"], T["io_bound_crit"]) if hot["search"] else DIM); y += 1
            for cpu_pct, node, kind, tname in hot["top"][:4]:
                self.put(y, 1, f"{cpu_pct:5.0f}% cpu  {node:<28} {kind:<9} {tname}", WARN if kind == "merge" else DIM); y += 1
        y += 1
        # alerts
        self.hline(y, f"ALERTS  {len(m.alerts)}"); y += 1
        h = self.s.getmaxyx()[0]
        for lv, msg in sorted(m.alerts, key=lambda a: {FATAL: 0, CRIT: 1, WARN: 2}.get(a[0], 9)):
            if y >= h - 1:
                break
            self.cell(y, 1, f"{SEV[lv]} {msg}", lv); y += 1
        if not m.alerts:
            self.cell(y, 1, "✔ no issues detected", OK)

    # ---- 2 nodes
    def draw_nodes(self, y):
        hdr = f"{'node':<26}{'r':>2} {'cpu%':>4} {'ld/c':>5} {'heap%':>5} {'ogc/s':>5} {'cache%':>6} {'rdMB/s':>6} {'iops':>5} {'disk%':>5} {'shd':>4} {'act/max':>9} {'queue':>5} {'rej/s':>6} {'q/s':>8} {'q ms':>6} {'f/s':>6} {'f ms':>6} {'fch%':>4} {'qc%':>4} {'rc%':>4} {'mrg':>3} {'ctx':>5} {'wq':>4} {'wrej/s':>6}"
        self.put(y, 0, hdr.ljust(self.width() - 1), HEAD, bold=True); y += 1
        key = self.SORT_NODES[self.sort_n][0]
        rows = sorted(self.m.rows_nodes, key=(lambda r: r[key]) if key == "name" else (lambda r: -r[key]))
        h = self.s.getmaxyx()[0]
        for r in rows:
            if y >= h - 1:
                break
            x = 0
            def c_(txt, lv, wdt):
                nonlocal x
                self.cell(y, x, txt, lv); x += wdt
            c_(f"{r['short']:<26}", DIM, 26); c_(f"{r['roles']:>2}", DIM, 3)
            c_(f"{r['cpu']:4.0f}", level(r["cpu"], T["cpu_warn"], T["cpu_crit"]), 5)
            c_(f"{r['load']:5.2f}", level(r["load"], T["load_warn"], T["load_crit"]), 6)
            c_(f"{r['heap']:5.0f}", level(r["heap"], T["heap_warn"], T["heap_crit"]), 6)
            c_(f"{r['oldgc']:5.2f}", level(r["oldgc"], T["oldgc_warn"], T["oldgc_crit"]), 6)
            c_(f"{r['cache_cov']:6.0f}", level(r["cache_cov"], T["cache_cov_warn"], T["cache_cov_crit"], invert=True) if r["cache_cov"] else DIM, 7)
            c_(f"{r['read_mbps']:6.0f}", level(r["read_mbps"], T["read_mbps_warn"], T["read_mbps_crit"]), 7)
            c_(f"{r['iops']:5.0f}", DIM, 6)
            c_(f"{r['disk']:5.0f}", level(r["disk"], T["disk_warn"], T["disk_crit"]), 6)
            c_(f"{r['shards']:4d}", level(r["shards"], T["shards_per_node_warn"], T["shards_per_node_crit"]), 5)
            busy = 100.0 * r["active"] / max(r["max"], 1)
            c_(f"{r['active']:4d}/{r['max']:<4d}", level(busy, 70, 95), 10)
            c_(f"{r['queue']:5d}", level(r["queue"], T["queue_warn"], T["queue_crit"]), 6)
            c_(f"{r['rej_rate']:6.2f}", level(r["rej_rate"], T["rej_warn"], T["rej_crit"]), 7)
            c_(f"{r['qps']:8.1f}", DIM, 9)
            c_(f"{r['qms']:6.1f}", level(r["qms"], T["qms_warn"], T["qms_crit"]) if r["qps"] else DIM, 7)
            c_(f"{r['fps']:6.1f}", DIM, 7)
            c_(f"{r['fms']:6.1f}", level(r["fms"], T["fms_warn"], T["fms_crit"]) if r["fps"] else DIM, 7)
            c_(f"{r['fetch_share']:4.0f}", level(r["fetch_share"], T["fetch_share_warn"], T["fetch_share_crit"]) if r["qps"] else DIM, 5)
            c_(f"{r['qc_hit']:4.0f}", DIM, 5); c_(f"{r['rc_hit']:4.0f}", DIM, 5)
            c_(f"{r['merges']:3d}", WARN if r["merges"] else DIM, 4)
            c_(f"{r['ctx']:5d}", level(r["ctx"], T["ctx_warn"], T["ctx_crit"]), 6)
            c_(f"{r['write_q']:4d}", level(r["write_q"], T["queue_warn"], T["queue_crit"]), 5)
            c_(f"{r['write_rej_rate']:6.2f}", level(r["write_rej_rate"], T["rej_warn"], T["rej_crit"]), 7)
            y += 1
        if self.m.rows_other and y < h - 3:
            y += 1
            self.put(y, 0, "non-data nodes (m=master c=coordinating i=ingest)", TITLE, bold=True); y += 1
            for r in sorted(self.m.rows_other, key=lambda r: r["name"]):
                if y >= h - 1:
                    break
                self.cell(y, 0, f"{r['short']:<26}{r['roles'] or 'c':>2}", DIM)
                self.cell(y, 30, f"cpu {r['cpu']:3.0f}%", level(r["cpu"], T["cpu_warn"], T["cpu_crit"]))
                self.cell(y, 40, f"heap {r['heap']:3.0f}%", level(r["heap"], T["heap_warn"], T["heap_crit"]))
                self.cell(y, 51, f"old gc {r['oldgc']:.2f}/s", level(r["oldgc"], T["oldgc_warn"], T["oldgc_crit"]))
                self.cell(y, 68, f"http open {r['http_open']}", DIM)
                y += 1
        return y

    # ---- 3 indices
    def draw_indices(self, y):
        hdr = f"{'index':<38} {'pri/rep':>7} {'docs M':>7} {'del%':>5} {'GB/shd':>6} {'seg/shd':>7} {'q/s':>8} {'q ms':>6} {'f ms':>6} {'fch%':>4} {'life q':>6} {'skew':>5} {'hot shard':<16} {'refresh':>7} {'mrw':>7} {'fields':>6} {'text':>5} {'codec':<8}"
        self.put(y, 0, hdr.ljust(self.width() - 1), HEAD, bold=True); y += 1
        key = self.SORT_IDX[self.sort_i][0]
        rows = [r for r in self.m.rows_idx if r["qtotal"] > 0 or r["ips"] > 0]
        rows = sorted(rows, key=(lambda r: r[key]) if key == "index" else (lambda r: -r[key]))[: self.top_n]
        h = self.s.getmaxyx()[0]
        for r in rows:
            if y >= h - 1:
                break
            x = 0
            def c_(txt, lv, wdt):
                nonlocal x
                self.cell(y, x, txt, lv); x += wdt
            c_(f"{r['index'][:38]:<38}", status_attr(r["health"]) if r["health"] != "green" else DIM, 39)
            c_(f"{r['pri']:3d}/{r['rep']:<3d}", CRIT if r["rep"] == 0 else DIM, 8)
            c_(f"{r['docs'] / 1e6:7.1f}", DIM, 8)
            c_(f"{r['del_pct']:5.1f}", level(r["del_pct"], T["del_warn"], T["del_crit"]), 6)
            c_(f"{r['shard_gb']:6.1f}", level(r["shard_gb"], T["shard_gb_warn"], T["shard_gb_crit"]), 7)
            c_(f"{r['seg_per_shard']:7.1f}", level(r["seg_per_shard"], T["seg_warn"], T["seg_crit"]), 8)
            c_(f"{r['qps']:8.1f}", DIM, 9)
            c_(f"{r['qms']:6.1f}", level(r["qms"], T["qms_warn"], T["qms_crit"]) if r["qps"] else DIM, 7)
            c_(f"{r['fms']:6.1f}", level(r["fms"], T["fms_warn"], T["fms_crit"]) if r["qps"] else DIM, 7)
            c_(f"{r['fetch_share']:4.0f}", level(r["fetch_share"], T["fetch_share_warn"], T["fetch_share_crit"]) if r["qps"] else DIM, 5)
            c_(f"{r['qms_life']:6.1f}", level(r["qms_life"], T["qms_warn"], T["qms_crit"]), 7)
            c_(f"{r['skew']:5.1f}" if r["skew"] else "    -", level(r["skew"], T["skew_warn"], T["skew_crit"]) if r["skew"] else DIM, 6)
            c_(f"{r['hot_shard']:<16}", DIM, 17)
            c_(f"{str(r['refresh']):>7}", WARN if str(r["refresh"]) == "-1" else DIM, 8)
            c_(f"{r['mrw']:7d}", WARN if r["mrw"] > T["mrw_warn"] else DIM, 8)
            c_(f"{r['fields']:6d}" if r["fields"] else "     -", level(r["fields"], T["fields_warn"], T["fields_crit"]) if r["fields"] else DIM, 7)
            c_(f"{r['text']:5d}" if r["fields"] else "    -", DIM, 6)
            c_(f"{r['codec'][:8]:<8}", DIM, 8)
            y += 1
        if self.m.alias_shards and y < h - 2:
            y += 1
            self.put(y, 0, "alias fan-out (primary shards per query)", TITLE, bold=True); y += 1
            x = 0
            for al, sh in sorted(self.m.alias_shards.items(), key=lambda kv: -kv[1])[:14]:
                txt = f"{al}={sh}  "
                if x + len(txt) >= self.width():
                    x = 0; y += 1
                    if y >= h - 1:
                        break
                self.cell(y, x, txt, level(sh, T["alias_shards_warn"], T["alias_shards_crit"])); x += len(txt)
        return y

    # ---- 4 hot shards
    def draw_shards(self, y):
        self.put(y, 0, "hottest shards this interval - query time per second of wall clock (1.00 = one search thread fully busy on this shard)", TITLE, bold=True); y += 1
        hdr = f"{'index':<38} {'shd':>4} {'p/r':>3} {'node':<26} {'q time s/s':>10} {'q/s':>7} {'q ms':>6} {'f time s/s':>10} {'docs M':>7} {'GB':>6} {'segs':>4} {'mrg':>3}"
        self.put(y, 0, hdr.ljust(self.width() - 1), HEAD, bold=True); y += 1
        key = self.SORT_SH[self.sort_s][0]
        rows = sorted(self.m.rows_shards, key=lambda r: -r[key])[: self.top_n]
        h = self.s.getmaxyx()[0]
        mx = rows[0]["qtime"] if rows else 1
        for r in rows:
            if y >= h - 4:
                break
            lv = CRIT if r["qtime"] >= 0.8 else (WARN if r["qtime"] >= 0.4 else DIM)
            self.cell(y, 0, f"{r['index'][:38]:<38} {r['shard']:4d} {r['prirep']:>3} {short_node(r['node']):<26} {r['qtime']:10.2f} {r['qps']:7.1f} {r['qms']:6.1f} {r['ftime']:10.2f} {r['docs'] / 1e6:7.1f} {r['gb']:6.1f} {r['segs']:4d} {r['merges']:3d}", lv)
            self.cell(y, 140, gauge(100.0 * r["qtime"] / mx if mx else 0, 20), lv)
            y += 1
        # per-node load share
        tot = sum(r["qtime"] for r in self.m.rows_shards) or 1
        per_node = collections.Counter()
        for r in self.m.rows_shards:
            per_node[short_node(r["node"], 22)] += r["qtime"]
        if per_node and y < h - 2:
            y += 1
            self.put(y, 0, "search load share by node", TITLE, bold=True); y += 1
            n = len(per_node); even = 100.0 / n
            x = 0
            for node, t in per_node.most_common():
                share = 100.0 * t / tot
                txt = f"{node} {share:4.1f}%  "
                if x + len(txt) >= self.width():
                    x = 0; y += 1
                    if y >= h - 1:
                        break
                self.cell(y, x, txt, CRIT if share > 2 * even else (WARN if share > 1.5 * even else DIM)); x += len(txt)
        return y

    # ---- 5 alerts
    def draw_alerts(self, y):
        self.put(y, 0, f"alerts ({len(self.m.alerts)}) - most severe first".ljust(self.width() - 1), HEAD, bold=True); y += 1
        h = self.s.getmaxyx()[0]
        for lv, msg in sorted(self.m.alerts, key=lambda a: {FATAL: 0, CRIT: 1, WARN: 2}.get(a[0], 9)):
            if y >= h - 1:
                break
            self.cell(y, 0, f"{SEV[lv]} ", lv); self.cell(y, 6, msg, lv); y += 1
        if not self.m.alerts:
            self.cell(y, 0, "✔ no issues detected", OK)
        return y

    # ---- 6 checks
    def draw_checks(self, y):
        hdr = f"{'':<6}{'area':<10}{'check':<48}{'value':<24}{'why it matters / fix'}"
        self.put(y, 0, hdr.ljust(self.width() - 1), HEAD, bold=True); y += 1
        h = self.s.getmaxyx()[0]
        if not self.m.checks:
            self.put(y, 0, "config audit runs on the first tick and every 5 minutes…", DIM); return y
        for sev, area, name, value, hint in self.m.checks:
            if y >= h - 1:
                break
            tag = {OK: "PASS ", WARN: "WARN ", CRIT: "FAIL ", FATAL: "FAIL ", DIM: "INFO "}[sev]
            self.cell(y, 0, tag, sev); self.cell(y, 6, f"{area:<10}", DIM); self.cell(y, 16, f"{name[:47]:<48}", sev); self.cell(y, 64, f"{value[:23]:<24}", sev); self.put(y, 88, hint, DIM); y += 1
        return y

    # ---- 7 live tasks: what is running RIGHT NOW (the "which query is killing us" view)
    def draw_tasks(self, y):
        m = self.m
        h = self.s.getmaxyx()[0]
        if m.no_tasks:
            self.put(y, 0, "task collection disabled (--no-tasks)", DIM); return y
        if m.task_err:
            self.cell(y, 0, f"_tasks API unavailable: {m.task_err[:120]} (needs monitor privilege)", WARN); return y
        self.put(y, 0, f"in-flight coordinating search tasks, longest first ({len(m.tasks)})"
                       + ("" if m.show_queries else "   query bodies hidden (--show-queries to display; may contain PHI)"), TITLE, bold=True); y += 1
        hdr = f"{'run s':>7} {'node':<26} {'type':<16} {'size':>6} {'agg':>3} {'indices':<46} {'task id (for POST _tasks/<id>/_cancel)'}"
        self.put(y, 0, hdr.ljust(self.width() - 1), HEAD, bold=True); y += 1
        for t in m.tasks[: self.top_n]:
            if y >= h - 6:
                break
            lv = level(t["run_s"], T["task_warn_s"], T["task_crit_s"])
            self.cell(y, 0, f"{t['run_s']:7.1f} {short_node(t['node']):<26} {t['stype'][:16]:<16} {str(t['size'] if t['size'] is not None else '-'):>6} {'yes' if t['aggs'] else '-':>3} {t['indices'][:46]:<46} {t['id']}", lv)
            if m.show_queries and t["source"]:
                y += 1
                self.put(y, 8, t["source"][: self.width() - 10], DIM)
            y += 1
        if not m.tasks:
            self.cell(y, 0, "✔ no search tasks in flight at this instant", OK); y += 1
        y += 1
        self.put(y, 0, f"background work competing with search ({len(m.bg_tasks)}): reindex / delete_by_query / forcemerge / snapshot", TITLE, bold=True); y += 1
        for t in m.bg_tasks[:8]:
            if y >= h - 1:
                break
            self.cell(y, 0, f"{t['run_s']:7.1f} {short_node(t['node']):<26} {t['action'].split('/')[-1]:<16} {t['indices'][:80]}", WARN); y += 1
        if not m.bg_tasks:
            self.cell(y, 0, "✔ none", OK); y += 1
        if m.explain or m.pending_top:
            y += 1
            self.put(y, 0, "cluster diagnostics", TITLE, bold=True); y += 1
            if m.explain:
                ex = m.explain
                self.cell(y, 0, f"unassigned {ex.get('index', '?')}[{ex.get('shard', '?')}] primary={ex.get('primary')} state={ex.get('current_state')} reason={ex.get('unassigned_info', {}).get('reason')}", WARN); y += 1
                self.put(y, 2, (ex.get("allocate_explanation") or "")[: self.width() - 4], DIM); y += 1
                for nd in ex.get("node_allocation_decisions", [])[:3]:
                    for d in nd.get("deciders", [])[:1]:
                        if y < h - 1:
                            self.put(y, 2, f"{nd.get('node_name', '?')}: {d.get('decider')}: {d.get('explanation', '')}"[: self.width() - 4], DIM); y += 1
            for pr, ms, src in m.pending_top:
                if y < h - 1:
                    self.put(y, 0, f"pending [{pr}] {ms / 1000:.1f}s in queue: {src}", WARN); y += 1
        return y

    def draw(self):
        self.s.erase()
        y = self.draw_header()
        {1: self.draw_overview, 2: self.draw_nodes, 3: self.draw_indices, 4: self.draw_shards, 5: self.draw_alerts, 6: self.draw_checks, 7: self.draw_tasks}[self.view](y)
        self.s.refresh()

    def _spawn(self, fn):
        """Run a (possibly slow) ES call off the UI thread so keys and the clock never freeze."""
        if self.worker and self.worker.is_alive():
            return
        self.worker = threading.Thread(target=fn, daemon=True)
        self.worker.start()

    def _hot_now(self):
        self.m.sample_hot(); self.m._alerts()

    def loop(self):
        last = 0
        quit_keys = (ord("q"), 27) if not isinstance(self.s, AnsiScreen) else (ord("q"),)  # ESC prefixes arrow keys on a raw tty
        while True:
            now = time.time()
            if not self.paused and now - last >= self.interval:
                self._spawn(self.m.refresh); last = now
            self.draw()
            ch = self.s.getch()
            if ch in quit_keys:
                return
            elif ord("1") <= ch <= ord("7"):
                self.view = ch - ord("0")
            elif ch == ord("s"):
                if self.view == 2: self.sort_n = (self.sort_n + 1) % len(self.SORT_NODES)
                elif self.view == 3: self.sort_i = (self.sort_i + 1) % len(self.SORT_IDX)
                elif self.view == 4: self.sort_s = (self.sort_s + 1) % len(self.SORT_SH)
            elif ch == ord("p"): self.paused = not self.paused
            elif ch == ord("h"): self._spawn(self._hot_now)
            elif ch == ord("d"): self.dump_msg = dump_snapshot(self.m)
            elif ch in (ord("+"), ord("=")): self.interval = min(self.interval + 1, 60)
            elif ch == ord("-"): self.interval = max(self.interval - 1, 1)
            time.sleep(0.2 if HAVE_CURSES else 0.5)


# ───────────────────────── snapshot / support bundle ─────────────────────────
def snapshot(m, with_hot_raw=False):
    """Everything estop currently knows, as plain JSON-able data (attach to a ticket / incident)."""
    d = dict(
        taken_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"), es_version=m.version, health_score=m.score, cluster=m.cluster,
        alerts=[dict(severity=SEV[lv].strip(), message=msg) for lv, msg in m.alerts],
        config_audit=[dict(status={OK: "PASS", WARN: "WARN", CRIT: "FAIL", FATAL: "FAIL", DIM: "INFO"}[sev], area=a, check=n, value=v, hint=h)
                      for sev, a, n, v, h in m.checks],
        nodes=m.rows_nodes, non_data_nodes=m.rows_other,
        indices=sorted(m.rows_idx, key=lambda r: -r["qps"])[:50], hot_shards=m.rows_shards[:50],
        alias_fanout=m.alias_shards, search_tasks=m.tasks, background_tasks=m.bg_tasks,
        allocation_explain=m.explain, pending_tasks=m.pending_top,
        hot_threads=dict(total=m.hot["total"], search=m.hot["search"], io_bound_pct=m.hot["io_bound"], kinds=dict(m.hot["kinds"]), top=m.hot["top"]),
    )
    if with_hot_raw:
        d["hot_threads_raw"] = m.hot_raw
    return json.loads(json.dumps(d, default=str))


def dump_snapshot(m, path=None):
    path = path or time.strftime("estop-%Y%m%d-%H%M%S.json")
    try:
        with open(path, "w") as f:
            json.dump(snapshot(m, with_hot_raw=True), f, indent=1)
        return f"✔ snapshot written to {os.path.abspath(path)}"
    except OSError as e:
        return f"✖ snapshot failed: {e}"


# ───────────────────────── main ─────────────────────────
def main():
    ap = argparse.ArgumentParser(description="htop-style Elasticsearch search-performance monitor")
    ap.add_argument("--version", action="version", version=f"estop {__version__}")
    ap.add_argument("-i", "--interval", type=int, default=5)
    ap.add_argument("-n", "--top", type=int, default=25, help="rows in indices / shards views")
    ap.add_argument("--index-prefix", default=os.environ.get("ES_INDEX_PREFIX", ""), help="e.g. pd_")
    ap.add_argument("--hot-every", type=int, default=6, help="sample hot_threads every N ticks")
    ap.add_argument("--no-hot", action="store_true", help="never call hot_threads")
    ap.add_argument("--plain", action="store_true", help="force ANSI renderer")
    ap.add_argument("--once", action="store_true", help="two samples, plain-text report, exit")
    ap.add_argument("--json", action="store_true", help="with --once: emit the full snapshot as JSON instead of text")
    ap.add_argument("--no-tasks", action="store_true", help="never call the _tasks API (view 7)")
    ap.add_argument("--show-queries", action="store_true", help="show search request bodies in view 7 (may contain sensitive data)")
    ap.add_argument("--node-strip", default=os.environ.get("ES_NODE_STRIP", ""),
                    help="comma-separated substrings to strip from node names for display, e.g. 'myorg-es-prod-,-vm'")
    args = ap.parse_args()
    NODE_STRIP[:] = [x for x in args.node_strip.split(",") if x]
    es = ES()
    try:
        model = Model(es, args.index_prefix, args.hot_every, args.no_hot, args.no_tasks, args.show_queries)
    except urllib.error.HTTPError as e:
        sys.exit(f"HTTP {e.code} from {es.url}: check ES_USER/ES_PASS or ES_API_KEY (needs monitor privilege)")
    except Exception as e:  # noqa
        sys.exit(f"cannot reach {es.url}: {e}  (INSECURE=1 for self-signed TLS)")
    if args.once:
        model.refresh()
        time.sleep(max(args.interval, 2))
        model.refresh()
        if model.err or not model.cluster.get("name"):
            sys.exit(f"collection failed: {model.err or 'no data'}")
        if not args.no_hot and not model.hot["ts"]:  # the second refresh already sampled when hot_every allows it
            model.sample_hot(); model._alerts()
        if args.json:
            print(json.dumps(snapshot(model, with_hot_raw=True), indent=1))
            return
        c = model.cluster
        print(clean(f"{c['name']} {c['status']}  health {model.score}/100  shard-q/s {c.get('qps', 0):.1f}  avg q {c.get('qms', 0):.1f} ms  fetch {c.get('fms', 0):.1f} ms ({c.get('fetch_share', 0):.0f}%)  pool {c.get('active', 0)}/{c.get('pool', 0)}  queue {c.get('queue', 0)}  rej {c.get('rej_rate', 0):.2f}/s  cache-cov {c.get('cache_cov', 0):.0f}%  io-bound {model.hot['io_bound']:.0f}%"))
        for r in sorted(model.rows_nodes, key=lambda r: -r["qps"]):
            print(clean(f"  {r['short']:<26} cpu {r['cpu']:3.0f}% ld/c {r['load']:4.2f} heap {r['heap']:3.0f}% cache {r['cache_cov']:3.0f}% rd {r['read_mbps']:4.0f}MB/s act {r['active']}/{r['max']} q {r['queue']} rej {r['rej_rate']:.2f}/s q/s {r['qps']:.1f} q {r['qms']:.1f}ms f {r['fms']:.1f}ms"))
        for lv, msg in sorted(model.alerts, key=lambda a: {FATAL: 0, CRIT: 1, WARN: 2}.get(a[0], 9)):
            print(clean(f"  [{SEV[lv].strip()}] {msg}"))
        if model.tasks:
            print("longest in-flight searches:")
            for t in model.tasks[:5]:
                print(clean(f"  {t['run_s']:6.1f}s {t['node']} [{t['indices'][:60]}] {t['id']}"))
        if model.checks:
            print("config audit:")
            for sev, area, name, value, hint in model.checks:
                if sev != OK:
                    print(clean(f"  [{ {WARN: 'WARN', CRIT: 'FAIL', FATAL: 'FAIL', DIM: 'INFO'}[sev]}] {area} · {name} = {value}  - {hint}"))
        return
    run = lambda scr: UI(scr, model, args.interval, args.top).loop()  # noqa
    if HAVE_CURSES and not args.plain:
        curses.wrapper(run)
    else:
        if not HAVE_CURSES:
            sys.stderr.write("note: no _curses module; using ANSI renderer\n"); time.sleep(1)
        ansi_wrapper(run)


if __name__ == "__main__":
    main()
