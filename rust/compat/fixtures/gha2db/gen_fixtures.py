#!/usr/bin/env python3
"""Build the gha2db test fixtures from real GH Archive hours.

Usage: gen_fixtures.py <dir with YYYY-MM-DD-H.json.gz files>

For every hour a subset of the events is kept: all events of a handful of
organisations (so org/repo filters have something to match) plus up to three
events of every other event type seen in the hour (so every writer path is
exercised), including a few events without `payload`/`repository`. All
e-mail addresses are replaced by stable `userN@example.com` pseudonyms (the
same address maps to the same pseudonym everywhere, so the actor caches and
the `Signed-off-by` trailers behave like on real data).
"""
import collections
import gzip
import json
import os
import re
import sys

SRC = sys.argv[1] if len(sys.argv) > 1 else "/tmp/g2r/gha"
DST = os.path.dirname(os.path.abspath(__file__))

PLAN = {
    "2013-06-01-10": ["mozilla", "thoughtbot-denver", "adobe", "aruseni", "klaussilveira", "blasv"],
    "2014-12-31-23": ["fujimura", "bharvidixit", "moneytree", "XeroAPI", "prakhar1989", "Secretchronicles"],
    "2015-01-01-15": ["rust-lang", "kaltura", "KSP-CKAN", "github", "sympy", "tkellen", "deeperx"],
    "2020-05-01-10": ["dotnet", "nextcloud", "matrix-org", "aws"],
    "2025-11-20-12": ["dotnet", "grafana", "Azure"],
}
PER_TYPE = 3
EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9\-]+(?:\.[A-Za-z0-9\-]+)+")
emails = {}


def pseudo(m):
    e = m.group(0).lower()
    if e not in emails:
        emails[e] = "user%d@example.com" % (len(emails) + 1)
    return emails[e]


def org_of(e):
    if "repo" in e:
        return e["repo"]["name"].split("/")[0]
    r = e.get("repository")
    if not r:
        return None
    return r.get("organization") or r.get("owner")


def build(hour, orgs):
    keep, per_type = [], collections.Counter()
    with gzip.open(os.path.join(SRC, hour + ".json.gz"), "rt", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            e = json.loads(line)
            t = e["type"]
            special = "payload" not in e or ("repo" not in e and "repository" not in e)
            if org_of(e) in orgs or per_type[t] < PER_TYPE or (special and per_type[t] < 2 * PER_TYPE):
                per_type[t] += 1
                keep.append(EMAIL.sub(pseudo, line))
    data = ("\n".join(keep) + "\n").encode("utf-8")
    out = os.path.join(DST, hour + ".json.gz")
    with open(out, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as f:
        f.write(data)
    print("%s: %d events, %d bytes raw, %d bytes gz" % (hour, len(keep), len(data), os.path.getsize(out)))


for hour, orgs in PLAN.items():
    build(hour, orgs)

# 2012 hours: `created_at` is `2012/03/11 12:00:00 -0700` (not RFC 3339) — the
# tools cannot decode them; a few lines are enough for the error paths.
with gzip.open(os.path.join(SRC, "2012-03-11-12.json.gz"), "rt", encoding="utf-8") as f:
    lines = [EMAIL.sub(pseudo, l.rstrip("\n")) for l in f if l.strip()][:3]
with open(os.path.join(DST, "2012-03-11-12.json.gz"), "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as f:
    f.write(("\n".join(lines) + "\n").encode("utf-8"))
print("2012-03-11-12: %d events" % len(lines))
# The real archive serves a 20-byte empty gzip member for this hour (kept as is).
with open(os.path.join(SRC, "2012-03-10-15.json.gz"), "rb") as f:
    empty = f.read()
assert gzip.decompress(empty) == b"" and len(empty) == 20
with open(os.path.join(DST, "2012-03-10-15.json.gz"), "wb") as f:
    f.write(empty)
print("emails pseudonymised: %d" % len(emails))
