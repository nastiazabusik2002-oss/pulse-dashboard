"""Перевірка /stats.json (формат ttn-dept-stats/1).

  python3 check_stats.py cache/stats.json            — перевірити файл
  STATS_TOKEN=... python3 check_stats.py http://pulse-dashboard.anastasiia-zabusyk.svc.cluster.local:8080/stats.json
                                                     — ще й 401 без токена / 200 з токеном
"""
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

REQUIRED = ["incoming", "processed", "automated", "backlog", "median_minutes", "sla_breached"]
TOP = {"schema", "department", "title", "owner", "generated_at", "tz", "sources", "metrics", "daily", "now"}


def fetch(url, token=None):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"} if token else {})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def check(raw):
    errs = []
    if len(raw) > 1_000_000:
        errs.append(f"розмір {len(raw)} байт > 1 МБ")
    d = json.loads(raw)
    if d.get("schema") != "ttn-dept-stats/1":
        errs.append("schema ≠ ttn-dept-stats/1")
    if set(d) - TOP:
        errs.append(f"зайві ключі верхнього рівня: {sorted(set(d) - TOP)}")
    if TOP - set(d):
        errs.append(f"бракує ключів верхнього рівня: {sorted(TOP - set(d))}")
    gen = datetime.strptime(d["generated_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    age_h = (datetime.now(timezone.utc) - gen).total_seconds() / 3600
    if age_h > 2:
        errs.append(f"generated_at старіший за 2 год ({age_h:.1f} год)")
    described = {m["key"] for m in d["metrics"]}
    for m in d["metrics"]:
        for k in ("key", "label", "unit", "definition", "higher_is_better"):
            if k not in m:
                errs.append(f"метрика {m.get('key')}: немає {k}")
    used = set()
    dates = []
    for row in d["daily"]:
        dates.append(row.get("date"))
        for k in REQUIRED:
            if k not in row:
                errs.append(f"{row.get('date')}: немає {k}")
        for k, v in row.items():
            if k == "date":
                continue
            used.add(k)
            if v is not None and not isinstance(v, (int, float)):
                errs.append(f"{row['date']}.{k}: не число і не null ({v!r})")
    used |= set(d["now"])
    if used - described:
        errs.append(f"не описані в metrics: {sorted(used - described)}")
    if dates != sorted(dates) or len(dates) != len(set(dates)):
        errs.append("дати не по порядку або є дублі")
    if "@" in raw.decode("utf-8", "replace").replace("ai_bot@ttn.global", ""):
        errs.append("схоже на email у відповіді")
    nulls = {k: sum(1 for r in d["daily"] if r.get(k) is None) for k in sorted(used - {"oldest_hours"})}
    print(f"днів: {len(dates)} ({dates[0]} … {dates[-1]}), generated_at {d['generated_at']} ({age_h:.1f} год тому), "
          f"розмір {len(raw)} Б")
    print("null по метриках:", {k: v for k, v in nulls.items() if v})
    print("now:", d["now"])
    return errs


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else "cache/stats.json"
    errs = []
    if src.startswith("http"):
        st, _ = fetch(src)
        print("без токена:", st)
        if st != 401:
            errs.append(f"без токена очікували 401, отримали {st}")
        st, _ = fetch(src, "wrong-token-" + "x" * 24)
        print("чужий токен:", st)
        if st != 401:
            errs.append(f"з чужим токеном очікували 401, отримали {st}")
        st, raw = fetch(src, os.environ.get("STATS_TOKEN", ""))
        print("з токеном:", st)
        if st != 200:
            errs.append(f"з токеном очікували 200, отримали {st}")
            raw = None
    else:
        raw = open(src, "rb").read()
    if raw:
        errs += check(raw)
    print("OK — усі перевірки пройдено" if not errs else "ПОМИЛКИ:\n- " + "\n- ".join(errs))
    sys.exit(1 if errs else 0)


if __name__ == "__main__":
    main()
