"""
Будує публічну сторінку дашборду з архіву.

Результат — папка site/:
  index.html            сторінка з мапою, фільтрами й графіками;
  data/meta.json        довідники і зведення, потрібні одразу;
  data/r/<регіон>.json  деталізація за одним регіоном (вантажиться після кліку по області);
  data/p/<n>.json       деталізація за групою рекламодавців (вантажиться після вибору рекламодавця).
Так сторінка відкривається швидко навіть з архівом у сотні тисяч оголошень.
Сторінку публікує наступний крок у .github/workflows/collect.yml.
"""

import json
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path

import collect as C

SITE_DIR = Path("site")
TEMPLATE = Path("site_template.html")
MAP_FILE = Path("ukraine_map.json")
SHARDS = 256


def dump(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def main():
    if not C.archive_files():
        print("Архів порожній, сторінку не будуємо.")
        return
    rates = C.load_rates()
    fetched = set()
    start_month = C.covered_from(C.load_coverage())
    ukraine = json.loads(MAP_FILE.read_text(encoding="utf-8"))

    regions = [[r["iso"], r["name"]] for r in sorted(ukraine["regions"], key=lambda r: r["name"])]
    regions += [[C.OUTSIDE_CODE, "Поза Україною"], [C.UNKNOWN_CODE, "Регіон невідомий"]]
    g_index = {code: i for i, (code, _) in enumerate(regions)}

    page_names = {}            # page_id -> (остання дата, назва)
    a, full = {}, {}           # (місяць, сторінка, дисклеймер[, регіон]) -> [грн, оголошень]
    stats = {"ads": 0, "no_rate": 0, "no_regions": 0, "before_start": 0}
    seen = set()

    for ad in C.iter_archive():
        if ad["id"] in seen:
            continue
        seen.add(ad["id"])
        v = C.ad_values(ad, rates, fetched)
        if v["day"] == "unknown" or C.month_key(v["day"]) < start_month:
            stats["before_start"] += 1
            continue
        month = C.month_key(v["day"])
        pid = str(ad.get("page_id", ""))
        name = ad.get("page_name", "") or "(без назви)"
        if pid not in page_names or v["day"] >= page_names[pid][0]:
            page_names[pid] = (v["day"], name)
        uah = v["uah"]
        if not math.isfinite(uah):
            stats["no_rate"] += 1 if math.isfinite(v["s"][2]) else 0
            uah = 0.0
        stats["ads"] += 1
        cell = a.setdefault((month, pid, int(v["disc"])), [0.0, 0])
        cell[0] += uah
        cell[1] += 1
        if not v["shares"]:
            stats["no_regions"] += 1
        for (code, _), share in v["shares"].items():
            cell = full.setdefault((month, pid, int(v["disc"]), g_index[code]), [0.0, 0])
            cell[0] += uah * share
            cell[1] += 1

    months = sorted({k[0] for k in a})
    m_index = {m: i for i, m in enumerate(months)}
    totals = {}
    for (m, pid, d), (s, n) in a.items():
        totals[pid] = totals.get(pid, 0.0) + s
    pages = sorted(page_names, key=lambda p: -totals.get(p, 0.0))
    p_index = {p: i for i, p in enumerate(pages)}

    def table(rows, keys):
        cols = {k: [] for k in keys}
        for row in rows:
            for k, val in zip(keys, row):
                cols[k].append(val)
        return cols

    a_rows = sorted(((m_index[m], p_index[p], d, round(s), n) for (m, p, d), (s, n) in a.items()))
    rb = {}
    region_files = {i: [] for i in range(len(regions))}
    page_files = {i: [] for i in range(SHARDS)}
    for (m, p, d, g), (s, n) in full.items():
        mi, pi = m_index[m], p_index[p]
        cell = rb.setdefault((mi, d, g), [0.0, 0])
        cell[0] += s
        cell[1] += n
        region_files[g].append((mi, pi, d, round(s), n))
        page_files[pi % SHARDS].append((mi, pi, d, g, round(s), n))
    rb_rows = sorted((mi, d, g, round(s), n) for (mi, d, g), (s, n) in rb.items())

    if SITE_DIR.exists():
        shutil.rmtree(SITE_DIR)
    data_dir = SITE_DIR / "data"
    last_update = (C.LAST_SUCCESS_FILE.read_text().strip()
                   if C.LAST_SUCCESS_FILE.exists() else "невідомо")
    dump(data_dir / "meta.json", {
        "updated": last_update,
        "built": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "from_month": start_month,
        "months": months,
        "pages": [[p, page_names[p][1]] for p in pages],
        "regions": regions,
        "shards": SHARDS,
        "a": table(a_rows, "mpdsn"),
        "rb": table(rb_rows, "mdgsn"),
        "stats": {**stats, "pages": len(pages)},
    })
    for g, rows in region_files.items():
        dump(data_dir / "r" / f"{regions[g][0]}.json", table(sorted(rows), "mpdsn"))
    for k, rows in page_files.items():
        dump(data_dir / "p" / f"{k}.json", table(sorted(rows), "mpdgsn"))

    map_json = json.dumps({"width": ukraine["width"], "height": ukraine["height"],
                           "regions": [{"iso": x["iso"], "name": x["name"], "d": x["d"]}
                                       for x in ukraine["regions"]]},
                          ensure_ascii=False, separators=(",", ":"))
    html = TEMPLATE.read_text(encoding="utf-8").replace("/*MAP_JSON*/null", map_json)
    (SITE_DIR / "index.html").write_text(html, encoding="utf-8")
    (SITE_DIR / ".nojekyll").write_text("")
    (SITE_DIR / "README.md").write_text(
        "# Політична реклама Meta в Україні — дашборд ОПОРИ\n\n"
        "Сторінку автоматично генерує конвеєр збору даних. Не редагуйте файли тут вручну: "
        "їх перезаписує щоденне оновлення.\n", encoding="utf-8")
    C.RATES_FILE.write_text(json.dumps(rates, ensure_ascii=False, sort_keys=True, indent=0))
    size = sum(f.stat().st_size for f in data_dir.rglob("*.json"))
    meta = (data_dir / "meta.json").stat().st_size
    print(f"Сторінку побудовано з {start_month}: місяців {len(months)}, сторінок {len(pages)}, "
          f"оголошень {stats['ads']}; meta.json {meta / 1e6:.1f} МБ, усі дані {size / 1e6:.1f} МБ")


if __name__ == "__main__":
    main()
