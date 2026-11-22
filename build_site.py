"""
Будує публічну сторінку дашборду з архіву data/raw.

Результат — папка site/ з трьома файлами: index.html (сторінка з мапою),
data.json (зведені дані) і .nojekyll (службовий файл для GitHub Pages).
Сторінку публікує наступний крок у .github/workflows/collect.yml.
"""

import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import collect as C

SITE_DIR = Path("site")
TEMPLATE = Path("site_template.html")
MAP_FILE = Path("ukraine_map.json")


def week_start(day):
    d = date.fromisoformat(day)
    return (d - timedelta(days=d.weekday())).isoformat()


def main():
    store = C.load_store()
    if not store:
        print("Архів порожній, сторінку не будуємо.")
        return
    rates = C.load_rates()
    fetched = set()
    ukraine = json.loads(MAP_FILE.read_text(encoding="utf-8"))

    regions = [[r["iso"], r["name"]] for r in sorted(ukraine["regions"], key=lambda r: r["name"])]
    regions += [[C.OUTSIDE_CODE, "Поза Україною"], [C.UNKNOWN_CODE, "Регіон невідомий"]]
    g_index = {code: i for i, (code, _) in enumerate(regions)}

    page_names = {}            # page_id -> (остання дата, назва)
    a, r = {}, {}              # зведення: (тиждень, сторінка, дисклеймер[, регіон]) -> [грн, оголошень]
    no_rate = no_regions = skipped = 0

    for ad in store.values():
        day = C.ad_day(ad)
        if day == "unknown":
            skipped += 1
            continue
        week = week_start(day)
        pid = str(ad.get("page_id", ""))
        name = ad.get("page_name", "") or "(без назви)"
        if pid not in page_names or day >= page_names[pid][0]:
            page_names[pid] = (day, name)
        disc = 1 if str(ad.get("bylines") or "").strip() else 0
        cur = ad.get("currency") or ""
        _, _, mid = C.range_values(ad.get("spend"))
        rate = C.rate_for(rates, cur, day, fetched) if cur else float("nan")
        uah = mid * rate
        if not math.isfinite(uah):
            no_rate += 1 if math.isfinite(mid) else 0
            uah = 0.0
        cell = a.setdefault((week, pid, disc), [0.0, 0])
        cell[0] += uah
        cell[1] += 1

        shares = {}
        for reg in ad.get("delivery_by_region") or []:
            share = C.to_num(reg.get("percentage"))
            if math.isfinite(share):
                code = C.match_region(reg.get("region"))[0]
                shares[code] = shares.get(code, 0.0) + share
        if not shares:
            no_regions += 1
        for code, share in shares.items():
            cell = r.setdefault((week, pid, disc, g_index[code]), [0.0, 0])
            cell[0] += uah * share
            cell[1] += 1

    weeks = sorted({k[0] for k in a})
    w_index = {w: i for i, w in enumerate(weeks)}
    totals = {}
    for (w, pid, d), (s, n) in a.items():
        totals[pid] = totals.get(pid, 0.0) + s
    pages = sorted(page_names, key=lambda p: -totals.get(p, 0.0))
    p_index = {p: i for i, p in enumerate(pages)}

    def columns(cube, with_region):
        cols = {"w": [], "p": [], "d": [], "s": [], "n": []}
        if with_region:
            cols["g"] = []
        for key in sorted(cube, key=lambda k: (w_index[k[0]], p_index[k[1]])):
            s, n = cube[key]
            cols["w"].append(w_index[key[0]])
            cols["p"].append(p_index[key[1]])
            cols["d"].append(key[2])
            if with_region:
                cols["g"].append(key[3])
            cols["s"].append(round(s))
            cols["n"].append(n)
        return cols

    last_update = (C.LAST_SUCCESS_FILE.read_text().strip()
                   if C.LAST_SUCCESS_FILE.exists() else "невідомо")
    data = {
        "updated": last_update,
        "built": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "weeks": weeks,
        "pages": [[p, page_names[p][1]] for p in pages],
        "regions": regions,
        "a": columns(a, False),
        "r": columns(r, True),
        "stats": {"ads": len(store) - skipped, "pages": len(pages),
                  "no_regions": no_regions, "no_rate": no_rate},
    }

    SITE_DIR.mkdir(exist_ok=True)
    (SITE_DIR / "data.json").write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")),
                                        encoding="utf-8")
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
    size = (SITE_DIR / "data.json").stat().st_size
    print(f"Сторінку побудовано: тижнів {len(weeks)}, сторінок {len(pages)}, "
          f"рядків {len(data['a']['w'])}/{len(data['r']['w'])}, data.json {size / 1e6:.1f} МБ")


if __name__ == "__main__":
    main()
