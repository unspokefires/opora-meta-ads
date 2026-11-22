"""
Щоденний збір політичної реклами Meta для дашборду ОПОРИ.

Що робить скрипт за один запуск:
  1. Канал пошуку: забирає з Ad Library API всю політичну рекламу, показану
     в Україні за останні WINDOW_DAYS днів.
  2. Канал сторінок (раз на тиждень): забирає оголошення сторінок зі звіту
     Ad Library Report, який лежить у репозиторії, щоб перевірити повноту
     першого каналу.
  3. Зберігає сирі дані в папці data/raw (архів, який не зникне з бібліотеки).
  4. Перераховує витрати в гривні й долари за курсом НБУ.
  5. Перезаписує робочі вкладки Google-таблиці; вкладку «атрибуція» лише доповнює.

Налаштування нижче можна змінювати без знання Python: це прості числа й дати.
"""

import glob
import gzip
import io
import json
import os
import re
import sys
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# ============================== НАЛАШТУВАННЯ ==============================
COUNTRY = "UA"
WINDOW_DAYS = 30                 # щоденний пошук: оголошення, показані за останні N днів
BACKFILL_FROM = "2026-01-01"     # з якої дати дозавантажити дані під час першого запуску
PAGE_WINDOW_DAYS = 35            # канал сторінок: оголошення за останні N днів
PAGE_CHANNEL_WEEKDAY = 0         # у який день тижня вмикати канал сторінок (0 = понеділок)
PAGE_CHANNEL_MAX_REQUESTS = 400  # максимум запитів каналу сторінок (по 10 сторінок у запиті)
TOP_UNMATCHED_REGIONS = 50       # скільки незіставлених назв регіонів показувати в перевірці
# ==========================================================================

API_URL = "https://graph.facebook.com/ads_archive"
NBU_URL = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange"
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
RATES_FILE = DATA_DIR / "nbu_rates.json"
OUTSIDE_CODE, OUTSIDE_NAME = "OUT", "поза Україною або не розпізнано"

FIELDS = [
    "id", "page_id", "page_name", "bylines",
    "ad_creation_time", "ad_delivery_start_time", "ad_delivery_stop_time",
    "ad_creative_bodies", "ad_creative_link_titles",
    "currency", "spend", "impressions",
    "delivery_by_region", "publisher_platforms", "languages",
]
RATE_LIMIT_CODES = {4, 17, 32, 613}
NOW = datetime.now(timezone.utc)
TODAY = NOW.date()
RUN_AT = NOW.strftime("%Y-%m-%d %H:%M UTC")


def log(msg):
    print(msg, flush=True)


# ------------------------------------------------------------------ Meta API

class FatalApiError(Exception):
    """Помилка, після якої немає сенсу продовжувати (наприклад, токен сплив)."""


def fetch_ads(token, extra_params, date_from, date_to, limit=250, max_retries=5, label=""):
    """Забирає всі порції відповіді API. Повертає (оголошення, чи повне завантаження)."""
    params = {
        "access_token": token,
        "ad_reached_countries": json.dumps([COUNTRY]),
        "ad_type": "POLITICAL_AND_ISSUE_ADS",
        "ad_active_status": "ALL",
        "ad_delivery_date_min": date_from,
        "ad_delivery_date_max": date_to,
        "fields": ",".join(FIELDS),
        "limit": limit,
        **extra_params,
    }
    ads, retries = [], 0
    while True:
        try:
            resp = requests.get(API_URL, params=params, timeout=120)
            payload = resp.json()
        except (requests.RequestException, ValueError) as e:
            retries += 1
            if retries > max_retries:
                log(f"{label} мережева помилка, зупиняюся: {type(e).__name__}")
                return ads, False
            time.sleep(30)
            continue

        if "error" in payload:
            err = payload["error"]
            code, msg = err.get("code"), err.get("message", "")
            if code == 190:
                raise FatalApiError(
                    "Токен Meta недійсний або сплив. Згенеруйте новий довгостроковий токен "
                    f"і оновіть секрет META_TOKEN. Повідомлення Meta: {msg}")
            if (code == 1 or "reduce the amount of data" in msg.lower()) and params["limit"] > 25:
                params["limit"] = max(25, params["limit"] // 2)
                continue
            if code in RATE_LIMIT_CODES and retries < max_retries:
                retries += 1
                wait = 120 * retries
                log(f"{label} ліміт запитів Meta, чекаю {wait} с…")
                time.sleep(wait)
                continue
            details = " | ".join(str(err[k]) for k in
                                 ("error_subcode", "error_user_title", "error_user_msg") if err.get(k))
            log(f"{label} помилка API (код {code}): {msg} {details}".strip())
            return ads, False

        retries = 0
        ads.extend(payload.get("data", []))
        paging = payload.get("paging", {})
        after = paging.get("cursors", {}).get("after")
        if not paging.get("next") or not after:
            return ads, True
        params["after"] = after


# ------------------------------------------------------------ сирий архів

def ad_day(ad):
    d = str(ad.get("ad_delivery_start_time") or ad.get("ad_creation_time") or "")[:10]
    return d if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) else "unknown"


def raw_path(day):
    if day == "unknown":
        return RAW_DIR / "unknown.jsonl.gz"
    return RAW_DIR / day[:4] / day[5:7] / f"{day}.jsonl.gz"


def load_store():
    store = {}
    for f in sorted(RAW_DIR.rglob("*.jsonl.gz")):
        with gzip.open(f, "rt", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    ad = json.loads(line)
                    store[str(ad["id"])] = ad
    return store


def merge_into_store(store, ads, channel):
    """Нова версія оголошення замінює стару (свіжіші витрати), канали накопичуються."""
    touched = set()
    for ad in ads:
        ad_id = str(ad.get("id", ""))
        if not ad_id:
            continue
        old = store.get(ad_id, {})
        channels = sorted(set(old.get("_channels", [])) | {channel})
        new = dict(ad)
        new["id"] = ad_id
        new["_channels"] = channels
        # Дату першої появи зберігаємо, а «останню» ні: інакше файли архіву
        # змінювалися б щодня навіть без нових даних.
        new["_first_seen"] = old.get("_first_seen", RUN_AT)
        store[ad_id] = new
        touched.add(ad_day(new))
    return touched


def save_store(store, days):
    """Перезаписує лише файли тих днів, які змінилися. Стиснення детерміноване,
    тож незмінені дані не створюють зайвих змін у git."""
    by_day = {}
    for ad in store.values():
        by_day.setdefault(ad_day(ad), []).append(ad)
    for day in days:
        path = raw_path(day)
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [json.dumps(a, ensure_ascii=False, sort_keys=True)
                 for a in sorted(by_day.get(day, []), key=lambda a: a["id"])]
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
            gz.write(("\n".join(lines) + "\n").encode("utf-8"))
        path.write_bytes(buf.getvalue())


# -------------------------------------------------------------- звіт Meta

def find_latest_report():
    files = [f for f in glob.glob("**/FacebookAdLibraryReport_*", recursive=True)
             if not f.startswith("data/") and f.lower().endswith((".zip", ".csv"))]
    if not files:
        return None
    def key(f):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(f))
        return (m.group(1) if m else "", f)
    return sorted(files, key=key)[-1]


def read_report(path):
    if path.lower().endswith(".zip"):
        with zipfile.ZipFile(path) as z:
            name = next(n for n in z.namelist() if n.lower().endswith("_advertisers.csv"))
            raw = pd.read_csv(z.open(name), dtype=str)
    else:
        raw = pd.read_csv(path, dtype=str)
    # Колонки беремо за порядком: так код не залежить від мови інтерфейсу звіту.
    df = raw.iloc[:, :5].copy()
    df.columns = ["page_id", "page_name", "disclaimer", "amount_usd", "ads_in_library"]
    df["amount_usd"] = df["amount_usd"].fillna("").str.strip()
    df["small"] = df["amount_usd"].str.startswith("≤")
    df["amount_num"] = pd.to_numeric(df["amount_usd"], errors="coerce").fillna(0)
    df["ads_in_library"] = pd.to_numeric(df["ads_in_library"], errors="coerce").fillna(0)
    rep = (df.groupby("page_id")
           .agg(page_name=("page_name", "last"),
                disclaimers=("disclaimer", lambda s: " | ".join(sorted(set(s.dropna())))),
                spend_usd_exact_part=("amount_num", "sum"),
                rows_under_100_usd=("small", "sum"),
                ads_in_library=("ads_in_library", "sum"))
           .reset_index())
    rep["rows_under_100_usd"] = rep["rows_under_100_usd"].astype(int)
    rep["ads_in_library"] = rep["ads_in_library"].astype(int)
    # Для сортування дрібні суми (≤100) рахуємо як 50.
    rep["sort_key"] = rep["spend_usd_exact_part"] + 50 * rep["rows_under_100_usd"]
    rep = rep.sort_values("sort_key", ascending=False).drop(columns="sort_key")
    rep["report_file"] = os.path.basename(path)
    return rep


# --------------------------------------------------------------- курси НБУ

def load_rates():
    return json.loads(RATES_FILE.read_text()) if RATES_FILE.exists() else {}


def fetch_nbu_day(day):
    try:
        r = requests.get(NBU_URL, params={"date": day.replace("-", ""), "json": ""}, timeout=60)
        data = r.json()
    except (requests.RequestException, ValueError):
        return None
    rates = {row["cc"]: float(row["rate"]) for row in data
             if isinstance(row, dict) and row.get("cc") and row.get("rate")}
    return rates or None


def rate_for(rates, cur, day, fetched):
    """Курс валюти до гривні на дату; якщо дня немає, шукаємо до 7 днів назад."""
    if cur == "UAH":
        return 1.0
    try:
        d = date.fromisoformat(day)
    except ValueError:
        return np.nan
    for back in range(8):
        key = (d - timedelta(days=back)).isoformat()
        if key not in rates and key not in fetched and key <= TODAY.isoformat():
            fetched.add(key)
            got = fetch_nbu_day(key)
            if got:
                rates[key] = got
        if cur in rates.get(key, {}):
            return rates[key][cur]
    return np.nan


# --------------------------------------------------------------- регіони

# Коди ISO 3166-2:UA, які розпізнає мапа Data Studio. Шаблони навмисно вузькі,
# бо Meta повертає й закордонні регіони (наприклад, Ivanovo Oblast у Росії).
# Шаблон — фрагмент або кілька фрагментів, що всі мають бути в назві. Порядок важливий.
REGION_RULES = [
    ("UA-40", "м. Севастополь", ["sevastop", "севастоп"]),
    ("UA-43", "Автономна Республіка Крим", ["crimea", "krym", "крим"]),
    ("UA-32", "Київська область", [("kyiv", "oblast"), ("kiev", "oblast"),
                                   ("kyiv", "region"), ("kiev", "region"),
                                   ("kyiv", "province"), ("kiev", "province"), "київськ"]),
    ("UA-30", "м. Київ", ["kyiv", "kiev", "київ"]),
    ("UA-05", "Вінницька область", ["vinnyts", "vinnits", "вінниц"]),
    ("UA-07", "Волинська область", ["volyn", "волин"]),
    ("UA-09", "Луганська область", ["luhansk", "lugansk", "луган"]),
    ("UA-12", "Дніпропетровська область", ["dnipro", "dnipropetrovsk", "dnepropetrovsk", "дніпр"]),
    ("UA-14", "Донецька область", ["donetsk", "донец"]),
    ("UA-18", "Житомирська область", ["zhytomyr", "zhitomir", "житомир"]),
    ("UA-21", "Закарпатська область", ["zakarpat", "transcarpath", "закарпат"]),
    ("UA-23", "Запорізька область", ["zaporiz", "zaporozh", "запоріз"]),
    ("UA-26", "Івано-Франківська область", [("ivano", "frank"), "івано-франків", "франківськ"]),
    ("UA-35", "Кіровоградська область", ["kirovohrad", "kirovograd", "kropyvnyts",
                                         "кіровоград", "кропивниц"]),
    ("UA-46", "Львівська область", ["lviv", "lvov", "львів"]),
    ("UA-48", "Миколаївська область", ["mykolaiv", "mykolayiv", "nikolaev", "nikolayev", "миколаїв"]),
    ("UA-51", "Одеська область", ["odesa", "odessa", "одес"]),
    ("UA-53", "Полтавська область", ["poltava", "полтав"]),
    ("UA-56", "Рівненська область", ["rivne", "rovno", "рівн"]),
    ("UA-59", "Сумська область", ["sumy", "sums'k", "сумськ", "суми"]),
    ("UA-61", "Тернопільська область", ["ternopil", "ternopol", "терноп"]),
    ("UA-63", "Харківська область", ["kharkiv", "kharkov", "харків"]),
    ("UA-65", "Херсонська область", ["kherson", "херсон"]),
    ("UA-68", "Хмельницька область", ["khmelnyts", "khmelnits", "хмельниц"]),
    ("UA-71", "Черкаська область", ["cherkas", "черкас"]),
    ("UA-77", "Чернівецька область", ["chernivts", "chernovts", "чернівец", "чернівц"]),
    ("UA-74", "Чернігівська область", ["chernihiv", "chernigov", "чернігів"]),
]
_region_cache = {}


def match_region(name):
    s = str(name or "").lower()
    if s not in _region_cache:
        _region_cache[s] = (OUTSIDE_CODE, OUTSIDE_NAME)
        for iso, name_uk, patterns in REGION_RULES:
            if any(all(part in s for part in ((p,) if isinstance(p, str) else p))
                   for p in patterns):
                _region_cache[s] = (iso, name_uk)
                break
    return _region_cache[s]


# --------------------------------------------------------------- таблиці

def to_num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return np.nan


def range_values(d):
    """Нижня межа, верхня межа, середина. Без верхньої межі середина = нижня межа."""
    if not isinstance(d, dict):
        return np.nan, np.nan, np.nan
    lo, hi = to_num(d.get("lower_bound")), to_num(d.get("upper_bound"))
    if np.isnan(lo):
        return lo, hi, np.nan
    return lo, hi, (lo if np.isnan(hi) else (lo + hi) / 2)


def join_texts(x, max_len=5000):
    if isinstance(x, list):
        x = " | ".join(str(t) for t in x if t)
    return str(x or "")[:max_len]


def build_tables(store, attribution, rates):
    fetched = set()
    ad_rows, region_rows, region_names = [], [], {}
    for ad in store.values():
        ad_id = ad["id"]
        page_id = str(ad.get("page_id", ""))
        day = ad_day(ad)
        cur = ad.get("currency", "") or ""
        s_lo, s_hi, s_mid = range_values(ad.get("spend"))
        i_lo, i_hi, i_mid = range_values(ad.get("impressions"))
        r_uah = rate_for(rates, cur, day, fetched) if cur else np.nan
        r_usd = rate_for(rates, "USD", day, fetched)
        spend_uah = s_mid * r_uah
        spend_usd = spend_uah / r_usd if r_usd else np.nan
        actor, actor_type = attribution.get(page_id, ("", ""))
        has_disclaimer = bool(str(ad.get("bylines") or "").strip())
        ad_rows.append({
            "ad_id": ad_id, "page_id": page_id, "page_name": ad.get("page_name", ""),
            "actor": actor, "actor_type": actor_type,
            "has_disclaimer": has_disclaimer, "bylines": ad.get("bylines", ""),
            "ad_creation_time": ad.get("ad_creation_time", ""),
            "delivery_start": ad.get("ad_delivery_start_time", ""),
            "delivery_stop": ad.get("ad_delivery_stop_time", ""),
            "is_active": not ad.get("ad_delivery_stop_time"),
            "currency": cur, "spend_lower": s_lo, "spend_upper": s_hi, "spend_mid": s_mid,
            "spend_uah_est": spend_uah, "spend_usd_est": spend_usd,
            "impressions_lower": i_lo, "impressions_upper": i_hi, "impressions_mid": i_mid,
            "platforms": ", ".join(ad.get("publisher_platforms") or []),
            "languages": ", ".join(ad.get("languages") or []),
            "ad_text": join_texts(ad.get("ad_creative_bodies"), 2000),  # повний текст — в архіві
            "link_title": join_texts(ad.get("ad_creative_link_titles"), 500),
            "ad_library_url": f"https://www.facebook.com/ads/library/?id={ad_id}",
            "channels": ", ".join(ad.get("_channels", [])),
            "first_seen": ad.get("_first_seen", ""),
            "no_regions": not ad.get("delivery_by_region"),
        })
        agg = {}
        for reg in ad.get("delivery_by_region") or []:
            share = to_num(reg.get("percentage"))
            if np.isnan(share):
                continue
            name = str(reg.get("region", ""))
            iso, name_uk = match_region(name)
            region_names[name] = region_names.get(name, 0) + 1
            a = agg.setdefault((iso, name_uk), 0.0)
            agg[(iso, name_uk)] = a + share
        for (iso, name_uk), share in agg.items():
            region_rows.append({
                "date": day, "page_id": page_id, "page_name": ad.get("page_name", ""),
                "actor": actor, "actor_type": actor_type, "has_disclaimer": has_disclaimer,
                "region_iso": iso, "region_uk": name_uk,
                "ads": 1, "spend_uah_est": spend_uah * share, "spend_usd_est": spend_usd * share,
                "impressions_est": i_mid * share,
            })
    ads_df = pd.DataFrame(ad_rows)
    keys = ["date", "page_id", "page_name", "actor", "actor_type", "has_disclaimer",
            "region_iso", "region_uk"]
    if region_rows:
        regions_df = (pd.DataFrame(region_rows)
                      .groupby(keys, dropna=False)
                      .agg(ads=("ads", "sum"), spend_uah_est=("spend_uah_est", "sum"),
                           spend_usd_est=("spend_usd_est", "sum"),
                           impressions_est=("impressions_est", "sum"))
                      .reset_index())
    else:
        regions_df = pd.DataFrame(columns=keys + ["ads", "spend_uah_est", "spend_usd_est",
                                                  "impressions_est"])
    return ads_df, regions_df, region_names


def build_pages(ads_df, report_df):
    pages = (ads_df.groupby("page_id")
             .agg(page_name=("page_name", "last"), actor=("actor", "last"),
                  actor_type=("actor_type", "last"),
                  has_disclaimer=("has_disclaimer", "max"), ads=("ad_id", "count"),
                  spend_uah_est=("spend_uah_est", "sum"), spend_usd_est=("spend_usd_est", "sum"),
                  first_start=("delivery_start", "min"), last_start=("delivery_start", "max"))
             .reset_index())
    if report_df is not None:
        rep = report_df[["page_id", "spend_usd_exact_part", "rows_under_100_usd"]].rename(
            columns={"spend_usd_exact_part": "report_spend_usd",
                     "rows_under_100_usd": "report_rows_under_100"})
        pages = pages.merge(rep, on="page_id", how="left")
    return pages.sort_values("spend_uah_est", ascending=False)


def build_region_map(region_names):
    rows = [{"region_meta": n, "region_iso": match_region(n)[0], "region_uk": match_region(n)[1],
             "occurrences": c} for n, c in region_names.items()]
    df = pd.DataFrame(rows, columns=["region_meta", "region_iso", "region_uk", "occurrences"])
    matched = df[df["region_iso"] != OUTSIDE_CODE].sort_values(["region_iso", "occurrences"],
                                                               ascending=[True, False])
    top_out = (df[df["region_iso"] == OUTSIDE_CODE]
               .sort_values("occurrences", ascending=False).head(TOP_UNMATCHED_REGIONS))
    return pd.concat([matched, top_out]), int((df["region_iso"] == OUTSIDE_CODE).sum())


# ------------------------------------------------------------ Google Sheets

def clean(v):
    if v is None:
        return ""
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, (float, np.floating)):
        return "" if np.isnan(v) else round(float(v), 4)
    return v


def df_to_values(df):
    body = [[clean(v) for v in row] for row in df.astype(object).itertuples(index=False)]
    return [list(df.columns)] + body


def write_tab(sh, title, df, max_rows=5000, max_bytes=4_000_000):
    """Перезаписує вкладку повністю. RAW: ID лишаються текстом, тексти не стають формулами.
    Дані надсилаються порціями, щоб не перевищити ліміт розміру запиту Google."""
    import gspread
    values = df_to_values(df)
    n_rows, n_cols = max(len(values), 2), max(len(values[0]), 1)
    try:
        ws = sh.worksheet(title)
        ws.clear()
        ws.resize(rows=n_rows, cols=n_cols)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=n_rows, cols=n_cols)
    start, chunk, size = 0, [], 0
    for row in values:
        row_size = len(json.dumps(row, ensure_ascii=False).encode("utf-8"))
        if chunk and (len(chunk) >= max_rows or size + row_size > max_bytes):
            ws.update(range_name=f"A{start + 1}", values=chunk, value_input_option="RAW")
            start, chunk, size = start + len(chunk), [], 0
        chunk.append(row)
        size += row_size
    if chunk:
        ws.update(range_name=f"A{start + 1}", values=chunk, value_input_option="RAW")
    log(f"Вкладка «{title}»: рядків {len(df)}")


ATTR_TAB = "атрибуція"
ATTR_HEADERS = ["page_id", "page_name", "актор", "тип_актора",
                "підстава_атрибуції", "перевірено", "коментар"]


def read_attribution(sh):
    import gspread
    try:
        rows = sh.worksheet(ATTR_TAB).get_all_values()
    except gspread.WorksheetNotFound:
        return {}
    out = {}
    for r in rows[1:]:
        r = (r + [""] * 4)[:4]
        if r[0].strip():
            out[r[0].strip()] = (r[2].strip(), r[3].strip())
    return out


def update_attribution(sh, pages_df):
    """Вкладку атрибуції ніколи не перезаписуємо: лише дописуємо нові сторінки."""
    import gspread
    try:
        ws = sh.worksheet(ATTR_TAB)
        existing = set(v.strip() for v in ws.col_values(1)[1:])
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=ATTR_TAB, rows=1000, cols=len(ATTR_HEADERS))
        ws.update(range_name="A1", values=[ATTR_HEADERS], value_input_option="RAW")
        existing = set()
    new = pages_df[~pages_df["page_id"].isin(existing)]
    if len(new):
        ws.append_rows([[r.page_id, r.page_name] + [""] * (len(ATTR_HEADERS) - 2)
                        for r in new.itertuples(index=False)], value_input_option="RAW")
    log(f"Вкладка «{ATTR_TAB}»: додано нових сторінок {len(new)}")


def open_sheet():
    import gspread
    creds = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    gc = gspread.service_account_from_dict(creds)
    return gc.open_by_url(os.environ["SHEET_URL"].strip())


# ------------------------------------------------------------------ запуск

def main():
    token = os.environ.get("META_TOKEN", "").strip()
    if not token:
        sys.exit("Немає секрету META_TOKEN.")
    store = load_store()
    log(f"В архіві оголошень: {len(store)}")
    problems, qa = [], []

    def add(k, v):
        qa.append({"показник": k, "значення": v})

    # 1. Канал пошуку
    date_from = os.environ.get("DATE_FROM", "").strip()
    date_to = os.environ.get("DATE_TO", "").strip() or TODAY.isoformat()
    if not date_from:
        date_from = BACKFILL_FROM if not store else (TODAY - timedelta(days=WINDOW_DAYS)).isoformat()
    log(f"Канал пошуку: {date_from} — {date_to}")
    try:
        search_ads, search_ok = fetch_ads(token, {"search_terms": "''"}, date_from, date_to,
                                          label="[пошук]")
    except FatalApiError as e:
        log(str(e))
        sys.exit(1)  # таблицю не чіпаємо: у ній лишаються попередні дані
    touched = merge_into_store(store, search_ads, "search")
    search_ids = {str(a.get("id")) for a in search_ads}
    if not search_ok:
        problems.append("канал пошуку завантажено не повністю")
    log(f"Канал пошуку: оголошень {len(search_ads)}" + ("" if search_ok else " (НЕПОВНО)"))

    # 2. Канал сторінок
    report_path = find_latest_report()
    report_df = read_report(report_path) if report_path else None
    mode = os.environ.get("PAGE_CHANNEL", "auto").strip().lower() or "auto"
    run_pages = report_df is not None and (
        mode == "yes" or (mode == "auto" and TODAY.weekday() == PAGE_CHANNEL_WEEKDAY))
    page_stats = "не запускався"
    if run_pages:
        p_from = (TODAY - timedelta(days=PAGE_WINDOW_DAYS)).isoformat()
        ids = [p for p in report_df["page_id"] if p and p != "0"]
        batches = [ids[i:i + 10] for i in range(0, len(ids), 10)][:PAGE_CHANNEL_MAX_REQUESTS]
        page_ads, page_ok = [], True
        log(f"Канал сторінок: {len(batches)} запитів, {p_from} — {TODAY}")
        try:
            for n, batch in enumerate(batches, 1):
                got, ok = fetch_ads(token, {"search_page_ids": ",".join(batch)},
                                    p_from, TODAY.isoformat(), label=f"[сторінки {n}]")
                page_ads.extend(got)
                page_ok = page_ok and ok
        except FatalApiError as e:
            log(str(e))
            page_ok = False
            problems.append("канал сторінок зупинено: токен")
        touched |= merge_into_store(store, page_ads, "pages")
        page_ids = {str(a.get("id")) for a in page_ads}
        # Оголошення, які знайшов лише канал сторінок і жодного разу не знайшов пошук.
        missed = sum(1 for i in page_ids if "search" not in store[i].get("_channels", []))
        page_stats = (f"сторінок {sum(len(b) for b in batches)} із {len(ids)}, "
                      f"оголошень {len(page_ids)}, з них не знайдених пошуком: {missed}")
        if not page_ok:
            problems.append("канал сторінок завантажено не повністю")
        log("Канал сторінок: " + page_stats)

    save_store(store, touched)

    # 3. Таблиці
    sh = open_sheet()
    attribution = read_attribution(sh)
    rates = load_rates()
    ads_df, regions_df, region_names = build_tables(store, attribution, rates)
    if ads_df.empty:
        log("В архіві немає жодного оголошення, таблицю не змінено.")
        sys.exit(1)
    RATES_FILE.parent.mkdir(parents=True, exist_ok=True)
    RATES_FILE.write_text(json.dumps(rates, ensure_ascii=False, sort_keys=True, indent=0))
    pages_df = build_pages(ads_df, report_df)
    region_map_df, n_unmatched = build_region_map(region_names)

    add("Останній запуск", RUN_AT)
    add("Статус", "OK" if not problems else "НЕПОВНО: " + "; ".join(problems))
    add("Канал пошуку, період", f"{date_from} — {date_to}")
    add("Канал пошуку, оголошень", len(search_ads))
    add("Канал сторінок", page_stats)
    add("Звіт Meta у репозиторії", os.path.basename(report_path) if report_path else "немає")
    add("Оголошень в архіві", len(ads_df))
    add("Сторінок в архіві", int(ads_df["page_id"].nunique()))
    add("Оголошень з дисклеймером", int(ads_df["has_disclaimer"].sum()))
    add("Оголошень без дисклеймера", int((~ads_df["has_disclaimer"]).sum()))
    add("Оголошень без даних про витрати", int(ads_df["spend_mid"].isna().sum()))
    add("Оголошень без даних про регіони", int(ads_df["no_regions"].sum()))
    add("Оголошень без курсу НБУ", int((ads_df["spend_mid"].notna()
                                      & ads_df["spend_uah_est"].isna()).sum()))
    for cur, n in ads_df["currency"].replace("", "(не вказано)").value_counts().items():
        add(f"Валюта {cur}, оголошень", int(n))
    add("Назв регіонів поза Україною або нерозпізнаних", n_unmatched)
    qa_df = pd.DataFrame(qa)

    write_tab(sh, "перевірка", qa_df)
    write_tab(sh, "оголошення", ads_df.sort_values("delivery_start", ascending=False))
    write_tab(sh, "регіони", regions_df)
    write_tab(sh, "сторінки", pages_df)
    write_tab(sh, "відповідність_регіонів", region_map_df)
    if report_df is not None:
        write_tab(sh, "звіт_meta", report_df)
    update_attribution(sh, pages_df[["page_id", "page_name"]])

    if problems:
        log("Запуск завершено з проблемами: " + "; ".join(problems))
        sys.exit(1)
    log("Готово.")


if __name__ == "__main__":
    main()
