"""
Щоденний збір політичної реклами Meta для дашборду ОПОРИ.

Що робить скрипт за один запуск:
  1. Канал пошуку: забирає з Ad Library API всю політичну рекламу, показану
     в Україні за останні WINDOW_DAYS днів, або — під час дозавантаження —
     за період, заданий у полях запуску (помісячно, з продовженням після перерви).
  2. Канал сторінок (раз на тиждень): забирає оголошення сторінок зі звіту
     Ad Library Report, який лежить у репозиторії, щоб перевірити повноту.
  3. Зберігає сирі дані в архіві (папка ARCHIVE_DIR, гілка archive репозиторію).
  4. Перераховує витрати в гривні й долари за курсом НБУ.
  5. Оновлює робочі вкладки Google-таблиці за останні SHEETS_MONTHS місяців;
     вкладку «атрибуція» лише доповнює.

Архів обробляється файл за файлом, тож скрипт не тримає в пам'яті всі
оголошення одночасно і працює навіть із сотнями тисяч оголошень.
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
COVERED_FROM = "2026-01"         # з якого місяця архів повний без дозавантаження
SHEETS_MONTHS = 12               # скільки останніх місяців показувати в Google-таблиці
REGIONS_PERIOD = "week"          # деталізація вкладки «регіони»: "day", "week" або "month"
PAGE_WINDOW_DAYS = 35            # канал сторінок: оголошення за останні N днів
PAGE_CHANNEL_WEEKDAY = 0         # у який день тижня вмикати канал сторінок (0 = понеділок)
PAGE_CHANNEL_MAX_REQUESTS = 400  # максимум запитів каналу сторінок (по 10 сторінок у запиті)
TIME_BUDGET_MIN = 270            # після скількох хвилин дозавантаження зупиняється до наступного запуску
TOP_UNMATCHED_REGIONS = 50       # скільки незіставлених назв регіонів показувати в перевірці
# ==========================================================================

API_URL = "https://graph.facebook.com/ads_archive"
NBU_URL = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange"
DATA_DIR = Path(os.environ.get("ARCHIVE_DIR", "data"))
RAW_DIR = DATA_DIR / "raw"
RATES_FILE = DATA_DIR / "nbu_rates.json"
LAST_SUCCESS_FILE = DATA_DIR / "last_update.txt"
COVERAGE_FILE = DATA_DIR / "coverage.json"
OUTSIDE_CODE, OUTSIDE_NAME = "OUT", "поза Україною або не розпізнано"
UNKNOWN_CODE, UNKNOWN_NAME = "UNK", "регіон невідомий"

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
STARTED = time.monotonic()


def log(msg):
    print(msg, flush=True)


def minutes_used():
    return (time.monotonic() - STARTED) / 60


# ------------------------------------------------------------------ Meta API

class FatalApiError(Exception):
    """Помилка, після якої немає сенсу продовжувати (наприклад, токен сплив)."""


class RateLimitStop(Exception):
    """Meta довго не знімає обмеження на кількість запитів: зупиняємо збір до наступного запуску."""


# Паузи після відповіді Meta «перевищено ліміт запитів», у секундах. Обмеження Meta
# діє в межах години, тож чекаємо поступово довше, але не залишаємо місяць незавершеним.
RATE_LIMIT_WAITS = [120, 300, 600, 900, 1200, 1800, 1800, 1800]


def throttle(resp):
    """Meta повідомляє, яку частку годинного ліміту вже використано (заголовок x-app-usage).
    Сповільнюємося заздалегідь, щоб не впертися в ліміт."""
    raw = resp.headers.get("x-app-usage") or resp.headers.get("X-App-Usage")
    if not raw:
        return
    try:
        usage = json.loads(raw)
        pct = max(float(usage.get(k, 0)) for k in ("call_count", "total_time", "total_cputime"))
    except (ValueError, TypeError, AttributeError):
        return
    if pct >= 95:
        log(f"    використано {pct:.0f}% годинного ліміту Meta, пауза 10 хв…")
        time.sleep(600)
    elif pct >= 85:
        time.sleep(120)
    elif pct >= 70:
        time.sleep(20)


def fetch_ads(token, extra_params, date_from, date_to, on_batch, limit=250, max_retries=5, label=""):
    """Забирає всі порції відповіді API і передає кожну в on_batch.
    Повертає (кількість оголошень, чи повне завантаження)."""
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
    total, retries = 0, 0
    while True:
        try:
            resp = requests.get(API_URL, params=params, timeout=180)
            payload = resp.json()
        except (requests.RequestException, ValueError) as e:
            retries += 1
            if retries > max_retries:
                log(f"{label} мережева помилка, зупиняюся: {type(e).__name__}")
                return total, False
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
            if code in RATE_LIMIT_CODES:
                # Позицію в списку (курсор) зберігаємо: після паузи продовжуємо з того самого місця.
                if retries < len(RATE_LIMIT_WAITS) and \
                        minutes_used() + RATE_LIMIT_WAITS[retries] / 60 < TIME_BUDGET_MIN:
                    wait = RATE_LIMIT_WAITS[retries]
                    retries += 1
                    log(f"{label} Meta обмежила кількість запитів, чекаю {wait // 60} хв…")
                    time.sleep(wait)
                    continue
                raise RateLimitStop(
                    "Meta обмежила кількість запитів і не зняла обмеження після тривалих пауз")
            details = " | ".join(str(err[k]) for k in
                                 ("error_subcode", "error_user_title", "error_user_msg") if err.get(k))
            log(f"{label} помилка API (код {code}): {msg} {details}".strip())
            return total, False

        retries = 0
        throttle(resp)
        batch = payload.get("data", [])
        total += len(batch)
        if batch:
            on_batch(batch)
        paging = payload.get("paging", {})
        after = paging.get("cursors", {}).get("after")
        if not paging.get("next") or not after:
            return total, True
        params["after"] = after


# ------------------------------------------------------------ сирий архів

def ad_day(ad):
    d = str(ad.get("ad_delivery_start_time") or ad.get("ad_creation_time") or "")[:10]
    return d if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) else "unknown"


def raw_path(day):
    if day == "unknown":
        return RAW_DIR / "unknown.jsonl.gz"
    return RAW_DIR / day[:4] / day[5:7] / f"{day}.jsonl.gz"


def read_file(path):
    ads = {}
    if path.exists():
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    ad = json.loads(line)
                    ads[str(ad["id"])] = ad
    return ads


def write_file(path, ads):
    """Детерміноване стиснення: незмінені дані дають ідентичні файли."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(ads[k], ensure_ascii=False, sort_keys=True) for k in sorted(ads)]
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(("\n".join(lines) + "\n").encode("utf-8"))
    data = buf.getvalue()
    if not path.exists() or path.read_bytes() != data:
        path.write_bytes(data)


def archive_files():
    return sorted(RAW_DIR.rglob("*.jsonl.gz"))


def iter_archive():
    """Оголошення архіву по одному, файл за файлом."""
    for f in archive_files():
        yield from read_file(f).values()


class Merger:
    """Зводить нові оголошення з архівом. Нова версія замінює стару (свіжіші витрати),
    канали накопичуються. Тримає в пам'яті лише файли днів, яких торкнулися."""

    def __init__(self):
        self.pending = {}
        self.new_ads = 0

    def merge(self, ads, channel):
        merged = []
        for ad in ads:
            ad_id = str(ad.get("id", ""))
            if not ad_id:
                continue
            day = ad_day(ad)
            if day not in self.pending:
                self.pending[day] = read_file(raw_path(day))
            old = self.pending[day].get(ad_id)
            if old is None:
                # оголошення могло змінити дату початку — шукаємо його лише в цьому дні
                self.new_ads += 1
                old = {}
            new = dict(ad)
            new["id"] = ad_id
            new["_channels"] = sorted(set(old.get("_channels", [])) | {channel})
            new["_first_seen"] = old.get("_first_seen", RUN_AT)
            self.pending[day][ad_id] = new
            merged.append(new)
        return merged

    def flush(self):
        for day, ads in self.pending.items():
            write_file(raw_path(day), ads)
        self.pending = {}


# ------------------------------------------------------- покриття архіву

def month_key(d):
    return d[:7]


def all_months(start, end):
    y, m = int(start[:4]), int(start[5:7])
    out = []
    while f"{y:04d}-{m:02d}" <= end:
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def load_coverage():
    if COVERAGE_FILE.exists():
        return set(json.loads(COVERAGE_FILE.read_text())["months_done"])
    return set()


def save_coverage(done):
    COVERAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    COVERAGE_FILE.write_text(json.dumps({"months_done": sorted(done)}, indent=0))


def covered_from(done):
    """Найраніший місяць, починаючи з якого архів повний до сьогодні."""
    months = all_months("2018-01", month_key(TODAY.isoformat()))
    start = None
    for m in reversed(months):
        if m in done or m >= COVERED_FROM:
            start = m
        else:
            break
    return start or COVERED_FROM


def month_ranges(date_from, date_to):
    """Розбиває період на календарні місяці."""
    out = []
    for m in all_months(date_from[:7], date_to[:7]):
        first = date.fromisoformat(m + "-01")
        nxt = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
        start = max(first.isoformat(), date_from)
        end = min((nxt - timedelta(days=1)).isoformat(), date_to)
        full = start == first.isoformat() and end == (nxt - timedelta(days=1)).isoformat()
        out.append((m, start, end, full))
    return out


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
    if s.strip() == "unknown":
        return UNKNOWN_CODE, UNKNOWN_NAME
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


def period_start(day):
    """Дата початку тижня (понеділок) або місяця для вкладки «регіони»."""
    try:
        d = date.fromisoformat(day)
    except ValueError:
        return day
    if REGIONS_PERIOD == "week":
        d = d - timedelta(days=d.weekday())
    elif REGIONS_PERIOD == "month":
        d = d.replace(day=1)
    return d.isoformat()



def ad_values(ad, rates, fetched):
    """Похідні значення одного оголошення: дата, валюта, оцінки витрат у грн і дол., регіони."""
    day = ad_day(ad)
    cur = ad.get("currency", "") or ""
    s_lo, s_hi, s_mid = range_values(ad.get("spend"))
    i_lo, i_hi, i_mid = range_values(ad.get("impressions"))
    r_uah = rate_for(rates, cur, day, fetched) if cur else np.nan
    r_usd = rate_for(rates, "USD", day, fetched)
    uah = s_mid * r_uah
    usd = uah / r_usd if r_usd else np.nan
    shares = {}
    for reg in ad.get("delivery_by_region") or []:
        share = to_num(reg.get("percentage"))
        if not np.isnan(share):
            key = match_region(reg.get("region"))
            shares[key] = shares.get(key, 0.0) + share
    return {"day": day, "cur": cur, "s": (s_lo, s_hi, s_mid), "i": (i_lo, i_hi, i_mid),
            "uah": uah, "usd": usd, "shares": shares,
            "disc": bool(str(ad.get("bylines") or "").strip())}


def build_tables(attribution, rates):
    """Один прохід архівом: вкладки за останні SHEETS_MONTHS місяців і підсумки за весь час."""
    fetched = set()
    cutoff_month = all_months("2000-01", month_key(TODAY.isoformat()))[-SHEETS_MONTHS]
    ad_rows, region_agg, region_names, pages, qa = [], {}, {}, {}, {
        "ads": 0, "disc": 0, "no_spend": 0, "no_rate": 0, "no_regions": 0, "currencies": {}}
    seen = set()
    for ad in iter_archive():
        if ad["id"] in seen:
            continue
        seen.add(ad["id"])
        v = ad_values(ad, rates, fetched)
        page_id = str(ad.get("page_id", ""))
        actor, actor_type = attribution.get(page_id, ("", ""))
        qa["ads"] += 1
        qa["disc"] += v["disc"]
        qa["no_spend"] += int(np.isnan(v["s"][2]))
        qa["no_rate"] += int(not np.isnan(v["s"][2]) and np.isnan(v["uah"]))
        qa["no_regions"] += int(not v["shares"])
        cur = v["cur"] or "(не вказано)"
        qa["currencies"][cur] = qa["currencies"].get(cur, 0) + 1
        for reg in ad.get("delivery_by_region") or []:
            name = str(reg.get("region", ""))
            region_names[name] = region_names.get(name, 0) + 1
        pg = pages.setdefault(page_id, {"page_id": page_id, "page_name": "", "actor": actor,
                                        "actor_type": actor_type, "has_disclaimer": False, "ads": 0,
                                        "spend_uah_est": 0.0, "spend_usd_est": 0.0,
                                        "first_start": "9999", "last_start": ""})
        if v["day"] >= pg["last_start"]:
            pg["page_name"] = ad.get("page_name", "")
        pg["has_disclaimer"] |= v["disc"]
        pg["ads"] += 1
        pg["spend_uah_est"] += 0.0 if np.isnan(v["uah"]) else v["uah"]
        pg["spend_usd_est"] += 0.0 if np.isnan(v["usd"]) else v["usd"]
        if v["day"] != "unknown":
            pg["first_start"] = min(pg["first_start"], v["day"])
            pg["last_start"] = max(pg["last_start"], v["day"])

        if v["day"] == "unknown" or month_key(v["day"]) < cutoff_month:
            continue
        ad_id = ad["id"]
        ad_rows.append({
            "ad_id": ad_id, "page_id": page_id, "page_name": ad.get("page_name", ""),
            "actor": actor, "actor_type": actor_type,
            "has_disclaimer": v["disc"], "bylines": ad.get("bylines", ""),
            "ad_creation_time": ad.get("ad_creation_time", ""),
            "delivery_start": ad.get("ad_delivery_start_time", ""),
            "delivery_stop": ad.get("ad_delivery_stop_time", ""),
            "is_active": not ad.get("ad_delivery_stop_time"),
            "currency": v["cur"], "spend_lower": v["s"][0], "spend_upper": v["s"][1],
            "spend_mid": v["s"][2], "spend_uah_est": v["uah"], "spend_usd_est": v["usd"],
            "impressions_lower": v["i"][0], "impressions_upper": v["i"][1],
            "impressions_mid": v["i"][2],
            "platforms": ", ".join(ad.get("publisher_platforms") or []),
            "languages": ", ".join(ad.get("languages") or []),
            "ad_text": join_texts(ad.get("ad_creative_bodies"), 2000),  # повний текст — в архіві
            "link_title": join_texts(ad.get("ad_creative_link_titles"), 500),
            "ad_library_url": f"https://www.facebook.com/ads/library/?id={ad_id}",
            "channels": ", ".join(ad.get("_channels", [])),
            "first_seen": ad.get("_first_seen", ""),
        })
        period = period_start(v["day"])
        for (iso, name_uk), share in v["shares"].items():
            key = (period, page_id, v["disc"], iso, name_uk)
            agg = region_agg.setdefault(key, [ad.get("page_name", ""), actor, actor_type, 0, 0.0, 0.0, 0.0])
            agg[3] += 1
            agg[4] += 0.0 if np.isnan(v["uah"]) else v["uah"] * share
            agg[5] += 0.0 if np.isnan(v["usd"]) else v["usd"] * share
            agg[6] += 0.0 if np.isnan(v["i"][2]) else v["i"][2] * share

    ads_df = pd.DataFrame(ad_rows)
    regions_df = pd.DataFrame(
        [{"date": k[0], "page_id": k[1], "page_name": a[0], "actor": a[1], "actor_type": a[2],
          "has_disclaimer": k[2], "region_iso": k[3], "region_uk": k[4], "ads": a[3],
          "spend_uah_est": a[4], "spend_usd_est": a[5], "impressions_est": a[6]}
         for k, a in region_agg.items()])
    pages_df = pd.DataFrame(list(pages.values()))
    if not pages_df.empty:
        pages_df["first_start"] = pages_df["first_start"].replace("9999", "")
    return ads_df, regions_df, pages_df, region_names, qa, cutoff_month


def merge_report(pages_df, report_df):
    if report_df is not None and not pages_df.empty:
        rep = report_df[["page_id", "spend_usd_exact_part", "rows_under_100_usd"]].rename(
            columns={"spend_usd_exact_part": "report_spend_usd",
                     "rows_under_100_usd": "report_rows_under_100"})
        pages_df = pages_df.merge(rep, on="page_id", how="left")
    return pages_df.sort_values("spend_uah_est", ascending=False) if not pages_df.empty else pages_df


def build_region_map(region_names):
    rows = [{"region_meta": n, "region_iso": match_region(n)[0], "region_uk": match_region(n)[1],
             "occurrences": c} for n, c in region_names.items()]
    df = pd.DataFrame(rows, columns=["region_meta", "region_iso", "region_uk", "occurrences"])
    matched = df[~df["region_iso"].isin([OUTSIDE_CODE])].sort_values(
        ["region_iso", "occurrences"], ascending=[True, False])
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


_last_write = [0.0]


def sheets_call(fn, *args, **kwargs):
    """Виклик Google Sheets з паузою між записами (ліміт Google — 60 записів на хвилину)
    і повтором після помилки 429 «перевищено квоту» чи тимчасових збоїв."""
    import gspread
    for attempt in range(8):
        wait = 1.2 - (time.monotonic() - _last_write[0])
        if wait > 0:
            time.sleep(wait)
        try:
            result = fn(*args, **kwargs)
            _last_write[0] = time.monotonic()
            return result
        except gspread.exceptions.APIError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (429, 500, 502, 503) and attempt < 7:
                log(f"Google Sheets: код {status}, чекаю 65 с і повторюю…")
                time.sleep(65)
                _last_write[0] = time.monotonic()
                continue
            raise


def write_tab(sh, title, df, max_rows=20000, max_bytes=4_000_000):
    """Перезаписує вкладку повністю. RAW: ID лишаються текстом, тексти не стають формулами.
    Дані надсилаються порціями, щоб не перевищити ліміт розміру запиту Google."""
    import gspread
    values = df_to_values(df)
    n_rows, n_cols = max(len(values), 2), max(len(values[0]), 1)
    try:
        ws = sh.worksheet(title)
        sheets_call(ws.clear)
        sheets_call(ws.resize, rows=n_rows, cols=n_cols)
    except gspread.WorksheetNotFound:
        ws = sheets_call(sh.add_worksheet, title=title, rows=n_rows, cols=n_cols)
    start, chunk, size = 0, [], 0
    for row in values:
        row_size = len(json.dumps(row, ensure_ascii=False).encode("utf-8"))
        if chunk and (len(chunk) >= max_rows or size + row_size > max_bytes):
            sheets_call(ws.update, range_name=f"A{start + 1}", values=chunk,
                        value_input_option="RAW")
            start, chunk, size = start + len(chunk), [], 0
        chunk.append(row)
        size += row_size
    if chunk:
        sheets_call(ws.update, range_name=f"A{start + 1}", values=chunk,
                    value_input_option="RAW")
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
        ws = sheets_call(sh.add_worksheet, title=ATTR_TAB, rows=1000, cols=len(ATTR_HEADERS))
        sheets_call(ws.update, range_name="A1", values=[ATTR_HEADERS], value_input_option="RAW")
        existing = set()
    new = pages_df[~pages_df["page_id"].isin(existing)]
    if len(new):
        rows = [[r.page_id, r.page_name] + [""] * (len(ATTR_HEADERS) - 2)
                for r in new.itertuples(index=False)]
        for i in range(0, len(rows), 20000):
            sheets_call(ws.append_rows, rows[i:i + 20000], value_input_option="RAW")
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
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    done = load_coverage()
    problems = []
    merger = Merger()

    # 1. Канал пошуку: звичайний режим або дозавантаження за період
    date_from = os.environ.get("DATE_FROM", "").strip()
    date_to = os.environ.get("DATE_TO", "").strip() or TODAY.isoformat()
    backfill = bool(date_from)
    if not backfill:
        date_from = (TODAY - timedelta(days=WINDOW_DAYS)).isoformat()
    ranges = month_ranges(date_from, date_to)
    if backfill:
        skipped = [m for m, _, _, full in ranges if full and m in done]
        ranges = [r for r in ranges if not (r[3] and r[0] in done)]
        if skipped:
            log(f"Уже завантажені місяці пропущено: {', '.join(skipped)}")
    log(f"Канал пошуку: {date_from} — {date_to}" + (" (дозавантаження)" if backfill else ""))

    search_total, stopped_early, rate_stopped = 0, False, False
    try:
        for m, start, end, full in ranges:
            if backfill and minutes_used() > TIME_BUDGET_MIN:
                stopped_early = True
                break
            got, ok = fetch_ads(token, {"search_terms": "''"}, start, end,
                                lambda b: merger.merge(b, "search"), label=f"[пошук {m}]")
            merger.flush()
            search_total += got
            log(f"  {m}: оголошень {got}" + ("" if ok else " (НЕПОВНО)"))
            if not ok:
                problems.append(f"канал пошуку за {m} завантажено не повністю")
            elif backfill and full:
                done.add(m)
                save_coverage(done)
    except RateLimitStop as e:
        merger.flush()
        save_coverage(done)
        rate_stopped = True
        log(str(e))
        problems.append("Meta обмежила кількість запитів — запустіть ще раз приблизно за годину"
                        + (" із тим самим періодом, завантажені місяці буде пропущено" if backfill else ""))
    except FatalApiError as e:
        merger.flush()
        save_coverage(done)
        log(str(e))
        sys.exit(1)  # таблицю не чіпаємо: у ній лишаються попередні дані
    if stopped_early:
        problems.append("дозавантаження зупинено за часом — запустіть ще раз із тим самим періодом, "
                        "завантажені місяці буде пропущено")
    log(f"Канал пошуку: оголошень {search_total}, нових в архіві {merger.new_ads}")

    # 2. Канал сторінок
    report_path = find_latest_report()
    report_df = read_report(report_path) if report_path else None
    mode = os.environ.get("PAGE_CHANNEL", "auto").strip().lower() or "auto"
    run_pages = report_df is not None and not backfill and not rate_stopped and (
        mode == "yes" or (mode == "auto" and TODAY.weekday() == PAGE_CHANNEL_WEEKDAY))
    page_stats = "не запускався"
    if run_pages:
        p_from = (TODAY - timedelta(days=PAGE_WINDOW_DAYS)).isoformat()
        ids = [p for p in report_df["page_id"] if p and p != "0"]
        batches = [ids[i:i + 10] for i in range(0, len(ids), 10)][:PAGE_CHANNEL_MAX_REQUESTS]
        page_ok, found, missed = True, set(), set()

        def on_pages(batch):
            for rec in merger.merge(batch, "pages"):
                found.add(rec["id"])
                if "search" not in rec["_channels"]:
                    missed.add(rec["id"])

        log(f"Канал сторінок: {len(batches)} запитів, {p_from} — {TODAY}")
        try:
            for n, batch in enumerate(batches, 1):
                _, ok = fetch_ads(token, {"search_page_ids": ",".join(batch)},
                                  p_from, TODAY.isoformat(), on_pages, label=f"[сторінки {n}]")
                page_ok = page_ok and ok
                if n % 50 == 0:
                    merger.flush()
        except FatalApiError as e:
            log(str(e))
            page_ok = False
            problems.append("канал сторінок зупинено: токен")
        except RateLimitStop as e:
            log(str(e))
            page_ok = False
            problems.append("канал сторінок зупинено: ліміт запитів Meta")
        merger.flush()
        page_stats = (f"сторінок {sum(len(b) for b in batches)} із {len(ids)}, "
                      f"оголошень {len(found)}, з них не знайдених пошуком: {len(missed)}")
        if not page_ok:
            problems.append("канал сторінок завантажено не повністю")
        log("Канал сторінок: " + page_stats)

    save_coverage(done)
    if search_total:
        LAST_SUCCESS_FILE.write_text(RUN_AT + "\n")

    # 3. Таблиці
    sh = open_sheet()
    attribution = read_attribution(sh)
    rates = load_rates()
    ads_df, regions_df, pages_df, region_names, qa_counts, cutoff_month = build_tables(attribution, rates)
    RATES_FILE.write_text(json.dumps(rates, ensure_ascii=False, sort_keys=True, indent=0))
    if not qa_counts["ads"]:
        log("В архіві немає жодного оголошення, таблицю не змінено.")
        sys.exit(1)
    pages_df = merge_report(pages_df, report_df)
    region_map_df, n_unmatched = build_region_map(region_names)

    qa = []

    def add(k, v):
        qa.append({"показник": k, "значення": v})

    add("Останній запуск", RUN_AT)
    add("Статус", "OK" if not problems else "НЕПОВНО: " + "; ".join(problems))
    add("Канал пошуку, період", f"{date_from} — {date_to}" + (" (дозавантаження)" if backfill else ""))
    add("Канал пошуку, оголошень", search_total)
    add("Нових оголошень в архіві за запуск", merger.new_ads)
    add("Канал сторінок", page_stats)
    add("Звіт Meta у репозиторії", os.path.basename(report_path) if report_path else "немає")
    add("Архів повний з місяця", covered_from(done))
    add("Оголошень в архіві (усього)", qa_counts["ads"])
    add("Сторінок в архіві (усього)", len(pages_df))
    add("Оголошень з дисклеймером", qa_counts["disc"])
    add("Оголошень без дисклеймера", qa_counts["ads"] - qa_counts["disc"])
    add("Оголошень без даних про витрати", qa_counts["no_spend"])
    add("Оголошень без даних про регіони", qa_counts["no_regions"])
    add("Оголошень без курсу НБУ", qa_counts["no_rate"])
    for cur, n in sorted(qa_counts["currencies"].items(), key=lambda x: -x[1]):
        add(f"Валюта {cur}, оголошень", n)
    add("Назв регіонів поза Україною або нерозпізнаних", n_unmatched)
    add("Вкладки «оголошення» і «регіони» показують дані з місяця", cutoff_month)
    qa_df = pd.DataFrame(qa)

    write_tab(sh, "перевірка", qa_df)
    write_tab(sh, "оголошення", ads_df.sort_values("delivery_start", ascending=False)
              if not ads_df.empty else ads_df)
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
