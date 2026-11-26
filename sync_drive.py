"""
Вивантаження архіву на спільний диск ОПОРИ (Google Drive).

Структура на диску:
  архів_jsonl/<рік>/<рік>-<місяць>.jsonl.gz   сирі відповіді API, по файлу на місяць;
  таблиці_csv/оголошення_<рік>.csv            одне оголошення в рядку, з повним текстом;
  таблиці_csv/регіони_<рік>.csv               витрати за місяцем, сторінкою й областю;
  таблиці_csv/сторінки.csv                    підсумки за рекламодавцями за весь архів;
  ПРОЧИТАЙ_МЕНЕ.txt                           опис файлів і полів.

CSV-файли збережено в UTF-8 з позначкою BOM, тож Excel одразу показує кирилицю.
На диск вивантажуються лише ті файли, що змінилися з попереднього запуску
(перелік і контрольні суми — у файлі drive_manifest.json в архіві).
"""

import csv
import gzip
import hashlib
import io
import json
import math
import os
import sys
import tempfile
import time
from pathlib import Path

import collect as C

DRIVE_ID = os.environ.get("DRIVE_ID", "").strip()
MANIFEST_FILE = C.DATA_DIR / "drive_manifest.json"
API = "https://www.googleapis.com/drive/v3/files"
UPLOAD = "https://www.googleapis.com/upload/drive/v3/files"
FOLDER = "application/vnd.google-apps.folder"
COMMON = {"supportsAllDrives": "true"}

README_TEXT = """Архів політичної реклами Meta в Україні — Громадянська мережа ОПОРА

Файли оновлюються автоматично щодня. Не редагуйте їх тут: зміни буде перезаписано.

архів_jsonl/ — сирі дані з Ad Library API, по файлу на місяць початку показу оголошення.
  Формат JSON Lines, стиснений gzip: кожен рядок — одне оголошення. Службові поля:
  _channels — яким каналом знайдено оголошення (search — пошук, pages — канал сторінок),
  _first_seen — коли оголошення вперше потрапило в архів.

таблиці_csv/оголошення_<рік>.csv — одне оголошення в рядку.
  spend_lower / spend_upper — діапазон витрат від Meta у валюті оголошення (currency);
  spend_mid — середина діапазону (для найвищого діапазону без верхньої межі — нижня межа);
  spend_uah_est / spend_usd_est — оцінка в гривнях і доларах за курсом НБУ на дату початку показу;
  has_disclaimer — чи вказав рекламодавець дисклеймер (не тотожне «український рекламодавець»).

таблиці_csv/регіони_<рік>.csv — оцінка витрат і показів за місяцем, сторінкою та областю:
  витрати оголошення, помножені на частку його показів у регіоні.
  OUT — поза Україною або нерозпізнаний регіон, UNK — регіон невідомий (за даними Meta).
  Деталізація до окремого оголошення є в архіві JSON (поле delivery_by_region).

таблиці_csv/сторінки.csv — підсумки за рекламодавцями (сторінками) за весь архів.

Атрибуція сторінок до політичних акторів ведеться окремо, у робочій Google-таблиці (вкладка «атрибуція»).

Порада для Excel: ID оголошень і сторінок мають 15–17 цифр, і Excel, відкриваючи CSV подвійним
кліком, перетворює їх на числа й спотворює останні цифри. Щоб цього уникнути, відкривайте файл через
«Дані → З тексту/CSV» і для колонок ad_id та page_id оберіть тип «Текст».
"""

AD_FIELDS = ["ad_id", "page_id", "page_name", "has_disclaimer", "ad_creation_time",
             "delivery_start", "delivery_stop", "currency", "spend_lower", "spend_upper",
             "spend_mid", "spend_uah_est", "spend_usd_est", "impressions_lower",
             "impressions_upper", "impressions_mid", "platforms", "languages", "ad_text",
             "link_title", "ad_library_url", "channels", "first_seen"]
REGION_FIELDS = ["month", "page_id", "page_name", "has_disclaimer", "region_iso", "region_uk",
                 "ads", "spend_uah_est", "spend_usd_est", "impressions_est"]
PAGE_FIELDS = ["page_id", "page_name", "has_disclaimer", "ads", "spend_uah_est",
               "spend_usd_est", "first_start", "last_start"]


def log(msg):
    print(msg, flush=True)


def num(x, digits=2):
    return "" if x is None or (isinstance(x, float) and math.isnan(x)) else round(float(x), digits)


def md5_file(path):
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ------------------------------------------------------------- Google Drive

class Drive:
    def __init__(self, session, drive_id):
        self.s, self.drive_id = session, drive_id

    def _call(self, method, url, **kw):
        for attempt in range(6):
            r = self.s.request(method, url, timeout=600, **kw)
            if r.status_code in (429, 500, 502, 503, 504) and attempt < 5:
                time.sleep(10 * (attempt + 1))
                continue
            if r.status_code == 404 and "driveId" in r.text:
                raise SystemExit("Сервісний акаунт не бачить спільний диск. Перевірте, що його адресу "
                                 "додано до диска з роллю «Менеджер вмісту» і що DRIVE_ID правильний.")
            if r.status_code >= 400:
                raise SystemExit(f"Google Drive відповів помилкою {r.status_code}: {r.text[:500]}")
            return r

    def check(self):
        self._call("GET", f"https://www.googleapis.com/drive/v3/drives/{self.drive_id}",
                   params={"fields": "id,name"})

    def find(self, name, parent, folder=False):
        q = [f"name = '{name}'", f"'{parent}' in parents", "trashed = false"]
        if folder:
            q.append(f"mimeType = '{FOLDER}'")
        r = self._call("GET", API, params={
            **COMMON, "q": " and ".join(q), "corpora": "drive", "driveId": self.drive_id,
            "includeItemsFromAllDrives": "true", "fields": "files(id,name)"})
        files = r.json().get("files", [])
        return files[0]["id"] if files else None

    def folder(self, name, parent):
        fid = self.find(name, parent, folder=True)
        if fid:
            return fid
        r = self._call("POST", API, params={**COMMON, "fields": "id"},
                       json={"name": name, "mimeType": FOLDER, "parents": [parent]})
        return r.json()["id"]

    def upload(self, local_path, name, parent, file_id=None, mime="application/octet-stream"):
        """Відновлюване завантаження: працює і з великими файлами."""
        if file_id:
            r = self._call("PATCH", f"{UPLOAD}/{file_id}",
                           params={**COMMON, "uploadType": "resumable"},
                           headers={"X-Upload-Content-Type": mime}, json={})
        else:
            r = self._call("POST", UPLOAD, params={**COMMON, "uploadType": "resumable", "fields": "id"},
                           headers={"X-Upload-Content-Type": mime},
                           json={"name": name, "parents": [parent]})
        with open(local_path, "rb") as fh:
            r2 = self._call("PUT", r.headers["Location"], data=fh, headers={"Content-Type": mime})
        return r2.json().get("id", file_id)


def open_drive():
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2 import service_account
    info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/drive"])
    return Drive(AuthorizedSession(creds), DRIVE_ID)


# ------------------------------------------------------------- підготовка файлів

def month_bundles():
    """Файли днів зводяться в один файл на місяць. Ключ зміни — контрольна сума вмісту днів."""
    by_month = {}
    for f in C.archive_files():
        month = "unknown" if f.name.startswith("unknown") else f.name[:7]
        by_month.setdefault(month, []).append(f)
    for month, files in sorted(by_month.items()):
        h = hashlib.md5()
        for f in files:
            h.update(f.read_bytes())
        yield month, h.hexdigest(), files


def write_bundle(files, path):
    with open(path, "wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as gz:
        for f in files:
            with gzip.open(f, "rb") as src:
                for line in src:
                    if line.strip():
                        gz.write(line if line.endswith(b"\n") else line + b"\n")


def write_tables(tmp):
    """Один прохід архівом: CSV оголошень і регіонів по роках, підсумки за сторінками."""
    rates = C.load_rates()
    fetched = set()
    out, pages, seen = {}, {}, set()
    cur_year, ads_fh, ads_w, regions = None, None, None, {}

    def close_year():
        if cur_year is None:
            return
        ads_fh.close()
        path = tmp / f"регіони_{cur_year}.csv"
        with open(path, "w", encoding="utf-8-sig", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(REGION_FIELDS)
            for (m, pid, d, iso, name), (pname, n, uah, usd, imp) in sorted(regions.items()):
                w.writerow([m, pid, pname, d, iso, name, n, num(uah), num(usd), num(imp, 0)])
        out[f"регіони_{cur_year}.csv"] = path

    for ad in C.iter_archive():
        if ad["id"] in seen:
            continue
        seen.add(ad["id"])
        v = C.ad_values(ad, rates, fetched)
        year = v["day"][:4] if v["day"] != "unknown" else "невідомий_рік"
        if year != cur_year:
            close_year()
            cur_year, regions = year, {}
            path = tmp / f"оголошення_{year}.csv"
            ads_fh = open(path, "w", encoding="utf-8-sig", newline="")
            ads_w = csv.writer(ads_fh)
            ads_w.writerow(AD_FIELDS)
            out[f"оголошення_{year}.csv"] = path
        pid = str(ad.get("page_id", ""))
        pname = ad.get("page_name", "")
        ads_w.writerow([
            ad["id"], pid, pname, v["disc"], ad.get("ad_creation_time", ""),
            ad.get("ad_delivery_start_time", ""), ad.get("ad_delivery_stop_time", ""), v["cur"],
            num(v["s"][0]), num(v["s"][1]), num(v["s"][2]), num(v["uah"]), num(v["usd"]),
            num(v["i"][0], 0), num(v["i"][1], 0), num(v["i"][2], 0),
            ", ".join(ad.get("publisher_platforms") or []), ", ".join(ad.get("languages") or []),
            C.join_texts(ad.get("ad_creative_bodies"), 30000),
            C.join_texts(ad.get("ad_creative_link_titles"), 2000),
            f"https://www.facebook.com/ads/library/?id={ad['id']}",
            ", ".join(ad.get("_channels", [])), ad.get("_first_seen", "")])
        uah = 0.0 if math.isnan(v["uah"]) else v["uah"]
        usd = 0.0 if math.isnan(v["usd"]) else v["usd"]
        imp = 0.0 if math.isnan(v["i"][2]) else v["i"][2]
        month = C.month_key(v["day"]) if v["day"] != "unknown" else ""
        for (iso, name), share in v["shares"].items():
            cell = regions.setdefault((month, pid, v["disc"], iso, name), [pname, 0, 0.0, 0.0, 0.0])
            cell[1] += 1
            cell[2] += uah * share
            cell[3] += usd * share
            cell[4] += imp * share
        pg = pages.setdefault(pid, [pname, False, 0, 0.0, 0.0, "9999", ""])
        if v["day"] != "unknown":
            if v["day"] >= pg[6]:
                pg[0] = pname
            pg[5], pg[6] = min(pg[5], v["day"]), max(pg[6], v["day"])
        pg[1] |= v["disc"]
        pg[2] += 1
        pg[3] += uah
        pg[4] += usd
    close_year()

    path = tmp / "сторінки.csv"
    with open(path, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(PAGE_FIELDS)
        for pid, (pname, disc, n, uah, usd, first, last) in sorted(pages.items(), key=lambda x: -x[1][3]):
            w.writerow([pid, pname, disc, n, num(uah), num(usd), "" if first == "9999" else first, last])
    out["сторінки.csv"] = path
    C.RATES_FILE.write_text(json.dumps(rates, ensure_ascii=False, sort_keys=True, indent=0))
    return out


# ------------------------------------------------------------------ запуск

def main():
    if not DRIVE_ID:
        log("DRIVE_ID не задано, вивантаження на диск пропущено.")
        return
    if not C.archive_files():
        log("Архів порожній, вивантажувати нічого.")
        return
    manifest = json.loads(MANIFEST_FILE.read_text()) if MANIFEST_FILE.exists() else {}
    files_state = manifest.setdefault("files", {})
    drive = open_drive()
    drive.check()
    root = DRIVE_ID
    raw_root = drive.folder("архів_jsonl", root)
    csv_root = drive.folder("таблиці_csv", root)
    year_folders = {}
    uploaded = 0

    def sync(key, local_path, name, parent, mime, change_key):
        nonlocal uploaded
        state = files_state.get(key, {})
        if state.get("key") == change_key and state.get("id"):
            return
        file_id = state.get("id") or drive.find(name, parent)
        file_id = drive.upload(local_path, name, parent, file_id, mime)
        files_state[key] = {"id": file_id, "key": change_key}
        MANIFEST_FILE.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=0))
        uploaded += 1
        log(f"  ↑ {key}")

    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        readme = tmp / "ПРОЧИТАЙ_МЕНЕ.txt"
        readme.write_text(README_TEXT, encoding="utf-8")
        sync("ПРОЧИТАЙ_МЕНЕ.txt", readme, readme.name, root, "text/plain", md5_file(readme))

        log("Архів JSON: перевіряю зміни за місяцями…")
        for month, change_key, files in month_bundles():
            year = month[:4] if month != "unknown" else "unknown"
            name = f"{month}.jsonl.gz"
            key = f"архів_jsonl/{year}/{name}"
            if files_state.get(key, {}).get("key") == change_key:
                continue
            if year not in year_folders:
                year_folders[year] = drive.folder(year, raw_root)
            path = tmp / name
            write_bundle(files, path)
            sync(key, path, name, year_folders[year], "application/gzip", change_key)
            path.unlink()

        log("Таблиці CSV: готую…")
        for name, path in write_tables(tmp).items():
            sync(f"таблиці_csv/{name}", path, name, csv_root, "text/csv", md5_file(path))

    MANIFEST_FILE.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=0))
    log(f"Спільний диск: оновлено файлів {uploaded}.")


if __name__ == "__main__":
    main()
