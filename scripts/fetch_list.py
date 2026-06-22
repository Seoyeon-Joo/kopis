"""
Phase 1: 목록 수집
Usage: python scripts/fetch_list.py <type>
Types: performances | venues | producers | awards | festivals | creators
"""
import sys, os, csv, time, requests
import xml.etree.ElementTree as ET
from datetime import datetime

API_KEY  = os.environ["KOPIS_API_KEY"]
BASE_URL = "http://www.kopis.or.kr/openApi/restful"
OUT_DIR  = "output"
TODAY    = datetime.today().strftime("%Y%m%d")

CONFIGS = {
    "performances": {
        "endpoint": "pblprfr",
        "params":   {"stdate": "20100101", "eddate": TODAY},
        "outfile":  "01_공연목록.csv",
        "id_field": "mt20id",
        "ids_file": "ids_performances.txt",
    },
    "venues": {
        "endpoint": "prfplc",
        "params":   {},
        "outfile":  "03_공연시설목록.csv",
        "id_field": "mt10id",
        "ids_file": "ids_venues.txt",
    },
    "producers": {"endpoint": "mnfct",    "params": {},                                     "outfile": "05_기획제작사목록.csv"},
    "awards":    {"endpoint": "award",    "params": {},                                     "outfile": "06_수상작목록.csv"},
    "festivals": {"endpoint": "festival", "params": {"stdate": "20100101", "eddate": TODAY}, "outfile": "07_축제목록.csv"},
    "creators":  {"endpoint": "prfstf",  "params": {},                                     "outfile": "08_원창작자목록.csv"},
}

os.makedirs(OUT_DIR, exist_ok=True)


def fetch_page(endpoint, params):
    for attempt in range(3):
        try:
            r = requests.get(
                f"{BASE_URL}/{endpoint}",
                params={**params, "service": API_KEY},
                timeout=30,
            )
            r.raise_for_status()
            return ET.fromstring(r.content)
        except Exception as e:
            print(f"  [retry {attempt+1}] {e}", flush=True)
            time.sleep(2)
    return None


def fetch_all(endpoint, base_params):
    rows, page = [], 1
    while True:
        root = fetch_page(endpoint, {**base_params, "cpage": page, "rows": 100})
        if root is None:
            break
        items = [{c.tag: (c.text or "").strip() for c in it} for it in root.findall("db")]
        if not items:
            break
        rows.extend(items)
        print(f"  page {page}: +{len(items)} = {len(rows):,} total", flush=True)
        if len(items) < 100:
            break
        page += 1
        time.sleep(0.2)
    return rows


def save_csv(rows, filename):
    if not rows:
        print(f"  데이터 없음: {filename}", flush=True)
        return
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  저장 완료: {path} ({len(rows):,}행)", flush=True)


def main():
    kind = sys.argv[1]
    cfg  = CONFIGS[kind]
    print(f"=== [{kind}] 수집 시작 ===", flush=True)

    rows = fetch_all(cfg["endpoint"], cfg.get("params", {}))
    save_csv(rows, cfg["outfile"])

    if "ids_file" in cfg:
        ids = [r.get(cfg["id_field"], "") for r in rows if r.get(cfg["id_field"])]
        ids_path = os.path.join(OUT_DIR, cfg["ids_file"])
        with open(ids_path, "w") as f:
            f.write("\n".join(ids))
        print(f"  ID 파일 저장: {ids_path} ({len(ids):,}개)", flush=True)

    print(f"=== [{kind}] 완료 ===", flush=True)


if __name__ == "__main__":
    main()
