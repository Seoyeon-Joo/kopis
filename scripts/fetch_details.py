"""
Phase 2: 상세 수집 (청크 병렬)
Usage: python scripts/fetch_details.py <type> <chunk_index> <total_chunks>
Types: performances | venues
"""
import sys, os, csv, time, requests
import xml.etree.ElementTree as ET

API_KEY  = os.environ["KOPIS_API_KEY"]
BASE_URL = "http://www.kopis.or.kr/openApi/restful"
IN_DIR   = "output"
OUT_DIR  = "chunk_output"

CONFIGS = {
    "performances": {
        "endpoint":  "pblprfr",
        "ids_file":  "ids_performances.txt",
        "outprefix": "02_공연상세",
    },
    "venues": {
        "endpoint":  "prfplc",
        "ids_file":  "ids_venues.txt",
        "outprefix": "04_공연시설상세",
    },
}

os.makedirs(OUT_DIR, exist_ok=True)


def fetch_detail(endpoint, id_val):
    for attempt in range(3):
        try:
            r = requests.get(
                f"{BASE_URL}/{endpoint}/{id_val}",
                params={"service": API_KEY},
                timeout=30,
            )
            r.raise_for_status()
            root = ET.fromstring(r.content)
            rows = []
            for it in root.findall("db"):
                row = {c.tag: (c.text or "").strip() for c in it}
                # styurls 평탄화
                styurls = it.find("styurls")
                if styurls is not None:
                    row["styurls"] = "|".join(
                        (u.text or "").strip() for u in styurls.findall("styurl")
                    )
                rows.append(row)
            return rows
        except Exception as e:
            print(f"  [retry {attempt+1}] {id_val}: {e}", flush=True)
            time.sleep(2)
    return []


def save_csv(rows, path):
    if not rows:
        return
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main():
    kind         = sys.argv[1]
    chunk_idx    = int(sys.argv[2])
    total_chunks = int(sys.argv[3])
    cfg          = CONFIGS[kind]

    ids_path = os.path.join(IN_DIR, cfg["ids_file"])
    with open(ids_path) as f:
        all_ids = [l.strip() for l in f if l.strip()]

    # 연속 슬라이스 분할
    total   = len(all_ids)
    start   = (total * chunk_idx) // total_chunks
    end     = (total * (chunk_idx + 1)) // total_chunks
    my_ids  = all_ids[start:end]

    print(
        f"=== [{kind}] chunk {chunk_idx}/{total_chunks} "
        f"(IDs {start}~{end-1}, 총 {len(my_ids):,}개) ===",
        flush=True,
    )

    all_rows = []
    for i, id_val in enumerate(my_ids):
        all_rows.extend(fetch_detail(cfg["endpoint"], id_val))
        if (i + 1) % 200 == 0:
            print(f"  {i+1:,}/{len(my_ids):,} 처리 ({len(all_rows):,}행)", flush=True)
        time.sleep(0.2)

    outfile = f"{cfg['outprefix']}_chunk{chunk_idx}.csv"
    save_csv(all_rows, os.path.join(OUT_DIR, outfile))
    print(f"  저장 완료: {outfile} ({len(all_rows):,}행)", flush=True)


if __name__ == "__main__":
    main()
