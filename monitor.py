"""
KOPIS 수집 진행 현황 실시간 대시보드
  python monitor.py  로 실행 (kopis_collector.py 와 동시에 실행)
"""

import os
import re
import time
from datetime import datetime, timedelta
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.text import Text
from rich import box

LOG_PATH = r"C:\Users\82106\Desktop\Claude\kopis_data\수집로그.txt"
DATA_DIR = r"C:\Users\82106\Desktop\Claude\kopis_data"
ERR_PATH = r"C:\Users\82106\Desktop\Claude\kopis_data\오류로그.txt"

# ── 수집 단계 정의 ─────────────────────────────────────────────────────────────
STEPS = [
    # (로그 키워드,             표시명,                  파일명,                      예상건수)
    ("[1/8]",  "공연목록",         "01_공연목록.csv",              25000),
    ("[2/8]",  "공연상세",         "02_공연상세.csv",              25000),
    ("[3/8]",  "공연시설목록",     "03_공연시설목록.csv",           3000),
    ("[4/8]",  "공연시설상세",     "04_공연시설상세.csv",           3000),
    ("[5/8]",  "기획/제작사",      "05_기획제작사목록.csv",         5000),
    ("[6/8]",  "수상작",           "06_수상작목록.csv",             1000),
    ("[7/8]",  "축제",             "07_축제목록.csv",               2000),
    ("[8/8]",  "원·창작자",        "08_원창작자목록.csv",           5000),
    ("[예매통계 1/5]", "예매상황판",     "09_예매상황판_일별.csv",     3000),
    ("[예매통계 2/5]", "예매통계(기간)", "10_예매통계_기간별(주별).csv", 1500),
    ("[예매통계 3/5]", "예매통계(월별)", "11_예매통계_월별.csv",      500),
    ("[예매통계 4/5]", "예매통계(시간)", None,                        0),
    ("[예매통계 5/5]", "예매통계(가격)", None,                        0),
    ("[공연통계 1/6]", "공연통계(기간)", "13_공연통계_기간별.csv",    5000),
    ("[공연통계 2/6]", "공연통계(지역)", "14_공연통계_지역별.csv",    3000),
    ("[공연통계 3/6]", "공연통계(장르)", "15_공연통계_장르별.csv",    3000),
    ("[공연통계 4/6]", "공연통계(공연)", "16_공연통계_공연별.csv",    5000),
    ("[공연통계 5/6]", "공연통계(시설)", "17_공연통계_시설별.csv",    3000),
    ("[공연통계 6/6]", "공연통계(가격)", "18_공연통계_가격대별.csv",  2000),
]

TOTAL_STEPS = len(STEPS)

# ── 로그 파싱 ──────────────────────────────────────────────────────────────────

def parse_log():
    """로그 파일을 읽어 현재 활성 단계와 누적 건수를 반환"""
    if not os.path.exists(LOG_PATH):
        return -1, 0, {}

    try:
        with open(LOG_PATH, encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    except Exception:
        return -1, 0, {}

    active_step = -1
    step_counts = {}   # step_idx -> 누적건수
    completed = set()

    for line in lines:
        line = line.strip()
        # 단계 시작 감지
        for i, (key, *_) in enumerate(STEPS):
            if key in line:
                active_step = i
                break
        # 누적 건수 파싱
        m = re.search(r"누적\s*([\d,]+)건", line)
        if m and active_step >= 0:
            step_counts[active_step] = int(m.group(1).replace(",", ""))
        # 저장 완료 감지 (→ 로 표기)
        if "→" in line and "행 저장" in line:
            m2 = re.search(r"([\d,]+)행 저장", line)
            if m2 and active_step >= 0:
                step_counts[active_step] = int(m2.group(1).replace(",", ""))
                completed.add(active_step)

    return active_step, step_counts, completed


def csv_size(filename):
    if filename is None:
        return None
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        return os.path.getsize(path)
    return None


def file_rows(filename):
    """CSV 파일의 데이터 행 수 (헤더 제외)"""
    if filename is None:
        return None
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8-sig", errors="ignore") as f:
            return sum(1 for _ in f) - 1
    except Exception:
        return None


def error_lines():
    if not os.path.exists(ERR_PATH):
        return []
    try:
        with open(ERR_PATH, encoding="utf-8", errors="ignore") as f:
            return [l.strip() for l in f if l.strip()]
    except Exception:
        return []


# ── 렌더링 ────────────────────────────────────────────────────────────────────

def bar(ratio: float, width: int = 20) -> str:
    filled = int(ratio * width)
    return "█" * filled + "░" * (width - filled)


def pct(val, total):
    if total == 0:
        return 0.0
    return min(val / total * 100, 100.0)


def build_table(active_step, step_counts, completed) -> Table:
    t = Table(
        box=box.ROUNDED,
        show_header=True,
        header_style="bold cyan",
        expand=True,
        padding=(0, 1),
    )
    t.add_column("단계", style="bold", width=6)
    t.add_column("항목", min_width=14)
    t.add_column("상태", width=8)
    t.add_column("진행 바", min_width=22)
    t.add_column("수집건수", justify="right", width=10)
    t.add_column("CSV 크기", justify="right", width=9)

    for i, (key, name, csvfile, est) in enumerate(STEPS):
        count = step_counts.get(i, 0)
        saved = file_rows(csvfile)
        size  = csv_size(csvfile)

        if i in completed or (saved is not None and saved > 0):
            status_txt = Text("✔ 완료", style="bold green")
            ratio = 1.0
        elif i == active_step:
            status_txt = Text("⟳ 수집중", style="bold yellow")
            ratio = pct(count, est) / 100
        elif i < active_step:
            status_txt = Text("✔ 완료", style="bold green")
            ratio = 1.0
        else:
            status_txt = Text("  대기", style="dim")
            ratio = 0.0

        b = bar(ratio, 20)
        if ratio >= 1.0:
            bar_txt = Text(b, style="green")
        elif ratio > 0:
            filled = int(ratio * 20)
            bar_txt = Text("█" * filled, style="yellow") + Text("░" * (20 - filled), style="dim")
        else:
            bar_txt = Text(b, style="dim")

        count_str = f"{count:,}" if (count or i <= active_step) else "-"
        if saved is not None:
            count_str = f"{saved:,}"

        size_str = "-"
        if size is not None:
            if size > 1_048_576:
                size_str = f"{size/1_048_576:.1f} MB"
            else:
                size_str = f"{size/1024:.0f} KB"

        # 구분선 (예매통계 / 공연통계 시작 전)
        if key == "[예매통계 1/5]":
            t.add_section()
        elif key == "[공연통계 1/6]":
            t.add_section()

        t.add_row(
            f"[{i+1:02d}]",
            name,
            status_txt,
            bar_txt,
            count_str,
            size_str,
        )

    return t


def build_summary(active_step, step_counts, completed, elapsed_sec) -> Panel:
    done_steps = len([i for i in range(TOTAL_STEPS) if i in completed or i < active_step])
    total_collected = sum(step_counts.values())

    elapsed = str(timedelta(seconds=int(elapsed_sec)))
    ratio_steps = done_steps / TOTAL_STEPS

    # 전체 진행 바
    overall = bar(ratio_steps, 40)
    overall_txt = (
        Text("█" * int(ratio_steps * 40), style="bold cyan")
        + Text("░" * (40 - int(ratio_steps * 40)), style="dim")
    )

    errs = error_lines()

    lines = Text()
    lines.append(f"  전체 진행  ", style="bold")
    lines.append(overall_txt)
    lines.append(f"  {done_steps}/{TOTAL_STEPS} 단계  ({ratio_steps*100:.0f}%)\n", style="bold cyan")
    lines.append(f"  경과 시간  ", style="bold")
    lines.append(f"{elapsed}\n", style="yellow")
    lines.append(f"  총 수집 건수  ", style="bold")
    lines.append(f"{total_collected:,} 건\n", style="green")
    if errs:
        lines.append(f"  오류  ", style="bold red")
        lines.append(f"{len(errs)}건 발생\n", style="red")

    return Panel(lines, title="[bold]수집 현황 요약[/bold]", border_style="cyan")


def build_dashboard(start_time):
    active_step, step_counts, completed = parse_log()
    elapsed = (datetime.now() - start_time).total_seconds()

    layout = Layout()
    layout.split_column(
        Layout(name="summary", size=7),
        Layout(name="table"),
        Layout(name="footer", size=3),
    )

    layout["summary"].update(build_summary(active_step, step_counts, completed, elapsed))
    layout["table"].update(
        Panel(
            build_table(active_step, step_counts, completed),
            title="[bold]항목별 수집 현황[/bold]",
            border_style="blue",
        )
    )

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    footer_txt = Text(f"  마지막 갱신: {now_str}   (Ctrl+C 로 종료)", style="dim")
    layout["footer"].update(Panel(footer_txt, border_style="dim"))

    return layout


def main():
    console = Console()
    start_time = datetime.now()

    console.print(
        Panel.fit(
            "[bold cyan]KOPIS 데이터 수집 실시간 모니터[/bold cyan]\n"
            "[dim]3초마다 자동 갱신됩니다. Ctrl+C로 종료.[/dim]",
            border_style="cyan",
        )
    )

    with Live(console=console, refresh_per_second=1, screen=True) as live:
        while True:
            try:
                live.update(build_dashboard(start_time))
                time.sleep(3)
            except KeyboardInterrupt:
                break

    console.print("[bold green]모니터 종료[/bold green]")


if __name__ == "__main__":
    main()
