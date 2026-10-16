#!/usr/bin/env python3
"""
Generate an SVG chart of Docker pulls over time for rekuiper.
Can fetch the latest pull count from Docker Hub and update docs/data/docker-pulls.csv.
"""

import sys
import os
import csv
import json
import urllib.request
from datetime import datetime, timezone

DOCKER_HUB_REPO = "indusstack/rekuiper"

def fetch_current_pulls():
    url = f"https://hub.docker.com/v2/repositories/{DOCKER_HUB_REPO}/"
    req = urllib.request.Request(url, headers={"User-Agent": "rekuiper-pull-tracker/1.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        return int(data.get("pull_count", 0))

def update_csv(csv_path, pulls):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = []
    if os.path.exists(csv_path):
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for r in reader:
                if len(r) >= 2:
                    rows.append((r[0].strip(), int(r[1].strip())))

    # Update or append today's count
    updated = False
    for i, (d, count) in enumerate(rows):
        if d == today:
            rows[i] = (today, max(count, pulls))
            updated = True
            break
    if not updated:
        rows.append((today, pulls))

    rows.sort(key=lambda x: x[0])

    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "pulls"])
        for r in rows:
            writer.writerow([r[0], r[1]])

    return rows

def load_csv(csv_path):
    rows = []
    if os.path.exists(csv_path):
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None) # skip header
            for r in reader:
                if len(r) >= 2:
                    rows.append((r[0].strip(), int(r[1].strip())))
    return rows

def format_count(n):
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)

def format_date_label(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%b %d")
    except Exception:
        return date_str

def render_svg(rows, output_path):
    if not rows:
        return

    width = 800
    height = 320
    pad_left = 65
    pad_right = 40
    pad_top = 70
    pad_bottom = 55

    chart_w = width - pad_left - pad_right
    chart_h = height - pad_top - pad_bottom

    latest_date, latest_pulls = rows[-1]
    min_pulls = 0
    max_pulls_raw = max(r[1] for r in rows)
    # Round max_pulls up to a nice round number
    if max_pulls_raw <= 1000:
        y_step = 250
    elif max_pulls_raw <= 5000:
        y_step = 1000
    elif max_pulls_raw <= 10000:
        y_step = 2000
    else:
        y_step = 5000

    max_pulls = ((max_pulls_raw // y_step) + 1) * y_step
    if max_pulls == max_pulls_raw:
        max_pulls += y_step

    def get_x(idx, total):
        if total <= 1:
            return pad_left + chart_w / 2
        return pad_left + (idx / (total - 1)) * chart_w

    def get_y(val):
        span = max_pulls - min_pulls
        ratio = (val - min_pulls) / span if span > 0 else 0
        return pad_top + chart_h - (ratio * chart_h)

    points = [(get_x(i, len(rows)), get_y(val), d, val) for i, (d, val) in enumerate(rows)]

    # Path data
    line_path = []
    for i, (x, y, _, _) in enumerate(points):
        cmd = "M" if i == 0 else "L"
        line_path.append(f"{cmd} {x:.1f} {y:.1f}")
    line_d = " ".join(line_path)

    area_d = f"{line_d} L {points[-1][0]:.1f} {pad_top + chart_h:.1f} L {points[0][0]:.1f} {pad_top + chart_h:.1f} Z"

    # Y-axis grid and labels
    y_ticks = []
    num_ticks = max(3, min(5, int(max_pulls / y_step) + 1))
    tick_step = max_pulls / (num_ticks - 1)
    for i in range(num_ticks):
        val = int(i * tick_step)
        y = get_y(val)
        y_ticks.append((y, format_count(val)))

    # X-axis labels (select subset so they don't overlap)
    x_ticks = []
    max_x_labels = 7
    step = max(1, len(points) // max_x_labels)
    chosen_indices = list(range(0, len(points), step))
    if (len(points) - 1) not in chosen_indices:
        chosen_indices.append(len(points) - 1)

    for idx in chosen_indices:
        x, _, d, _ = points[idx]
        x_ticks.append((x, format_date_label(d)))

    svg_elements = []
    svg_elements.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%" height="auto">')
    svg_elements.append("""<defs>
  <linearGradient id="lineGrad" x1="0" y1="0" x2="0" y2="1">
    <stop offset="0%" stop-color="#58a6ff" stop-opacity="0.35" />
    <stop offset="100%" stop-color="#58a6ff" stop-opacity="0.0" />
  </linearGradient>
  <style>
    .bg { fill: #0d1117; stroke: #30363d; stroke-width: 1px; rx: 6px; }
    .title { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; font-size: 14px; font-weight: 600; fill: #e6edf3; }
    .stat-text { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; font-size: 13px; font-weight: 500; fill: #8b949e; }
    .grid { stroke: #21262d; stroke-width: 1px; stroke-dasharray: 4,4; }
    .axis-text { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; font-size: 11px; fill: #7d8590; }
    .spark-line { fill: none; stroke: #58a6ff; stroke-width: 2px; stroke-linecap: round; stroke-linejoin: round; }
    .spark-point { fill: #0d1117; stroke: #58a6ff; stroke-width: 2px; }
  </style>
</defs>""")

    # Background
    svg_elements.append(f'<rect width="{width}" height="{height}" class="bg" />')

    # Header: clean title on left, plain pull count on right
    svg_elements.append(f'<text x="{pad_left}" y="36" class="title">Docker Pulls</text>')
    svg_elements.append(f'<text x="{width - pad_right}" y="36" text-anchor="end" class="stat-text">{latest_pulls:,} total</text>')

    # Y grid lines and labels
    for y, label in y_ticks:
        svg_elements.append(f'<line x1="{pad_left}" y1="{y:.1f}" x2="{width - pad_right}" y2="{y:.1f}" class="grid" />')
        svg_elements.append(f'<text x="{pad_left - 10}" y="{y + 4:.1f}" text-anchor="end" class="axis-text">{label}</text>')

    # X labels and ticks
    for x, label in x_ticks:
        svg_elements.append(f'<line x1="{x:.1f}" y1="{pad_top + chart_h}" x2="{x:.1f}" y2="{pad_top + chart_h + 5}" stroke="#30363d" stroke-width="1px" />')
        svg_elements.append(f'<text x="{x:.1f}" y="{pad_top + chart_h + 20}" text-anchor="middle" class="axis-text">{label}</text>')

    # Fill area
    svg_elements.append(f'<path d="{area_d}" fill="url(#lineGrad)" />')

    # Main line
    svg_elements.append(f'<path d="{line_d}" class="spark-line" />')

    # Points with tooltips
    for x, y, d, val in points:
        svg_elements.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" class="spark-point"><title>{d}: {val:,} pulls</title></circle>')

    svg_elements.append('</svg>')

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(svg_elements) + "\n")
    print(f"Rendered chart to {output_path} ({len(rows)} data points, latest: {latest_pulls:,})")

def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(root, "docs", "data", "docker-pulls.csv")
    svg_path = os.path.join(root, "docs", "docker-pulls.svg")

    if "--fetch" in sys.argv:
        try:
            print(f"Fetching latest pulls for {DOCKER_HUB_REPO}...")
            pulls = fetch_current_pulls()
            print(f"Current pull count: {pulls}")
            rows = update_csv(csv_path, pulls)
        except Exception as e:
            print(f"Error fetching from Docker Hub: {e}", file=sys.stderr)
            rows = load_csv(csv_path)
    else:
        rows = load_csv(csv_path)

    render_svg(rows, svg_path)

if __name__ == "__main__":
    main()
