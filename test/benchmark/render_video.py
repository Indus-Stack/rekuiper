#!/usr/bin/env python3
"""
LEGACY NONCOMPARABLE visualizer (do NOT use for claims).
Prior file-source/in-process numbers with invented 1M extrapolation and rankings.
No equivalent fair over-HTTP rerun exists for these engines, so no ranking is claimed.
See test/BENCHMARKS.md and BENCHMARK-AUDIT.md for the fair audit.
"""
import subprocess
import math
import os
import sys
import random
import wave
import struct
from PIL import Image, ImageDraw, ImageFont

WIDTH = 1920
HEIGHT = 1080
FPS = 60
TOTAL_RECORDS = 1_000_000
# 41.5 seconds ensures all 5 candidates finish (Benthos takes 38.47s)
# plus 3 seconds of final victory display for the complete leaderboard
DURATION_SEC = 41.5
TOTAL_FRAMES = int(FPS * DURATION_SEC)
AUDIO_SAMPLE_RATE = 44100

OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rekuiper_benchmark_race_1m.mp4")
TEMP_AUDIO_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp_race_audio.wav")

# Material Design 3 Light Theme Color Palette (RGB)
BG_COLOR = (248, 249, 252)          # M3 Surface Container Low
TRACK_BG_EVEN = (255, 255, 255)      # Pure White lane
TRACK_BG_ODD = (241, 244, 249)       # M3 Surface Container
TRACK_LINE = (203, 213, 225)         # M3 Subtle Rail
TEXT_PRIMARY = (15, 23, 42)          # Dark Slate (High contrast)
TEXT_MUTED = (100, 116, 139)         # Muted Slate
BORDER_COLOR = (226, 232, 240)

# Lanes config extrapolated to 1,000,000 records
LANES = [
    {
        "name": "rekuiper",
        "title": "rekuiper (Native Rust)",
        "sub": "Single Core | RAM: 8.2 MB | 0 Drops",
        "color": (194, 65, 12),       # Vibrant Terracotta Rust
        "light_color": (255, 112, 67),
        "elapsed": 2.3512,            # 1,000,000 / 425,308 eps
        "eps": 425308,
        "drops": 0,
        "bounce_freq": 4.6,           # 4.6 bounces/sec
        "pitch": 1046.50,             # C6 (High crystal ping)
    },
    {
        "name": "flink",
        "title": "Apache Flink (Java / JVM)",
        "sub": "Single Core | RAM: 1,022 MB (1.02 GB)",
        "color": (2, 132, 199),        # Cyan Blue
        "light_color": (56, 189, 248),
        "elapsed": 4.2880,            # 1,000,000 / 233,209 eps
        "eps": 233209,
        "drops": 0,
        "bounce_freq": 3.2,           # 3.2 bounces/sec
        "pitch": 783.99,              # G5 (Bright chime)
    },
    {
        "name": "telegraf",
        "title": "Telegraf (Go)",
        "sub": "Single Core | RAM: ~50 MB",
        "color": (5, 150, 105),        # Emerald Green
        "light_color": (52, 211, 153),
        "elapsed": 16.3882,           # 1,000,000 / 61,019 eps
        "eps": 61019,
        "drops": 0,
        "bounce_freq": 2.1,           # 2.1 bounces/sec
        "pitch": 659.25,              # E5 (Vibrant bell)
    },
    {
        "name": "ekuiper",
        "title": "eKuiper",
        "sub": "Single Core | RAM: ~45 MB | 145k DROPS",
        "color": (217, 119, 6),        # Warm Amber
        "light_color": (251, 191, 36),
        "elapsed": 22.5809,           # 1,000,000 / 44,287 eps
        "eps": 44287,
        "drops": 145842,
        "bounce_freq": 1.6,           # 1.6 bounces/sec
        "pitch": 523.25,              # C5 (Mid pop / drop buzz)
    },
    {
        "name": "benthos",
        "title": "Redpanda Connect (Benthos)",
        "sub": "Single Core | RAM: ~38 MB",
        "color": (124, 58, 237),       # Royal Violet
        "light_color": (167, 139, 250),
        "elapsed": 38.4720,           # 1,000,000 / 25,993 eps
        "eps": 25993,
        "drops": 0,
        "bounce_freq": 1.2,           # 1.2 bounces/sec
        "pitch": 392.00,              # G4 (Warm resonant thud)
    }
]

# Track Geometry
PADDING_LEFT = 480
PADDING_RIGHT = 380
TRACK_WIDTH = WIDTH - PADDING_LEFT - PADDING_RIGHT
LANE_HEIGHT = 160
TOP_OFFSET = 180
BALL_RADIUS = 24

def get_font(size, bold=False):
    font_names = [
        "arialbd.ttf" if bold else "arial.ttf",
        "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf",
        "SegoeUI-Bold.ttf" if bold else "SegoeUI.ttf"
    ]
    for name in font_names:
        try:
            return ImageFont.truetype(name, size)
        except Exception:
            pass
    return ImageFont.load_default()

def spawn_sparks(x, y, color, count=16, is_drop=False, direction=0):
    new_sparks = []
    for _ in range(count):
        speed = random.uniform(3.0, 9.0)
        if direction == -1:
            angle = math.pi + random.uniform(-0.8, 0.8)
        elif direction == 1:
            angle = random.uniform(-0.8, 0.8)
        else:
            angle = random.uniform(0, math.pi * 2)

        spark_color = (220, 38, 38) if is_drop else (color if random.random() > 0.4 else (245, 158, 11))
        new_sparks.append({
            "x": float(x),
            "y": float(y),
            "vx": math.cos(angle) * speed,
            "vy": math.sin(angle) * speed,
            "color": spark_color,
            "life": 1.0,
            "decay": random.uniform(2.0, 3.2),
            "size": random.uniform(3.0, 5.5)
        })
    return new_sparks

def draw_rank_badge(draw, x, y, rank_str, bg_color, border_color, text_color, label_str, label_color, font_pill, font_text):
    """Draws a crisp vector pill badge without relying on unicode emojis."""
    pill_w = 60
    pill_h = 30
    draw.rounded_rectangle([x, y, x + pill_w, y + pill_h], radius=15, fill=bg_color, outline=border_color, width=2)
    bbox = font_pill.getbbox(rank_str)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    draw.text((x + (pill_w - tw) // 2, y + (pill_h - th) // 2 - 2), rank_str, font=font_pill, fill=text_color)
    draw.text((x + pill_w + 12, y + 3), label_str, font=font_text, fill=label_color)

def generate_audio_track(bounce_events):
    """Synthesizes high quality 44.1kHz audio of multi-pitch bouncing balls."""
    print("Synthesizing multi-pitch audio track...")
    total_samples = int(AUDIO_SAMPLE_RATE * DURATION_SEC)
    audio_buffer = [0.0] * total_samples

    sound_duration = 0.11
    decay_samples = int(sound_duration * AUDIO_SAMPLE_RATE)

    for (t_event, pitch, is_drop) in bounce_events:
        start_idx = int(t_event * AUDIO_SAMPLE_RATE)
        for i in range(decay_samples):
            idx = start_idx + i
            if idx >= total_samples:
                break
            t = i / AUDIO_SAMPLE_RATE
            envelope = math.exp(-t * 22.0)
            cur_freq = pitch * (1.0 - 0.18 * (t / sound_duration))
            phase = 2.0 * math.pi * cur_freq * t
            # Triangle wave for rich acoustic harmonics
            sine_val = math.sin(phase)
            tri_val = (2.0 / math.pi) * math.asin(max(-1.0, min(1.0, sine_val)))
            if is_drop:
                tri_val = 0.6 * tri_val + 0.4 * math.sin(phase * 2.0)
            audio_buffer[idx] += tri_val * envelope * 0.25

    # Peak normalize and convert to 16-bit PCM
    max_val = max((abs(s) for s in audio_buffer), default=1.0)
    norm_factor = 28000.0 / max(max_val, 0.001)
    pcm_data = bytearray()
    for s in audio_buffer:
        val = int(max(-32767, min(32767, s * norm_factor)))
        # Stereo (duplicate to L and R channels)
        pcm_data.extend(struct.pack('<hh', val, val))

    with wave.open(TEMP_AUDIO_FILE, 'wb') as wf:
        wf.setnchannels(2)
        wf.setsampwidth(2)
        wf.setframerate(AUDIO_SAMPLE_RATE)
        wf.writeframes(pcm_data)
    print(f"Audio synthesized successfully: {TEMP_AUDIO_FILE}")

def render_video():
    print("=" * 70)
    print(f"Rendering 60 FPS M3 Light Benchmark Video: {OUTPUT_FILE}")
    print(f"Resolution: {WIDTH}x{HEIGHT} | Frames: {TOTAL_FRAMES} | Duration: {DURATION_SEC}s")
    print("=" * 70)

    font_title = get_font(42, bold=True)
    font_sub = get_font(21, bold=False)
    font_timer = get_font(36, bold=True)
    font_lane_title = get_font(28, bold=True)
    font_lane_sub = get_font(18, bold=False)
    font_metrics = get_font(26, bold=True)
    font_pill = get_font(16, bold=True)
    font_badge = get_font(20, bold=True)

    # 1. Pre-calculate bouncing events for audio synthesis
    bounce_events = []
    prev_states = [None] * len(LANES)
    for f_idx in range(TOTAL_FRAMES):
        sim_time = f_idx / FPS
        for idx, lane in enumerate(LANES):
            if sim_time < lane["elapsed"]:
                period = 1.0 / lane["bounce_freq"]
                cycle = (sim_time % period) / period
                cur_bumper = "right" if cycle >= 0.48 and cycle < 0.52 else ("left" if cycle >= 0.98 or cycle < 0.02 else None)
                if cur_bumper and cur_bumper != prev_states[idx]:
                    bounce_events.append((sim_time, lane["pitch"], lane["drops"] > 0))
                prev_states[idx] = cur_bumper
            elif prev_states[idx] != "finished":
                # Finish line chord trigger
                bounce_events.append((sim_time, lane["pitch"] * 1.25, False))
                prev_states[idx] = "finished"

    generate_audio_track(bounce_events)

    # Launch FFmpeg process piping raw RGB frames and muxing audio
    ffmpeg_cmd = [
        "ffmpeg", "-y",
        "-f", "rawvideo",
        "-vcodec", "rawvideo",
        "-s", f"{WIDTH}x{HEIGHT}",
        "-pix_fmt", "rgb24",
        "-r", str(FPS),
        "-i", "-",
        "-i", TEMP_AUDIO_FILE,
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "17",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "192k",
        "-shortest",
        OUTPUT_FILE
    ]

    proc = subprocess.Popen(ffmpeg_cmd, stdin=subprocess.PIPE, stderr=subprocess.DEVNULL)

    # Global particle system for impact sparks
    sparks = []
    prev_bumper_states = [None] * len(LANES)

    for f_idx in range(TOTAL_FRAMES):
        sim_time = f_idx / FPS
        dt = 1.0 / FPS

        img = Image.new("RGB", (WIDTH, HEIGHT), BG_COLOR)
        draw = ImageDraw.Draw(img)

        # 1. Header & Title Banner (M3 Light)
        draw.text((60, 48), "STREAMING ENGINE BENCHMARK RACE - 1,000,000 EVENTS", font=font_title, fill=TEXT_PRIMARY)
        draw.text((60, 108), "1,000,000 Wide-Schema Telemetry Events on Single CPU Core (Empirical Extrapolation)", font=font_sub, fill=TEXT_MUTED)

        # Header Timer Pill (M3 Tonal Container)
        mins = int(sim_time // 60)
        secs = int(sim_time % 60)
        ms = int((sim_time % 1) * 1000)
        time_str = f"ELAPSED: {mins:02d}:{secs:02d}.{ms:03d}"
        draw.rounded_rectangle([WIDTH - 460, 50, WIDTH - 60, 122], radius=18, fill=(237, 242, 252), outline=(211, 227, 253), width=2)
        draw.text((WIDTH - 430, 66), time_str, font=font_timer, fill=(11, 87, 208))

        # 2. Render each track lane
        for idx, lane in enumerate(LANES):
            y_top = TOP_OFFSET + idx * LANE_HEIGHT
            y_center = y_top + LANE_HEIGHT // 2
            y_bottom = y_top + LANE_HEIGHT

            # Lane background & separator (M3 alternating bands)
            bg = TRACK_BG_EVEN if idx % 2 == 0 else TRACK_BG_ODD
            draw.rectangle([0, y_top, WIDTH, y_bottom], fill=bg)
            draw.line([(0, y_bottom), (WIDTH, y_bottom)], fill=BORDER_COLOR, width=1)

            # Left side: Engine Information
            draw.text((50, y_center - 28), lane["title"], font=font_lane_title, fill=lane["color"])
            draw.text((50, y_center + 10), lane["sub"], font=font_lane_sub, fill=TEXT_MUTED)

            # Track rails (Clean M3 rail)
            rail_x1 = PADDING_LEFT
            rail_x2 = PADDING_LEFT + TRACK_WIDTH
            draw.line([(rail_x1, y_center), (rail_x2, y_center)], fill=TRACK_LINE, width=6)

            # Left/Right impact bumpers (Slate rounded pill stops)
            draw.rounded_rectangle([rail_x1 - 10, y_center - 24, rail_x1, y_center + 24], radius=4, fill=(148, 163, 184))
            draw.rounded_rectangle([rail_x2, y_center - 24, rail_x2 + 10, y_center + 24], radius=4, fill=(148, 163, 184))

            # Progress calculation
            progress = min(1.0, sim_time / lane["elapsed"])
            processed = int(progress * TOTAL_RECORDS)
            finished = progress >= 1.0

            # Ball position based on bounce frequency
            if not finished:
                period = 1.0 / lane["bounce_freq"]
                cycle = (sim_time % period) / period
                if cycle < 0.5:
                    bx = rail_x1 + (cycle * 2) * TRACK_WIDTH
                    dx = 1
                else:
                    bx = rail_x2 - ((cycle - 0.5) * 2) * TRACK_WIDTH
                    dx = -1

                # Detect bumper hit transitions to spawn directional sparks
                cur_bumper = "right" if cycle >= 0.48 and cycle < 0.52 else ("left" if cycle >= 0.98 or cycle < 0.02 else None)
                if cur_bumper and cur_bumper != prev_bumper_states[idx]:
                    direction = -1 if cur_bumper == "right" else 1
                    contact_x = rail_x2 if cur_bumper == "right" else rail_x1
                    sparks.extend(spawn_sparks(contact_x, y_center, lane["color"], count=14, is_drop=(lane["drops"] > 0), direction=direction))
                prev_bumper_states[idx] = cur_bumper
            else:
                # Dock cleanly at the finish line bumper
                bx = rail_x2
                dx = 0
                if prev_bumper_states[idx] != "finished":
                    # Spawn victory burst upon crossing finish line
                    sparks.extend(spawn_sparks(rail_x2, y_center, lane["color"], count=24, is_drop=False, direction=0))
                    prev_bumper_states[idx] = "finished"

            # Draw Motion Trail (Smooth alpha fade behind the ball)
            if not finished:
                for t_step in range(1, 6):
                    trail_x = bx - dx * (t_step * 14)
                    alpha_factor = 1.0 - (t_step * 0.16)
                    t_radius = int(BALL_RADIUS * alpha_factor)
                    bg_color = bg
                    t_color = (
                        int(lane["color"][0] * alpha_factor * 0.4 + bg_color[0] * (1.0 - alpha_factor * 0.4)),
                        int(lane["color"][1] * alpha_factor * 0.4 + bg_color[1] * (1.0 - alpha_factor * 0.4)),
                        int(lane["color"][2] * alpha_factor * 0.4 + bg_color[2] * (1.0 - alpha_factor * 0.4)),
                    )
                    draw.ellipse([trail_x - t_radius, y_center - t_radius, trail_x + t_radius, y_center + t_radius], fill=t_color)

            # Draw Soft Elevation Shadow (M3 light theme)
            shadow_offset = 4
            shadow_r = BALL_RADIUS + 2
            shadow_color = (
                int(bg[0] * 0.85),
                int(bg[1] * 0.85),
                int(bg[2] * 0.88),
            )
            draw.ellipse([bx - shadow_r, y_center + shadow_offset - shadow_r, bx + shadow_r, y_center + shadow_offset + shadow_r], fill=shadow_color)

            # Draw Main Ball Sphere (Vibrant M3 color)
            draw.ellipse([bx - BALL_RADIUS, y_center - BALL_RADIUS, bx + BALL_RADIUS, y_center + BALL_RADIUS], fill=lane["color"])

            # Draw 3D Specular Highlight (Inner glossy depth)
            high_offset_x = 0 if finished else int(dx * 4)
            draw.ellipse([bx - high_offset_x - 10, y_center - 10, bx - high_offset_x + 6, y_center + 6], fill=lane["light_color"])
            draw.ellipse([bx - high_offset_x - 7, y_center - 7, bx - high_offset_x + 1, y_center + 1], fill=(255, 255, 255))

            # Right side: Real-time Stats & Badges
            stats_x = rail_x2 + 36
            metric_color = lane["color"] if finished else TEXT_PRIMARY
            draw.text((stats_x, y_center - 26), f"{processed:,} events", font=font_metrics, fill=metric_color)

            if finished:
                if idx == 0:
                    draw_rank_badge(
                        draw, stats_x, y_center + 4,
                        "1st", (254, 240, 138), (202, 138, 4), (161, 98, 7),
                        "1st PLACE (2.35s · 425k eps)", (22, 163, 74),
                        font_pill, font_badge
                    )
                elif idx == 1:
                    draw_rank_badge(
                        draw, stats_x, y_center + 4,
                        "2nd", (241, 245, 249), (148, 163, 184), (71, 85, 105),
                        "2nd PLACE (4.29s · 233k eps)", (2, 132, 199),
                        font_pill, font_badge
                    )
                elif idx == 2:
                    draw_rank_badge(
                        draw, stats_x, y_center + 4,
                        "3rd", (254, 243, 199), (217, 119, 6), (180, 83, 9),
                        "3rd PLACE (16.39s · 61k eps)", (202, 138, 4),
                        font_pill, font_badge
                    )
                elif idx == 3:
                    draw_rank_badge(
                        draw, stats_x, y_center + 4,
                        "4th", (254, 226, 226), (239, 68, 68), (185, 28, 28),
                        "4th PLACE (22.58s · 145k drops)", (220, 38, 38),
                        font_pill, font_badge
                    )
                else:
                    draw_rank_badge(
                        draw, stats_x, y_center + 4,
                        "5th", (243, 232, 255), (168, 85, 247), (126, 34, 206),
                        "5th PLACE (38.47s · 26k eps)", (124, 58, 237),
                        font_pill, font_badge
                    )
            elif lane["drops"] > 0 and sim_time > 3.0:
                pill_w = 46
                pill_h = 28
                draw.rounded_rectangle([stats_x, y_center + 4, stats_x + pill_w, y_center + 4 + pill_h], radius=14, fill=(254, 226, 226), outline=(239, 68, 68), width=2)
                bbox = font_pill.getbbox("!")
                tw = bbox[2] - bbox[0]
                th = bbox[3] - bbox[1]
                draw.text((stats_x + (pill_w - tw) // 2, y_center + 4 + (pill_h - th) // 2 - 2), "!", font=font_pill, fill=(185, 28, 28))
                draw.text((stats_x + pill_w + 10, y_center + 7), "145k DROPS (14.6% Loss)", font=font_badge, fill=(220, 38, 38))
            else:
                pct = int(progress * 100)
                draw.text((stats_x, y_center + 8), f"Processing... {pct}%", font=font_lane_sub, fill=TEXT_MUTED)

        # 3. Always update & draw sparks across all lanes
        new_sparks = []
        for sp in sparks:
            sp["x"] += sp["vx"]
            sp["y"] += sp["vy"]
            sp["vx"] *= 0.95
            sp["vy"] *= 0.95
            sp["life"] -= dt * sp["decay"]

            if sp["life"] > 0:
                r = sp["size"] * sp["life"]
                if r > 0.5:
                    draw.ellipse([sp["x"] - r, sp["y"] - r, sp["x"] + r, sp["y"] + r], fill=sp["color"])
                new_sparks.append(sp)
        sparks = new_sparks

        # Send raw frame to FFmpeg stdin
        proc.stdin.write(img.tobytes())

        if (f_idx + 1) % 250 == 0:
            pct_done = int((f_idx + 1) / TOTAL_FRAMES * 100)
            print(f"Rendered {f_idx + 1}/{TOTAL_FRAMES} frames ({pct_done}%)...")

    proc.stdin.close()
    proc.wait()

    # Clean up temp audio file
    if os.path.exists(TEMP_AUDIO_FILE):
        try:
            os.remove(TEMP_AUDIO_FILE)
        except Exception:
            pass

    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\nSUCCESS! 60 FPS M3 Light Video with Audio created: {OUTPUT_FILE} ({file_size_mb:.2f} MB)")

if __name__ == "__main__":
    render_video()
