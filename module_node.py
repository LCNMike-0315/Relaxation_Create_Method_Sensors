#!/usr/bin/env python3
# Bottom-Layer Vital-Sign Sensor Node (0~10 per Middle Pi)
# Connects: Webcam + micro:bit (GSR, BPM, Temp) -> Computes personal baseline Z-scores & adaptability
# -> Streams 2-min summary & metrics to assigned Middle Pi.
# Broadcasts persistent BLE beacon (MODULE_ID) for proximity routing & resolves room from Top Pi.
# Automatically switches to simulated sensors if no micro:bit is connected (or with --sim).

from __future__ import annotations

import csv
import json
import math
import os
import random
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import cv2
import serial
from serial.tools import list_ports

# --- CONFIGURATION ---
MODULE_ID = os.getenv("MODULE_ID") or socket.gethostname().upper()
CAMERA_INDEX = int(os.getenv("CAMERA_INDEX", "0"))
BAUD = 115200
SIM_CONTROL_PORT = int(os.getenv("SIM_CONTROL_PORT", "8002"))
OUT_DIR = Path("data")
TOP_URL_FILE = Path("top_url.txt")         # Points to Top Pi (http://10.37.23.195:8000) for dynamic room routing
MIDDLE_URL_FILE = Path("middle_url.txt")   # Auto-updated cache / manual fallback if Top Pi is offline
QUEUE_FILE = OUT_DIR / f"{MODULE_ID}_unsent.jsonl"
HISTORY_FILE = OUT_DIR / f"{MODULE_ID}_history.json"
HEARTBEAT_INTERVAL_S = 5.0
HISTORY_MAX_LEN = 50

# Fallback baseline stats when personal history < 5 sessions (matches SPA_final_v9.ipynb)
DEFAULT_BASELINE_MEAN = [20.48, 77.74, 20.71, 36.0]
DEFAULT_BASELINE_STD = [6.02, 23.48, 5.70, 1.0]

RAW_HEADER = ["pi_time", "mb_ms", "gsr", "bpm", "temp_raw", "temp_c"]
SUMMARY_HEADER = [
    "timestamp", "module_id", "assigned_room", "duration_s",
    "eda_peaks_2min", "mean_bpm", "resp_rate_min", "mean_temp_c",
    "norm_eda", "norm_bpm", "norm_resp", "norm_temp",
    "adaptability_score", "sent_to_middle"
]

# Grove Temperature Sensor v1.2 (100k NTC thermistor, B=4250)
B_VALUE = 4250.0
R0 = 100000.0

route_lock = threading.Lock()
current_mid_id: str = "UNASSIGNED"
current_middle_url: str = ""
_bt_proc: subprocess.Popen | None = None

metrics_lock = threading.Lock()
history_buffer: deque[dict] = deque(maxlen=HISTORY_MAX_LEN)
latest_payload_cache: dict | None = None


# --- PURE-PYTHON STATISTICAL HELPERS (No numpy/scipy required) ---
def percentile_25(values: list[float]) -> float:
    s = sorted(values)
    n = len(s)
    if n == 0:
        return 0.0
    if n == 1:
        return float(s[0])
    pos = 0.25 * (n - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(s[lo])
    return float(s[lo] + (pos - lo) * (s[hi] - s[lo]))


def pop_std(values: list[float]) -> float:
    n = len(values)
    if n == 0:
        return 0.0
    mu = sum(values) / n
    return math.sqrt(sum((x - mu) ** 2 for x in values) / n)


def linear_slope(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den > 0 else 0.0


def load_personal_history():
    global latest_payload_cache
    if not HISTORY_FILE.exists():
        return
    try:
        raw = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            with metrics_lock:
                history_buffer.clear()
                for item in raw[-HISTORY_MAX_LEN:]:
                    if isinstance(item, dict) and "vitals" in item:
                        history_buffer.append(item)
                if history_buffer and "last_payload" in history_buffer[-1]:
                    latest_payload_cache = history_buffer[-1]["last_payload"]
    except Exception:
        pass


def save_personal_history_locked():
    OUT_DIR.mkdir(exist_ok=True)
    try:
        HISTORY_FILE.write_text(json.dumps(list(history_buffer), indent=2), encoding="utf-8")
    except OSError:
        pass


def compute_personal_metrics(
    eda_2min: float,
    avg_bpm: float,
    resp_rate: float,
    avg_temp_c: float,
    ts_iso: str,
) -> tuple[list[float], dict]:
    """
    Replicates cal() and calculate_adaptability_score() from SPA_final_v9.ipynb:
    - Uses 25th percentile baseline & std when history >= 5 points.
    - Computes slope + delta adaptability score across [E, H, R, BodyTemp].
    """
    clean_vitals = [
        float(eda_2min) if not math.isnan(eda_2min) else DEFAULT_BASELINE_MEAN[0],
        float(avg_bpm) if not math.isnan(avg_bpm) else DEFAULT_BASELINE_MEAN[1],
        float(resp_rate) if not math.isnan(resp_rate) else DEFAULT_BASELINE_MEAN[2],
        float(avg_temp_c) if not math.isnan(avg_temp_c) else DEFAULT_BASELINE_MEAN[3],
    ]

    with metrics_lock:
        history_buffer.append({"ts": ts_iso, "vitals": clean_vitals})
        hist_list = [entry["vitals"] for entry in history_buffer]

        if len(hist_list) < 5:
            means = list(DEFAULT_BASELINE_MEAN)
            stds = list(DEFAULT_BASELINE_STD)
        else:
            means = [percentile_25([row[i] for row in hist_list]) for i in range(4)]
            stds = [pop_std([row[i] for row in hist_list]) for i in range(4)]

        norm_vec = []
        for i in range(4):
            s_val = stds[i] if (stds[i] > 0 and not math.isnan(stds[i])) else 1.0
            z = abs((clean_vitals[i] - means[i]) / s_val)
            norm_vec.append(round(z, 2))

        if len(hist_list) > 1:
            e_series = [row[0] for row in hist_list]
            h_series = [row[1] for row in hist_list]
            r_series = [row[2] for row in hist_list]
            bt_series = [row[3] for row in hist_list]

            emotion_slope = (
                linear_slope(e_series) + linear_slope(h_series) + linear_slope(r_series)
            ) / 3.0
            bodytemp_slope = linear_slope(bt_series)

            emotion_deltas = (
                abs(e_series[-1] - e_series[-2])
                + abs(h_series[-1] - h_series[-2])
                + abs(r_series[-1] - r_series[-2])
            )
            bodytemp_delta = abs(bt_series[-1] - bt_series[-2])

            slope_val = round(emotion_slope + bodytemp_slope, 3)
            delta_val = round(emotion_deltas + bodytemp_delta, 3)
            score_val = round(slope_val + delta_val, 3)
        else:
            slope_val = 0.0
            delta_val = 0.0
            score_val = 0.0

        adapt_dict = {"slope": slope_val, "delta": delta_val, "score": score_val}
        return norm_vec, adapt_dict


def get_top_url() -> str:
    env_url = os.getenv("TOP_URL", "").strip()
    if env_url:
        return env_url.rstrip("/")
    if TOP_URL_FILE.exists():
        text = TOP_URL_FILE.read_text(encoding="utf-8").strip()
        if text:
            return text.rstrip("/")
    return ""


def get_cached_middle_url() -> str:
    env_url = os.getenv("MIDDLE_URL", "").strip()
    if env_url:
        return env_url.rstrip("/")
    if MIDDLE_URL_FILE.exists():
        text = MIDDLE_URL_FILE.read_text(encoding="utf-8").strip()
        if text:
            return text.rstrip("/")
    return ""


def get_active_route() -> tuple[str, str]:
    with route_lock:
        if current_middle_url:
            return current_mid_id, current_middle_url
    return current_mid_id, get_cached_middle_url()


def post_json(url: str, payload: dict, timeout: float = 4.0) -> bool:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def get_json(url: str, timeout: float = 3.0) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            if 200 <= resp.status < 300:
                return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
        pass
    return None


def sync_dynamic_route():
    """Queries Top Pi for current room assignment; detaches cleanly from old room if changed."""
    global current_mid_id, current_middle_url
    top_url = get_top_url()
    if not top_url:
        cached = get_cached_middle_url()
        with route_lock:
            current_middle_url = cached
        return

    q = urllib.parse.urlencode({"mod_id": MODULE_ID})
    res = get_json(f"{top_url}/api/route?{q}", timeout=3.0)
    if not res or not res.get("middle_url"):
        return

    new_mid = str(res.get("mid_id", "UNKNOWN")).strip().upper()
    new_url = str(res["middle_url"]).strip().rstrip("/")
    mode = res.get("mode", "auto")

    old_url = ""
    old_mid = ""
    switched = False
    with route_lock:
        if new_url != current_middle_url:
            old_url = current_middle_url
            old_mid = current_mid_id
            current_mid_id = new_mid
            current_middle_url = new_url
            switched = True
        else:
            current_mid_id = new_mid

    if switched:
        try:
            MIDDLE_URL_FILE.write_text(new_url + "\n", encoding="utf-8")
        except OSError:
            pass
        mode_label = "automatically" if mode == "auto" else mode
        if old_url:
            post_json(f"{old_url}/api/detach", {"module_id": MODULE_ID}, timeout=2.5)
            print(f"\n[{MODULE_ID}] ROOM SWITCH ({mode_label}): {old_mid} -> {new_mid} ({new_url})\n")
        else:
            print(f"[{MODULE_ID}] Assigned Room ({mode_label}): {new_mid} ({new_url})")


def start_ble_beacon():
    """Enables persistent BLE advertising with Local Name = MODULE_ID."""
    global _bt_proc
    if shutil.which("rfkill"):
        try:
            subprocess.run(["rfkill", "unblock", "bluetooth"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
        except Exception:
            pass

    if shutil.which("btmgmt"):
        for cmd in [
            ["btmgmt", "power", "on"],
            ["btmgmt", "le", "on"],
            ["btmgmt", "connectable", "on"],
            ["btmgmt", "discov", "on"],
            ["btmgmt", "name", MODULE_ID],
            ["btmgmt", "advertising", "on"],
        ]:
            try:
                subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
            except Exception:
                pass

    if shutil.which("bluetoothctl") and _bt_proc is None:
        try:
            _bt_proc = subprocess.Popen(
                ["bluetoothctl"],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            if _bt_proc.stdin:
                _bt_proc.stdin.write(f"power on\nsystem-alias {MODULE_ID}\nadvertise on\n")
                _bt_proc.stdin.flush()
            print(f"[{MODULE_ID}] BLE beacon active (advertising as '{MODULE_ID}').")
        except Exception as exc:
            print(f"[{MODULE_ID}] Note: BLE beacon failed to start ({exc}).")


def stop_ble_beacon():
    global _bt_proc
    if _bt_proc is not None:
        try:
            if _bt_proc.stdin:
                _bt_proc.stdin.write("advertise off\nquit\n")
                _bt_proc.stdin.flush()
            _bt_proc.terminate()
        except Exception:
            pass
        _bt_proc = None


def flush_unsent_queue(middle_url: str):
    if not middle_url or not QUEUE_FILE.exists():
        return
    lines = QUEUE_FILE.read_text(encoding="utf-8").splitlines()
    if not lines:
        return

    remaining = []
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not post_json(f"{middle_url}/api/bottom", payload):
            remaining.append(line)

    if remaining:
        QUEUE_FILE.write_text("\n".join(remaining) + "\n", encoding="utf-8")
    else:
        QUEUE_FILE.unlink(missing_ok=True)


def heartbeat_worker(stop_event: threading.Event):
    """Syncs room assignment from Top Pi and registers heartbeat (with latest metrics) on Middle Pi."""
    while not stop_event.is_set():
        sync_dynamic_route()
        mid_id, middle_url = get_active_route()
        if middle_url:
            with metrics_lock:
                cached = dict(latest_payload_cache) if latest_payload_cache else None
            if cached is not None:
                cached["assigned_room"] = mid_id
            payload = {
                "module_id": MODULE_ID,
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "latest_reading": cached,
            }
            ok = post_json(f"{middle_url}/api/heartbeat", payload, timeout=3.0)
            if ok:
                flush_unsent_queue(middle_url)
        stop_event.wait(HEARTBEAT_INTERVAL_S)


def raw_to_celsius(adc_val: int) -> float:
    if adc_val <= 0 or adc_val >= 1023:
        return float("nan")
    r = (1023.0 / adc_val - 1.0) * R0
    temp_k = 1.0 / (math.log(r / R0) / B_VALUE + (1.0 / 298.15))
    return temp_k - 273.15


def count_eda_peaks(gsr_values: list[int], fs: float = 5.0, min_prominence: float = 1.2) -> int:
    if len(gsr_values) < int(fs * 5):
        return 0

    win = max(1, int(fs * 0.6))
    smoothed = [
        sum(gsr_values[max(0, i - win):i + 1]) / len(gsr_values[max(0, i - win):i + 1])
        for i in range(len(gsr_values))
    ]

    base_win = max(1, int(fs * 10))
    phasic_vals = []
    for i in range(len(smoothed)):
        baseline = sum(smoothed[max(0, i - base_win):i + 1]) / len(smoothed[max(0, i - base_win):i + 1])
        phasic_vals.append(abs(baseline - smoothed[i]))

    peaks = 0
    in_peak = False
    min_gap_samples = int(fs * 1.5)
    last_peak_idx = -min_gap_samples

    for i, val in enumerate(phasic_vals):
        if val >= min_prominence and not in_peak and (i - last_peak_idx) >= min_gap_samples:
            peaks += 1
            in_peak = True
            last_peak_idx = i
        elif val < (min_prominence * 0.5):
            in_peak = False

    return peaks


def run_respiration_tracker(stop_event: threading.Event, result_container: dict, sim_mode: bool = False):
    cap = cv2.VideoCapture(CAMERA_INDEX, cv2.CAP_V4L2)
    if not cap.isOpened():
        cap = cv2.VideoCapture(CAMERA_INDEX)
    if not cap.isOpened():
        if sim_mode:
            result_container["resp_rate"] = random.randint(14, 18)
        else:
            print(f"  [!] Warning: Could not open webcam at index {CAMERA_INDEX}")
            result_container["resp_rate"] = 0
        return

    before = None
    breath_cycles = 0
    in_breath = False
    last_breath_t = 0.0
    start_time = time.time()

    MOTION_HIGH = 1.5
    MOTION_LOW = 0.8
    MIN_BREATH_INTERVAL_S = 1.5

    while not stop_event.is_set():
        ret, frame = cap.read()
        if not ret:
            time.sleep(0.05)
            continue

        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        if before is None:
            before = gray.astype("float")
            continue

        cv2.accumulateWeighted(gray, before, 0.5)
        frame_delta = cv2.absdiff(gray, cv2.convertScaleAbs(before))
        thresh = cv2.threshold(frame_delta, 3, 255, cv2.THRESH_BINARY)[1]

        mean_val = float(thresh.mean())
        now = time.time()

        if MOTION_HIGH <= mean_val <= 60.0:
            if not in_breath and (now - last_breath_t) >= MIN_BREATH_INTERVAL_S:
                breath_cycles += 1
                in_breath = True
                last_breath_t = now
        elif mean_val < MOTION_LOW:
            in_breath = False

        time.sleep(0.05)

    cap.release()
    elapsed_min = max((time.time() - start_time) / 60.0, 1e-6)
    result_container["resp_rate"] = round(breath_cycles / elapsed_min)


class SimulatedMicrobit:
    """Emulates micro:bit serial output (5 Hz GSR, BPM, Temp) with keyboard and HTTP triggers."""

    def __init__(self, control_port: int = SIM_CONTROL_PORT):
        self.boot_t = time.time()
        self.recording = False
        self.rec_start_t = 0.0
        self.pending_cmd: str | None = None
        self.lock = threading.Lock()
        self.base_gsr = random.randint(610, 670)
        self.base_bpm = random.randint(68, 82)
        self.base_temp_adc = random.randint(512, 535)

        self._start_keyboard_listener()
        self._start_http_trigger(control_port)

    def trigger_start(self):
        with self.lock:
            if not self.recording:
                self.recording = True
                self.rec_start_t = time.time()
                self.pending_cmd = "#START"

    def trigger_stop(self):
        with self.lock:
            if self.recording:
                self.recording = False
                self.pending_cmd = "#STOP"

    def toggle(self):
        with self.lock:
            if self.recording:
                self.recording = False
                self.pending_cmd = "#STOP"
            else:
                self.recording = True
                self.rec_start_t = time.time()
                self.pending_cmd = "#START"

    def _start_keyboard_listener(self):
        def _watch_stdin():
            while True:
                try:
                    line = sys.stdin.readline()
                    if not line:
                        break
                    self.toggle()
                except Exception:
                    break

        threading.Thread(target=_watch_stdin, daemon=True).start()

    def _start_http_trigger(self, port: int):
        sim_ref = self

        class SimControlHandler(BaseHTTPRequestHandler):
            def do_POST(self):
                if self.path == "/start":
                    sim_ref.trigger_start()
                    self._reply(200, {"status": "started", "module_id": MODULE_ID})
                elif self.path == "/stop":
                    sim_ref.trigger_stop()
                    self._reply(200, {"status": "stopped", "module_id": MODULE_ID})
                else:
                    self._reply(404, {"error": "Use POST /start or POST /stop"})

            def _reply(self, code: int, body: dict):
                raw = json.dumps(body).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, format, *args):
                return

        try:
            server = HTTPServer(("0.0.0.0", port), SimControlHandler)
            threading.Thread(target=server.serve_forever, daemon=True).start()
        except OSError as exc:
            print(f"[{MODULE_ID}] Note: HTTP sim trigger port {port} unavailable ({exc})")

    def readline(self) -> str:
        with self.lock:
            if self.pending_cmd:
                cmd = self.pending_cmd
                self.pending_cmd = None
                return cmd

        if not self.recording:
            time.sleep(0.1)
            return ""

        elapsed = time.time() - self.rec_start_t
        if elapsed >= 120.0:
            with self.lock:
                self.recording = False
            return "#STOP"

        time.sleep(0.2)
        mb_ms = int((time.time() - self.boot_t) * 1000)

        cycle_pos = elapsed % 14.0
        spike = int(14.0 * math.exp(-((cycle_pos - 4.0) ** 2) / 1.6)) if 1.5 <= cycle_pos <= 7.5 else 0
        gsr = self.base_gsr + spike + random.randint(-2, 2)
        bpm = max(55, min(115, self.base_bpm + int(4 * math.sin(elapsed / 9.0)) + random.randint(-2, 2)))
        temp_raw = self.base_temp_adc + random.randint(-1, 1)
        return f"{mb_ms},{gsr},{bpm},{temp_raw}"


def find_port() -> str | None:
    for p in list_ports.comports():
        if p.vid == 0x0D28:
            return p.device
    return None


def mean(values: list[float]) -> float:
    valid = [v for v in values if not math.isnan(v)]
    return sum(valid) / len(valid) if valid else float("nan")


def finalize_session(rows: list[tuple], resp_result: dict, wall_duration_s: float):
    global latest_payload_cache
    if not rows:
        print("  [!] No data rows recorded.")
        sync_dynamic_route()
        return

    gsr_list = [r[0] for r in rows]
    bpm_list = [r[1] for r in rows if r[1] > 0]
    temp_c_list = [r[3] for r in rows]

    eda_2min = count_eda_peaks(gsr_list, fs=5.0)
    avg_bpm = mean(bpm_list)
    resp_rate = int(resp_result.get("resp_rate", 0))
    avg_temp_c = mean(temp_c_list)
    ts_iso = datetime.now().isoformat(timespec="seconds")

    norm_vec, adapt_dict = compute_personal_metrics(
        eda_2min=eda_2min,
        avg_bpm=avg_bpm,
        resp_rate=resp_rate,
        avg_temp_c=avg_temp_c,
        ts_iso=ts_iso,
    )

    mid_id, middle_url = get_active_route()

    payload = {
        "timestamp": ts_iso,
        "module_id": MODULE_ID,
        "assigned_room": mid_id,
        "duration_s": round(wall_duration_s, 1),
        "eda_peaks_2min": eda_2min,
        "mean_bpm": round(avg_bpm, 1) if not math.isnan(avg_bpm) else None,
        "resp_rate_min": resp_rate,
        "mean_temp_c": round(avg_temp_c, 2) if not math.isnan(avg_temp_c) else None,
        "norm_vec": norm_vec,
        "adaptability": adapt_dict,
        "adaptability_score": adapt_dict["score"],
    }

    with metrics_lock:
        latest_payload_cache = dict(payload)
        if history_buffer:
            history_buffer[-1]["last_payload"] = dict(payload)
        save_personal_history_locked()

    sent_ok = False
    if middle_url:
        sent_ok = post_json(f"{middle_url}/api/bottom", payload)
        if not sent_ok:
            with open(QUEUE_FILE, "a", encoding="utf-8") as qf:
                qf.write(json.dumps(payload) + "\n")

    print("\n" + "=" * 56)
    print(f"MODULE {MODULE_ID} ({mid_id}) — 2-MIN RESULT ({wall_duration_s:.1f}s)")
    print(f"  1. EDA / 2min          : {eda_2min} (GSR range: {min(gsr_list)}-{max(gsr_list)})")
    print(f"  2. Heart Rate (BPM)    : {avg_bpm:.1f}")
    print(f"  3. Respiration Rate    : {resp_rate}")
    print(f"  4. Avg Temperature     : {avg_temp_c:.2f} °C")
    print(f"  5. Norm Vector (Z)     : {norm_vec}")
    print(f"  6. Adaptability Score  : {adapt_dict['score']} (slope={adapt_dict['slope']}, delta={adapt_dict['delta']})")
    print(f"  -> Middle Pi ({mid_id} @ {middle_url or 'UNSET'}): {'SENT' if sent_ok else 'QUEUED/LOCAL'}")
    print("=" * 56 + "\n")

    summary_path = OUT_DIR / f"{MODULE_ID}_summary.csv"
    write_header = not summary_path.exists()
    with open(summary_path, "a", newline="") as sf:
        sw = csv.writer(sf)
        if write_header:
            sw.writerow(SUMMARY_HEADER)
        sw.writerow([
            ts_iso,
            MODULE_ID,
            mid_id,
            round(wall_duration_s, 1),
            eda_2min,
            payload["mean_bpm"] if payload["mean_bpm"] is not None else "",
            resp_rate,
            payload["mean_temp_c"] if payload["mean_temp_c"] is not None else "",
            norm_vec[0],
            norm_vec[1],
            norm_vec[2],
            norm_vec[3],
            adapt_dict["score"],
            "1" if sent_ok else "0",
        ])

    sync_dynamic_route()


def run_capture_loop(read_line_fn, sim_mode: bool, hb_stop: threading.Event):
    f = writer = None
    rows = []
    session_start_wall = 0.0
    resp_stop_event = None
    resp_thread = None
    resp_result = {}

    try:
        while True:
            line = read_line_fn()
            if not line:
                continue

            if line == "#START":
                if f:
                    f.close()
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                raw_path = OUT_DIR / f"{MODULE_ID}_raw_{ts}.csv"

                f = open(raw_path, "w", newline="")
                writer = csv.writer(f)
                writer.writerow(RAW_HEADER)
                rows = []
                session_start_wall = time.time()

                resp_result = {"resp_rate": 0}
                resp_stop_event = threading.Event()
                resp_thread = threading.Thread(
                    target=run_respiration_tracker,
                    args=(resp_stop_event, resp_result, sim_mode),
                    daemon=True,
                )
                resp_thread.start()
                mode_tag = " [SIM]" if sim_mode else ""
                mid_id, _ = get_active_route()
                print(f"[{ts}] START{mode_tag} (Room: {mid_id}) -> Recording 2 min...")

            elif line == "#STOP":
                if writer is None:
                    continue
                if f:
                    f.close()
                    f = writer = None

                wall_duration_s = time.time() - session_start_wall if session_start_wall > 0 else 120.0

                if resp_stop_event and resp_thread:
                    resp_stop_event.set()
                    resp_thread.join(timeout=5.0)

                print("STOP received.")
                finalize_session(rows, resp_result, wall_duration_s)

            elif writer is not None:
                parts = line.split(",")
                if len(parts) != 4:
                    continue
                try:
                    mb_ms, gsr, bpm, temp_raw = (int(x) for x in parts)
                except ValueError:
                    continue

                temp_c = raw_to_celsius(temp_raw)
                writer.writerow([
                    datetime.now().isoformat(timespec="milliseconds"),
                    mb_ms, gsr, bpm, temp_raw, round(temp_c, 2)
                ])
                f.flush()
                rows.append((gsr, bpm, temp_raw, temp_c))

                if sim_mode and len(rows) % 10 == 0:
                    print(f"  [SIM] samples={len(rows)} | gsr={gsr}, bpm={bpm}, temp={temp_c:.2f}°C")

    except KeyboardInterrupt:
        print("\nExiting.")
        hb_stop.set()
        if resp_stop_event:
            resp_stop_event.set()
    finally:
        hb_stop.set()
        stop_ble_beacon()
        if f:
            f.close()


def main():
    force_sim = "--sim" in sys.argv
    cli_args = [a for a in sys.argv[1:] if a != "--sim"]
    port = None if force_sim else (cli_args[0] if cli_args else find_port())

    OUT_DIR.mkdir(exist_ok=True)
    load_personal_history()
    start_ble_beacon()
    sync_dynamic_route()

    hb_stop = threading.Event()
    threading.Thread(target=heartbeat_worker, args=(hb_stop,), daemon=True).start()

    mid_id, middle_url = get_active_route()
    if port:
        print(f"[{MODULE_ID}] Connected to {port} @ {BAUD} baud.")
        print(f"[{MODULE_ID}] Top Pi Directory: {get_top_url() or 'Unset'} | Active Room: {mid_id} ({middle_url or 'Unset'})")
        print("Press Button A on the micro:bit to start a 2-minute capture...")
        with serial.Serial(port, BAUD, timeout=1) as ser:
            ser.reset_input_buffer()
            run_capture_loop(
                lambda: ser.readline().decode("utf-8", errors="ignore").strip(),
                sim_mode=False,
                hb_stop=hb_stop,
            )
    else:
        print(f"[{MODULE_ID}] no microbit — switching to simulation sensors")
        print(f"[{MODULE_ID}] Top Pi Directory: {get_top_url() or 'Unset'} | Active Room: {mid_id} ({middle_url or 'Unset'})")
        print(f"[{MODULE_ID}] Press Enter (or POST http://<PI-IP>:{SIM_CONTROL_PORT}/start & /stop) to toggle capture...")
        sim = SimulatedMicrobit(SIM_CONTROL_PORT)
        run_capture_loop(sim.readline, sim_mode=True, hb_stop=hb_stop)


if __name__ == "__main__":
    main()