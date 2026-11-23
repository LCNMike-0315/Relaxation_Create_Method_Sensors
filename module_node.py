#!/usr/bin/env python3
# Bottom-Layer Vital-Sign Sensor Node (0~10 per Middle Pi)
# Connects: Webcam + micro:bit (GSR, BPM, Temp) -> Streams 2-min summary to assigned Middle Pi.
# Dynamically resolves its assigned room (MID-01..03) from Top Pi every 5s, with local fallback.
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
HEARTBEAT_INTERVAL_S = 5.0

RAW_HEADER = ["pi_time", "mb_ms", "gsr", "bpm", "temp_raw", "temp_c"]
SUMMARY_HEADER = [
    "timestamp", "module_id", "assigned_room", "duration_s",
    "eda_peaks_2min", "mean_bpm", "resp_rate_min", "mean_temp_c", "sent_to_middle"
]

# Grove Temperature Sensor v1.2 (100k NTC thermistor, B=4250)
B_VALUE = 4250.0
R0 = 100000.0

route_lock = threading.Lock()
current_mid_id: str = "UNASSIGNED"
current_middle_url: str = ""


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
        if old_url:
            post_json(f"{old_url}/api/detach", {"module_id": MODULE_ID}, timeout=2.5)
            print(f"\n[{MODULE_ID}] ROOM SWITCH ({mode}): {old_mid} -> {new_mid} ({new_url})\n")
        else:
            print(f"[{MODULE_ID}] Assigned Room ({mode}): {new_mid} ({new_url})")


def start_ble_beacon():
    """Sets Bluetooth adapter alias to MODULE_ID and enables advertising if bluez tools exist."""
    if not shutil.which("bluetoothctl"):
        return
    try:
        subprocess.run(["bluetoothctl", "power", "on"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
        subprocess.run(["bluetoothctl", "system-alias", MODULE_ID], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
        subprocess.run(["bluetoothctl", "advertise", "on"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
    except Exception:
        pass


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
    """Syncs room assignment from Top Pi and registers heartbeat with assigned Middle Pi."""
    while not stop_event.is_set():
        sync_dynamic_route()
        _mid_id, middle_url = get_active_route()
        if middle_url:
            payload = {
                "module_id": MODULE_ID,
                "timestamp": datetime.now().isoformat(timespec="seconds"),
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
        baseline = sum(smoothed[max(0, i - base_win):i + 1]) / len(smoothed