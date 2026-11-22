#!/usr/bin/env python3
# Ubuntu Raspberry Pi Vital-Sign Module Coordinator
# Connects: Webcam + micro:bit (GSR, BPM, Temp) -> Outputs 2-min summary

import csv
import math
import os
import socket
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import cv2
import serial
from serial.tools import list_ports

# --- CONFIGURATION ---
# Reads MODULE_ID from environment variable, or defaults to the Pi's hostname (e.g. MOD-01)
MODULE_ID = os.getenv("MODULE_ID") or socket.gethostname().upper()
CAMERA_INDEX = int(os.getenv("CAMERA_INDEX", "0"))
BAUD = 115200
OUT_DIR = Path("data")

RAW_HEADER = ["pi_time", "mb_ms", "gsr", "bpm", "temp_raw", "temp_c"]
SUMMARY_HEADER = [
    "timestamp", "module_id", "duration_s",
    "eda_peaks_2min", "mean_bpm", "resp_rate_min", "mean_temp_c"
]

# Grove Temperature Sensor v1.2 (100k NTC thermistor, B=4250)
B_VALUE = 4250.0
R0 = 100000.0


def raw_to_celsius(adc_val: int) -> float:
    if adc_val <= 0 or adc_val >= 1023:
        return float("nan")
    r = (1023.0 / adc_val - 1.0) * R0
    temp_k = 1.0 / (math.log(r / R0) / B_VALUE + (1.0 / 298.15))
    return temp_k - 273.15


def count_eda_peaks(gsr_values: list[int], fs: float = 5.0, min_prominence: float = 1.2) -> int:
    """
    Counts phasic Skin Conductance Responses (SCR) over the 2-minute window.
    Uses a fixed prominence threshold and detects bidirectional phasic deflections
    relative to a 10s rolling baseline.
    """
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


def run_respiration_tracker(stop_event: threading.Event, result_container: dict):
    """
    Uses V4L2 webcam capture and rising-edge breath cycle detection (with a 1.5s cooldown
    so individual frames within the same breath are not counted multiple times).
    Outputs integer breaths/min.
    """
    cap = cv2.VideoCapture(CAMERA_INDEX, cv2.CAP_V4L2)
    if not cap.isOpened():
        cap = cv2.VideoCapture(CAMERA_INDEX)
    if not cap.isOpened():
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


def find_port() -> str | None:
    for p in list_ports.comports():
        if p.vid == 0x0D28:  # micro:bit DAPLink
            return p.device
    return None


def mean(values: list[float]) -> float:
    valid = [v for v in values if not math.isnan(v)]
    return sum(valid) / len(valid) if valid else float("nan")


def finalize_session(rows: list[tuple], resp_result: dict, wall_duration_s: float):
    if not rows:
        print("  [!] No data rows recorded.")
        return

    gsr_list = [r[0] for r in rows]
    bpm_list = [r[1] for r in rows if r[1] > 0]
    temp_c_list = [r[3] for r in rows]

    eda_2min = count_eda_peaks(gsr_list, fs=5.0)
    avg_bpm = mean(bpm_list)
    resp_rate = int(resp_result.get("resp_rate", 0))
    avg_temp_c = mean(temp_c_list)

    print("\n" + "=" * 52)
    print(f"MODULE {MODULE_ID} — 2-MIN RESULT ({wall_duration_s:.1f}s)")
    print(f"  1. EDA / 2min          : {eda_2min} (GSR range: {min(gsr_list)}-{max(gsr_list)})")
    print(f"  2. Heart Rate (BPM)    : {avg_bpm:.1f}")
    print(f"  3. Respiration Rate    : {resp_rate}")
    print(f"  4. Avg Temperature     : {avg_temp_c:.2f} °C")
    print("=" * 52 + "\n")

    summary_path = OUT_DIR / f"{MODULE_ID}_summary.csv"
    write_header = not summary_path.exists()
    with open(summary_path, "a", newline="") as sf:
        sw = csv.writer(sf)
        if write_header:
            sw.writerow(SUMMARY_HEADER)
        sw.writerow([
            datetime.now().isoformat(timespec="seconds"),
            MODULE_ID,
            round(wall_duration_s, 1),
            eda_2min,
            round(avg_bpm, 1) if not math.isnan(avg_bpm) else "",
            resp_rate,
            round(avg_temp_c, 2) if not math.isnan(avg_temp_c) else "",
        ])


def main():
    port = sys.argv[1] if len(sys.argv) > 1 else find_port()
    if not port:
        sys.exit("micro:bit not found. Check `ls /dev/ttyACM*` or USB cable.")

    OUT_DIR.mkdir(exist_ok=True)
    print(f"[{MODULE_ID}] Connected to {port} @ {BAUD} baud.")
    print("Press Button A on the micro:bit to start a 2-minute capture...")

    f = writer = None
    rows = []
    session_start_wall = 0.0
    resp_stop_event = None
    resp_thread = None
    resp_result = {}

    with serial.Serial(port, BAUD, timeout=1) as ser:
        ser.reset_input_buffer()
        try:
            while True:
                line = ser.readline().decode("utf-8", errors="ignore").strip()
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
                        args=(resp_stop_event, resp_result),
                        daemon=True,
                    )
                    resp_thread.start()
                    print(f"[{ts}] START -> Recording 2 min...")

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

        except KeyboardInterrupt:
            print("\nExiting.")
            if resp_stop_event:
                resp_stop_event.set()
        finally:
            if f:
                f.close()


if __name__ == "__main__":
    main()