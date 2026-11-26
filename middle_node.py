#!/usr/bin/env python3
# Middle-Layer Raspberry Pi Aggregator (3 Middle Pis: MID-01, MID-02, MID-03)
# Receives from 0~10 dynamic Bottom Pis -> Computes Room Context Vectors, Cosine Similarities,
# Conflict Detection, and Room Device/HVAC Controls -> Forwards to Top Pi.
# Supports clean /api/detach on room switches and BLE RSSI room scanning.

from __future__ import annotations

import asyncio
import csv
import json
import math
import os
import socket
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# --- CONFIGURATION ---
MIDDLE_ID = os.getenv("MIDDLE_ID") or socket.gethostname().upper()
LISTEN_PORT = int(os.getenv("MIDDLE_PORT", "8001"))
TOP_URL_FILE = Path("top_url.txt")
OUT_DIR = Path("data")
ACTIVE_TIMEOUT_S = 40.0     # Bottom Pi removed from active list if silent > 40s
SYNC_INTERVAL_S = 15.0      # Periodic heartbeat/roster sync to Top Pi
BLE_SCAN_INTERVAL_S = 3.0   # BLE proximity scan cadence (if bleak is installed)

# --- CONTEXT & ENVIRONMENTAL CONTROL PARAMETERS (from SPA_final_v9.ipynb) ---
INDICATOR_WEIGHTS = [0.4, 0.3, 0.2, 0.1]
COS_SIM_CONFLICT_THRESHOLD = 0.95
EMOTION_CONTROL_THRESHOLD = 0.8
TEMP_THRESHOLD_C = 23.0
ROOM_CAPACITY = 10.0

# Fixed fallback room temperature until physical sensor is attached to mid-01/02/03
DEFAULT_RM_TEMP_C = float(os.getenv("RM_TEMP", "23.0"))

RECEIVED_HEADER = [
    "received_at", "middle_id", "module_id", "duration_s",
    "eda_peaks_2min", "mean_bpm", "resp_rate_min", "mean_temp_c",
    "adaptability_score"
]

state_lock = threading.Lock()
# active_nodes[module_id] = {"last_seen": float, "ip": str, "latest_reading": dict | None}
active_nodes: dict[str, dict] = {}


def read_room_temperature() -> float:
    """
    Returns current room temperature (°C).
    When adding a physical temperature sensor (e.g., BME280/SHT31) to mid-01/mid-02/mid-03,
    implement the hardware read inside [HARDWARE SENSOR HOOK] below.
    """
    # --- [HARDWARE SENSOR HOOK] ---------------------------------
    # try:
    #     return round(read_physical_temp_sensor(), 2)
    # except Exception:
    #     pass
    # ------------------------------------------------------------
    return round(DEFAULT_RM_TEMP_C, 2)


def cosine_similarity(v1: list[float], v2: list[float]) -> float:
    dot = sum(a * b for a, b in zip(v1, v2))
    n1 = math.sqrt(sum(a * a for a in v1))
    n2 = math.sqrt(sum(b * b for b in v2))
    return dot / (n1 * n2) if (n1 > 0 and n2 > 0) else 0.0


def control_room_devices(state_vector: list[float], current_temp_c: float) -> dict:
    """Determines actuator emotion setting & AC target temperature."""
    emo_vec = state_vector[:3]
    max_emo = max(emo_vec) if emo_vec else 0.0
    dom_idx = emo_vec.index(max_emo) if emo_vec else 0

    emotion_setting = "Random"
    if max_emo > EMOTION_CONTROL_THRESHOLD:
        emotion_map = {0: "Anxiety", 1: "Displeasure", 2: "Anger"}
        emotion_setting = emotion_map.get(dom_idx, "Random")

    target_temp = 22.0 if current_temp_c >= TEMP_THRESHOLD_C else 25.0
    return {
        "device_setting": emotion_setting,
        "ac_target_temp_c": target_temp,
    }


def compute_room_context_locked(rm_temp_c: float) -> dict:
    """
    Computes room context vectors across 3 strategies (Equality, Equality With Weight, Fairness),
    cosine similarities against the group sum vector, conflict detection (< 0.95),
    and room device/HVAC commands without numpy/scipy.
    """
    ids_with_vecs = []
    norm_by_id: dict[str, list[float]] = {}
    score_by_id: dict[str, float] = {}

    for mod_id in sorted(active_nodes.keys()):
        r = active_nodes[mod_id].get("latest_reading")
        if r and isinstance(r.get("norm_vec"), list) and len(r["norm_vec"]) >= 4:
            ids_with_vecs.append(mod_id)
            norm_by_id[mod_id] = [float(x) for x in r["norm_vec"][:4]]
            score_by_id[mod_id] = float(r.get("adaptability_score", 0.0))

    crowdedness = round(len(active_nodes) / ROOM_CAPACITY, 2)

    if not ids_with_vecs:
        zero_vec = [0.0, 0.0, 0.0, 0.0]
        return {
            "evaluated_count": 0,
            "rm_temp_c": rm_temp_c,
            "crowdedness": crowdedness,
            "strategy_vectors": {
                "Equality": zero_vec,
                "Equality With Weight": zero_vec,
                "Fairness": zero_vec,
            },
            "cosine_similarity": {
                "Equality": None,
                "Equality With Weight": None,
                "Fairness": None,
            },
            "conflict_detected": False,
            "conflict_outlier": None,
            "priority_weights": {},
            "fairness_target": None,
            "room_control": control_room_devices(zero_vec, rm_temp_c),
        }

    n = len(ids_with_vecs)

    # 1. Priorities for each strategy
    pri_equality = [1.0] * n

    total_score = sum(score_by_id[pid] for pid in ids_with_vecs)
    if total_score == 0 or n == 1:
        pri_eq_weight = [1.0] * n
    else:
        pri_eq_weight = [1.0 - (score_by_id[pid] / total_score) for pid in ids_with_vecs]

    fairness_target = min(ids_with_vecs, key=lambda pid: score_by_id[pid])
    pri_fairness = [1.0 if pid == fairness_target else 0.0 for pid in ids_with_vecs]

    strategy_pri_map = {
        "Equality": pri_equality,
        "Equality With Weight": pri_eq_weight,
        "Fairness": pri_fairness,
    }

    # 2. Diagonal extraction equivalent: state_vec[j] = weights[j] * sum_i(pri[i] * I[i][j])
    strategy_vectors: dict[str, list[float]] = {}
    for strat_name, pris in strategy_pri_map.items():
        vec = []
        for j in range(4):
            weighted_sum = sum(pris[i] * norm_by_id[ids_with_vecs[i]][j] for i in range(n))
            vec.append(round(INDICATOR_WEIGHTS[j] * weighted_sum, 4))
        strategy_vectors[strat_name] = vec

    # 3. Cosine similarity against 3-emotion simple sum vector
    group_emo_sum = [
        sum(norm_by_id[pid][j] for pid in ids_with_vecs)
        for j in range(3)
    ]

    cos_sims: dict[str, float] = {}
    conflict_detected = False
    for strat_name, vec in strategy_vectors.items():
        sim = round(cosine_similarity(vec[:3], group_emo_sum), 4)
        cos_sims[strat_name] = sim
        if sum(group_emo_sum) > 0 and sim < COS_SIM_CONFLICT_THRESHOLD:
            conflict_detected = True

    # 4. Room device & AC control driven by Equality With Weight context vector
    room_control = control_room_devices(strategy_vectors["Equality With Weight"], rm_temp_c)

    priority_weights = {
        pid: round(pri_eq_weight[idx], 3)
        for idx, pid in enumerate(ids_with_vecs)
    }

    return {
        "evaluated_count": n,
        "rm_temp_c": rm_temp_c,
        "crowdedness": crowdedness,
        "strategy_vectors": strategy_vectors,
        "cosine_similarity": cos_sims,
        "conflict_detected": conflict_detected,
        "conflict_outlier": fairness_target if conflict_detected else None,
        "priority_weights": priority_weights,
        "fairness_target": fairness_target,
        "room_control": room_control,
    }


def get_top_url() -> str:
    env_url = os.getenv("TOP_URL", "").strip()
    if env_url:
        return env_url.rstrip("/")
    if TOP_URL_FILE.exists():
        text = TOP_URL_FILE.read_text(encoding="utf-8").strip()
        if text:
            return text.rstrip("/")
    return ""


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


def prune_stale_nodes_locked(now: float):
    stale_ids = [
        mid for mid, info in active_nodes.items()
        if (now - info["last_seen"]) > ACTIVE_TIMEOUT_S
    ]
    for mid in stale_ids:
        del active_nodes[mid]
        print(f"[{MIDDLE_ID}] Bottom node detached (timeout): {mid}")


def build_group_packet_locked(event_type: str, trigger_module: str | None = None) -> dict:
    now = time.time()
    prune_stale_nodes_locked(now)

    rm_temp_c = read_room_temperature()
    context_analysis = compute_room_context_locked(rm_temp_c)

    readings = [
        info["latest_reading"]
        for info in active_nodes.values()
        if info["latest_reading"] is not None
    ]

    def avg_field(key: str) -> float | None:
        vals = [r[key] for r in readings if r.get(key) is not None]
        return round(sum(vals) / len(vals), 2) if vals else None

    return {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "middle_id": MIDDLE_ID,
        "listen_port": LISTEN_PORT,
        "event": event_type,
        "trigger_module": trigger_module,
        "rm_temp_c": rm_temp_c,
        "active_bottom_count": len(active_nodes),
        "reporting_bottom_count": len(readings),
        "active_module_ids": sorted(active_nodes.keys()),
        "group_summary": {
            "mean_eda_peaks_2min": avg_field("eda_peaks_2min"),
            "mean_bpm": avg_field("mean_bpm"),
            "mean_resp_rate_min": avg_field("resp_rate_min"),
            "mean_temp_c": avg_field("mean_temp_c"),
        },
        "context_analysis": context_analysis,
        "bottom_nodes": {
            mid: {
                "ip": info["ip"],
                "last_seen_ago_s": round(now - info["last_seen"], 1),
                "latest_reading": info["latest_reading"],
            }
            for mid, info in sorted(active_nodes.items())
        },
    }


def forward_to_top(packet: dict) -> bool:
    top_url = get_top_url()
    if not top_url:
        return False
    return post_json(f"{top_url}/api/middle", packet)


def periodic_top_sync(stop_event: threading.Event):
    while not stop_event.is_set():
        with state_lock:
            packet = build_group_packet_locked(event_type="heartbeat")
        forward_to_top(packet)
        stop_event.wait(SYNC_INTERVAL_S)


def ble_scanner_worker(stop_event: threading.Event):
    """Scans nearby MOD-XX BLE advertisements and sends RSSI readings to Top Pi."""
    try:
        from bleak import BleakScanner  # type: ignore
    except ImportError:
        print(f"[{MIDDLE_ID}] BLE scanner disabled (bleak not installed).")
        return

    print(f"[{MIDDLE_ID}] BLE scanner active (bleak).")
    last_log_t = 0.0

    async def _scan_loop():
        nonlocal last_log_t
        while not stop_event.is_set():
            top_url = get_top_url()
            if top_url:
                try:
                    devices = await BleakScanner.discover(timeout=2.5, return_adv=True)
                    readings = {}
                    for _addr, (dev, adv) in devices.items():
                        name = (adv.local_name or dev.name or "").strip().upper()
                        if name.startswith("MOD-"):
                            readings[name] = adv.rssi
                    if readings:
                        now = time.time()
                        if (now - last_log_t) >= 15.0:
                            print(f"[{MIDDLE_ID}] BLE RSSI detected: {readings}")
                            last_log_t = now
                        post_json(
                            f"{top_url}/api/ble_scan",
                            {"middle_id": MIDDLE_ID, "readings": readings},
                            timeout=2.5,
                        )
                except Exception:
                    pass
            await asyncio.sleep(BLE_SCAN_INTERVAL_S)

    try:
        asyncio.run(_scan_loop())
    except Exception:
        pass


class MiddleHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args):
        return

    def _send_json(self, code: int, body: dict):
        raw = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path == "/api/status":
            with state_lock:
                packet = build_group_packet_locked(event_type="status_query")
            self._send_json(200, packet)
        else:
            self._send_json(404, {"error": "Not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json(400, {"error": "Invalid JSON"})
            return

        client_ip = self.client_address[0]
        now = time.time()

        if self.path == "/api/heartbeat":
            mod_id = str(data.get("module_id", "")).strip().upper()
            if not mod_id:
                self._send_json(400, {"error": "Missing module_id"})
                return

            hb_reading = data.get("latest_reading")
            with state_lock:
                is_new = mod_id not in active_nodes
                if is_new:
                    active_nodes[mod_id] = {
                        "last_seen": now,
                        "ip": client_ip,
                        "latest_reading": hb_reading if isinstance(hb_reading, dict) else None,
                    }
                else:
                    active_nodes[mod_id]["last_seen"] = now
                    active_nodes[mod_id]["ip"] = client_ip
                    if isinstance(hb_reading, dict):
                        active_nodes[mod_id]["latest_reading"] = hb_reading
                count = len(active_nodes)

            if is_new:
                print(f"[{MIDDLE_ID}] Bottom node attached: {mod_id} ({client_ip}) | Active count: {count}")
            self._send_json(200, {"ok": True, "middle_id": MIDDLE_ID, "active_count": count})

        elif self.path == "/api/detach":
            mod_id = str(data.get("module_id", "")).strip().upper()
            if not mod_id:
                self._send_json(400, {"error": "Missing module_id"})
                return

            packet = None
            with state_lock:
                existed = mod_id in active_nodes
                if existed:
                    del active_nodes[mod_id]
                    packet = build_group_packet_locked(event_type="detach", trigger_module=mod_id)
                count = len(active_nodes)

            if existed and packet is not None:
                print(f"[{MIDDLE_ID}] Bottom node detached (room switch): {mod_id} | Active count: {count}")
                forward_to_top(packet)
            self._send_json(200, {"ok": True, "middle_id": MIDDLE_ID, "active_count": count})

        elif self.path == "/api/bottom":
            mod_id = str(data.get("module_id", "")).strip().upper()
            if not mod_id:
                self._send_json(400, {"error": "Missing module_id"})
                return

            with state_lock:
                if mod_id not in active_nodes:
                    active_nodes[mod_id] = {"last_seen": now, "ip": client_ip, "latest_reading": data}
                else:
                    active_nodes[mod_id]["last_seen"] = now
                    active_nodes[mod_id]["ip"] = client_ip
                    active_nodes[mod_id]["latest_reading"] = data

                csv_path = OUT_DIR / f"{MIDDLE_ID}_received.csv"
                write_header = not csv_path.exists()
                with open(csv_path, "a", newline="") as cf:
                    cw = csv.writer(cf)
                    if write_header:
                        cw.writerow(RECEIVED_HEADER)
                    cw.writerow([
                        datetime.now().isoformat(timespec="seconds"),
                        MIDDLE_ID,
                        mod_id,
                        data.get("duration_s", ""),
                        data.get("eda_peaks_2min", ""),
                        data.get("mean_bpm", ""),
                        data.get("resp_rate_min", ""),
                        data.get("mean_temp_c", ""),
                        data.get("adaptability_score", ""),
                    ])

                packet = build_group_packet_locked(event_type="reading", trigger_module=mod_id)

            sent_top = forward_to_top(packet)
            gs = packet["group_summary"]
            ctx = packet["context_analysis"]
            print(
                f"[{MIDDLE_ID}] Reading from {mod_id} | Active={packet['active_bottom_count']} "
                f"| Avg(EDA={gs['mean_eda_peaks_2min']}, BPM={gs['mean_bpm']}, "
                f"Resp={gs['mean_resp_rate_min']}, BodyTemp={gs['mean_temp_c']}) "
                f"| Ctrl={ctx['room_control']} | Conflict={ctx['conflict_detected']} "
                f"-> TopPi: {'SENT' if sent_top else 'UNREACHABLE'}"
            )
            self._send_json(200, {"ok": True, "forwarded_to_top": sent_top})

        else:
            self._send_json(404, {"error": "Not found"})


def main():
    OUT_DIR.mkdir(exist_ok=True)
    stop_event = threading.Event()
    threading.Thread(target=periodic_top_sync, args=(stop_event,), daemon=True).start()
    threading.Thread(target=ble_scanner_worker, args=(stop_event,), daemon=True).start()

    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), MiddleHandler)
    print(f"[{MIDDLE_ID}] Listening on 0.0.0.0:{LISTEN_PORT} (RmTemp={read_room_temperature()}°C)")
    print(f"[{MIDDLE_ID}] Target Top Pi: {get_top_url() or 'Not configured (set top_url.txt)'}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down Middle Node.")
    finally:
        stop_event.set()
        server.server_close()


if __name__ == "__main__":
    main()