#!/usr/bin/env python3
# Middle-Layer Raspberry Pi Aggregator (3 Middle Pis: MID-01, MID-02, MID-03)
# Receives from 0~10 dynamic Bottom Pis -> Aggregates & forwards to 1 Top Pi
# Supports clean /api/detach on room switches and optional BLE RSSI room scanning.

from __future__ import annotations

import asyncio
import csv
import json
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
BLE_SCAN_INTERVAL_S = 4.0   # BLE proximity scan cadence (if bleak is installed)

RECEIVED_HEADER = [
    "received_at", "middle_id", "module_id", "duration_s",
    "eda_peaks_2min", "mean_bpm", "resp_rate_min", "mean_temp_c"
]

state_lock = threading.Lock()
# active_nodes[module_id] = {"last_seen": float, "ip": str, "latest_reading": dict | None}
active_nodes: dict[str, dict] = {}


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
        "active_bottom_count": len(active_nodes),
        "reporting_bottom_count": len(readings),
        "active_module_ids": sorted(active_nodes.keys()),
        "group_summary": {
            "mean_eda_peaks_2min": avg_field("eda_peaks_2min"),
            "mean_bpm": avg_field("mean_bpm"),
            "mean_resp_rate_min": avg_field("resp_rate_min"),
            "mean_temp_c": avg_field("mean_temp_c"),
        },
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

    async def _scan_loop():
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
        return  # Keep console output clean

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

            with state_lock:
                is_new = mod_id not in active_nodes
                if is_new:
                    active_nodes[mod_id] = {"last_seen": now, "ip": client_ip, "latest_reading": None}
                else:
                    active_nodes[mod_id]["last_seen"] = now
                    active_nodes[mod_id]["ip"] = client_ip
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
                    ])

                packet = build_group_packet_locked(event_type="reading", trigger_module=mod_id)

            sent_top = forward_to_top(packet)
            gs = packet["group_summary"]
            print(
                f"[{MIDDLE_ID}] Reading from {mod_id} | Active={packet['active_bottom_count']} "
                f"| GroupAvg(EDA={gs['mean_eda_peaks_2min']}, BPM={gs['mean_bpm']}, "
                f"Resp={gs['mean_resp_rate_min']}, Temp={gs['mean_temp_c']}) "
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
    print(f"[{MIDDLE_ID}] Listening on 0.0.0.0:{LISTEN_PORT}")
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