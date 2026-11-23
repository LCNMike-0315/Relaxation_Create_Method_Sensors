#!/usr/bin/env python3
# Top-Layer Raspberry Pi Coordinator (1 Top Pi <- 3 Middle Pis <- 0~10 Bottom Pis each)
# Receives aggregated & individual data from MID-01, MID-02, MID-03

from __future__ import annotations

import csv
import json
import os
import socket
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# --- CONFIGURATION ---
TOP_ID = os.getenv("TOP_ID") or socket.gethostname().upper()
LISTEN_PORT = int(os.getenv("TOP_PORT", "8000"))
OUT_DIR = Path("data")
MASTER_CSV = OUT_DIR / "TOP_master_readings.csv"
LATEST_JSON = OUT_DIR / "TOP_latest_state.json"

MASTER_HEADER = [
    "top_received_at", "middle_id", "trigger_module",
    "active_bottom_count", "reporting_bottom_count",
    "group_mean_eda", "group_mean_bpm", "group_mean_resp", "group_mean_temp_c",
    "raw_json"
]

state_lock = threading.Lock()
# middle_states[middle_id] = {"last_seen": float, "ip": str, "packet": dict}
middle_states: dict[str, dict] = {}


def build_global_snapshot_locked() -> dict:
    now = time.time()
    total_bottom_active = 0
    all_bpm = []
    all_eda = []
    all_resp = []
    all_temp = []

    middles_out = {}
    for mid, info in sorted(middle_states.items()):
        pkt = info["packet"]
        total_bottom_active += pkt.get("active_bottom_count", 0)
        for b_info in pkt.get("bottom_nodes", {}).values():
            r = b_info.get("latest_reading")
            if r:
                if r.get("eda_peaks_2min") is not None:
                    all_eda.append(r["eda_peaks_2min"])
                if r.get("mean_bpm") is not None:
                    all_bpm.append(r["mean_bpm"])
                if r.get("resp_rate_min") is not None:
                    all_resp.append(r["resp_rate_min"])
                if r.get("mean_temp_c") is not None:
                    all_temp.append(r["mean_temp_c"])

        middles_out[mid] = {
            "ip": info["ip"],
            "last_seen_ago_s": round(now - info["last_seen"], 1),
            "active_bottom_count": pkt.get("active_bottom_count", 0),
            "active_module_ids": pkt.get("active_module_ids", []),
            "group_summary": pkt.get("group_summary", {}),
            "bottom_nodes": pkt.get("bottom_nodes", {}),
        }

    def avg(vals: list[float]) -> float | None:
        return round(sum(vals) / len(vals), 2) if vals else None

    return {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "top_id": TOP_ID,
        "connected_middle_count": len(middle_states),
        "total_active_bottom_count": total_bottom_active,
        "global_summary": {
            "mean_eda_peaks_2min": avg(all_eda),
            "mean_bpm": avg(all_bpm),
            "mean_resp_rate_min": avg(all_resp),
            "mean_temp_c": avg(all_temp),
        },
        "middle_nodes": middles_out,
    }


class TopHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args):
        return

    def _send_json(self, code: int, body: dict):
        raw = json.dumps(body, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path in ("/", "/api/status"):
            with state_lock:
                snapshot = build_global_snapshot_locked()
            self._send_json(200, snapshot)
        else:
            self._send_json(404, {"error": "Not found"})

    def do_POST(self):
        if self.path != "/api/middle":
            self._send_json(404, {"error": "Not found"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        try:
            packet = json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json(400, {"error": "Invalid JSON"})
            return

        mid = str(packet.get("middle_id", "")).strip().upper()
        if not mid:
            self._send_json(400, {"error": "Missing middle_id"})
            return

        client_ip = self.client_address[0]
        now = time.time()
        event_type = packet.get("event", "unknown")

        with state_lock:
            is_new = mid not in middle_states
            middle_states[mid] = {
                "last_seen": now,
                "ip": client_ip,
                "packet": packet,
            }
            snapshot = build_global_snapshot_locked()
            LATEST_JSON.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")

            if event_type == "reading":
                gs = packet.get("group_summary", {})
                write_header = not MASTER_CSV.exists()
                with open(MASTER_CSV, "a", newline="") as mf:
                    mw = csv.writer(mf)
                    if write_header:
                        mw.writerow(MASTER_HEADER)
                    mw.writerow([
                        datetime.now().isoformat(timespec="seconds"),
                        mid,
                        packet.get("trigger_module", ""),
                        packet.get("active_bottom_count", 0),
                        packet.get("reporting_bottom_count", 0),
                        gs.get("mean_eda_peaks_2min", ""),
                        gs.get("mean_bpm", ""),
                        gs.get("mean_resp_rate_min", ""),
                        gs.get("mean_temp_c", ""),
                        json.dumps(packet),
                    ])

        if is_new:
            print(f"[{TOP_ID}] Middle Pi connected: {mid} ({client_ip})")

        if event_type == "reading":
            print("\n" + "=" * 60)
            print(f"[{TOP_ID}] UPDATE from {mid} (trigger: {packet.get('trigger_module')})")
            for m_key, m_val in snapshot["middle_nodes"].items():
                gs = m_val["group_summary"]
                print(
                    f"  • {m_key}: {m_val['active_bottom_count']} bottom Pis {m_val['active_module_ids']} "
                    f"| Avg EDA={gs.get('mean_eda_peaks_2min')}, BPM={gs.get('mean_bpm')}, "
                    f"Resp={gs.get('mean_resp_rate_min')}, Temp={gs.get('mean_temp_c')}°C"
                )
            print(f"  => GLOBAL ({snapshot['total_active_bottom_count']} active Pis): {snapshot['global_summary']}")
            print("=" * 60 + "\n")

        self._send_json(200, {"ok": True, "top_id": TOP_ID})


def main():
    OUT_DIR.mkdir(exist_ok=True)
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), TopHandler)
    print(f"[{TOP_ID}] Top Coordinator listening on 0.0.0.0:{LISTEN_PORT}")
    print(f"[{TOP_ID}] Live JSON endpoint: http://0.0.0.0:{LISTEN_PORT}/api/status")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down Top Node.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()