#!/usr/bin/env python3
# Top-Layer Raspberry Pi Coordinator (1 Top Pi <- 3 Middle Pis <- 0~10 Bottom Pis each)
# Acts as Central Room Directory, Experiment Data Logger, and Live Dashboard (/rooms).

from __future__ import annotations

import csv
import io
import json
import os
import re
import shutil
import socket
import threading
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

# --- CONFIGURATION ---
TOP_ID = os.getenv("TOP_ID") or socket.gethostname().upper()
LISTEN_PORT = int(os.getenv("TOP_PORT", "8000"))
OUT_DIR = Path("data")
RAW_SESSIONS_DIR = OUT_DIR / "raw_sessions"

MASTER_CSV = OUT_DIR / "TOP_master_readings.csv"
RAW_PACKETS_JSONL = OUT_DIR / "TOP_raw_packets.jsonl"
PARTICIPANT_SESSIONS_CSV = OUT_DIR / "TOP_participant_sessions.csv"
ROOM_CONTEXT_CSV = OUT_DIR / "TOP_room_context_log.csv"
ROUTING_LOG_CSV = OUT_DIR / "TOP_room_routing_log.csv"
BLE_RSSI_CSV = OUT_DIR / "TOP_ble_rssi_log.csv"
CAPTURE_EVENTS_CSV = OUT_DIR / "TOP_capture_events.csv"

LATEST_JSON = OUT_DIR / "TOP_latest_state.json"
ASSIGNMENTS_JSON = OUT_DIR / "TOP_room_assignments.json"

BLE_STALE_S = 20.0
BLE_HYSTERESIS_DBM = float(os.getenv("BLE_HYSTERESIS_DBM", "3.0"))
AUTO_CHECK_INTERVAL_S = float(os.getenv("AUTO_CHECK_INTERVAL_S", "15.0"))
MIDDLE_STALE_S = float(os.getenv("MIDDLE_STALE_S", "35.0"))

TEMP_THRESHOLD_C = 23.0
BODY_TEMP_THRESHOLD_C = 36.0
EMOTION_NAMES = ["Anxiety", "Displeasure", "Anger"]
EMOTION_WEIGHTS = [0.4, 0.3, 0.2]

MASTER_HEADER = [
    "top_received_at", "middle_id", "trigger_module",
    "active_bottom_count", "reporting_bottom_count",
    "group_mean_eda", "group_mean_bpm", "group_mean_resp", "group_mean_temp_c",
    "raw_json"
]

PARTICIPANT_SESSION_HEADER = [
    "recorded_at", "middle_id", "trigger_module", "is_trigger_module", "mod_id",
    "assigned_mode", "ble_rssi_current_room", "is_recording", "sim_mode",
    "eda_peaks_2min", "mean_bpm", "resp_rate_min", "mean_temp_c",
    "z_eda", "z_hr", "z_rr", "z_temp",
    "adaptability_score", "priority_weight",
    "thermal_comfort", "thermal_mismatch",
    "stress_types", "dominant_stress", "is_conflict_outlier",
    "room_temp_c", "crowdedness", "ac_target_temp_c", "device_setting",
    "conflict_detected", "cos_sim_equality", "cos_sim_eq_weight", "cos_sim_fairness",
    "move_recommended_to", "move_recommendation_reason"
]

ROOM_CONTEXT_HEADER = [
    "recorded_at", "event_type", "middle_id", "trigger_module",
    "rm_temp_c", "crowdedness", "active_bottom_count", "reporting_bottom_count",
    "active_module_ids", "recording_module_ids",
    "group_mean_eda", "group_mean_bpm", "group_mean_resp", "group_mean_temp_c",
    "vec_equality", "vec_eq_weight", "vec_fairness",
    "cos_sim_equality", "cos_sim_eq_weight", "cos_sim_fairness",
    "conflict_detected", "conflict_outlier",
    "ac_target_temp_c", "device_setting"
]

ROUTING_LOG_HEADER = [
    "recorded_at", "event_type", "mod_id", "from_mid", "to_mid",
    "mode", "source_tag", "rssi_mid01", "rssi_mid02", "rssi_mid03", "reason"
]

BLE_RSSI_HEADER = [
    "recorded_at", "mod_id", "scanner_mid_id", "rssi_dbm", "assigned_mid_id", "routing_mode"
]

CAPTURE_EVENTS_HEADER = [
    "recorded_at", "mod_id", "middle_id", "event", "recording_elapsed_s", "sim_mode"
]

state_lock = threading.Lock()
middle_states: dict[str, dict] = {}
room_assignments: dict[str, dict] = {}
ble_observations: dict[str, dict[str, dict]] = {}
latest_move_recommendations: dict[str, dict] = {}
prev_recording_state: dict[str, bool] = {}
last_room_signature: dict[str, str] = {}
last_logged_recommendations: dict[str, tuple[str, str]] = {}


def post_json_resp(url: str, payload: dict, timeout: float = 4.0) -> dict | None:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if 200 <= resp.status < 300:
                return json.loads(resp.read().decode("utf-8"))
    except Exception:
        pass
    return None


def append_csv_row_locked(path: Path, header: list[str], row: list):
    OUT_DIR.mkdir(exist_ok=True)
    write_header = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(header)
        w.writerow(row)


def log_routing_event_locked(
    event_type: str,
    mod_id: str,
    from_mid: str,
    to_mid: str,
    mode: str,
    source_tag: str,
    now: float,
    reason: str = "",
):
    obs = ble_observations.get(mod_id, {})

    def fresh_rssi(mid_name: str):
        info = obs.get(mid_name)
        if info and (now - info["ts"]) <= BLE_STALE_S:
            return info["rssi"]
        return ""

    append_csv_row_locked(
        ROUTING_LOG_CSV,
        ROUTING_LOG_HEADER,
        [
            datetime.now().isoformat(timespec="seconds"),
            event_type,
            mod_id,
            from_mid,
            to_mid,
            mode,
            source_tag,
            fresh_rssi("MID-01"),
            fresh_rssi("MID-02"),
            fresh_rssi("MID-03"),
            reason,
        ],
    )


def load_assignments():
    if ASSIGNMENTS_JSON.exists():
        try:
            data = json.loads(ASSIGNMENTS_JSON.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                room_assignments.update(data)
        except Exception:
            pass


def save_assignments_locked():
    OUT_DIR.mkdir(exist_ok=True)
    ASSIGNMENTS_JSON.write_text(json.dumps(room_assignments, indent=2), encoding="utf-8")


def prune_stale_middles_locked(now: float):
    stale_mids = [
        mid for mid, info in middle_states.items()
        if (now - info["last_seen"]) > MIDDLE_STALE_S
    ]
    for mid in stale_mids:
        del middle_states[mid]
        print(f"[{TOP_ID}] Middle Pi offline (timeout > {MIDDLE_STALE_S:.0f}s): {mid}")


def default_middle_id_locked() -> str:
    if "MID-01" in middle_states:
        return "MID-01"
    if middle_states:
        return sorted(middle_states.keys())[0]
    return "MID-01"


def resolve_route_locked(mod_id: str) -> dict:
    now = time.time()
    prune_stale_middles_locked(now)
    entry = room_assignments.get(mod_id)
    if entry and entry.get("mid_id"):
        target_mid = entry["mid_id"]
        mode = entry.get("mode", "auto")
    else:
        target_mid = default_middle_id_locked()
        mode = "auto"
        room_assignments[mod_id] = {
            "mid_id": target_mid,
            "mode": mode,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        save_assignments_locked()
        log_routing_event_locked("ROOM_ASSIGN", mod_id, "", target_mid, mode, "ROUTE_INIT", now)

    m_info = middle_states.get(target_mid)
    middle_url = f"http://{m_info['ip']}:{m_info.get('port', 8001)}" if m_info else ""

    rec = latest_move_recommendations.get(mod_id)
    active_rec = None
    if isinstance(rec, dict):
        if rec.get("from_mid") == target_mid and rec.get("to_mid") and rec.get("to_mid") != target_mid:
            active_rec = rec
        else:
            latest_move_recommendations.pop(mod_id, None)

    return {
        "ok": True,
        "mod_id": mod_id,
        "mid_id": target_mid,
        "middle_url": middle_url,
        "mode": mode,
        "move_recommendation": active_rec,
    }


def evaluate_ble_routing_locked(
    mod_id: str,
    now: float,
    force_recheck: bool = False,
    source_tag: str = "BLE",
):
    current = room_assignments.get(mod_id, {})
    if current.get("mode") == "manual":
        return

    obs = ble_observations.get(mod_id, {})
    fresh = {
        mid: info["rssi"]
        for mid, info in obs.items()
        if (now - info["ts"]) <= BLE_STALE_S and mid in middle_states
    }

    if not fresh:
        if force_recheck:
            prev_mid = current.get("mid_id", "")
            default_mid = default_middle_id_locked()
            room_assignments[mod_id] = {
                "mid_id": default_mid,
                "mode": "auto",
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
            if prev_mid != default_mid:
                latest_move_recommendations.pop(mod_id, None)
            save_assignments_locked()
            log_routing_event_locked("ROOM_ASSIGN", mod_id, prev_mid, default_mid, "auto", source_tag, now, "No BLE signal yet")
            print(f"[{TOP_ID}] ROOM ASSIGN ({source_tag} - no BLE signal yet): {mod_id} -> {default_mid}")
        return

    best_mid, best_rssi = max(fresh.items(), key=lambda kv: kv[1])
    cur_mid = current.get("mid_id")
    rssi_summary = ", ".join(f"{m}:{r:.0f}dBm" for m, r in sorted(fresh.items()))

    if force_recheck or not cur_mid or cur_mid not in fresh:
        if cur_mid != best_mid or current.get("mode") != "auto":
            room_assignments[mod_id] = {
                "mid_id": best_mid,
                "mode": "auto",
                "rssi": best_rssi,
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
            if cur_mid != best_mid:
                latest_move_recommendations.pop(mod_id, None)
            save_assignments_locked()
            log_routing_event_locked("ROOM_ASSIGN", mod_id, cur_mid or "", best_mid, "auto", source_tag, now, rssi_summary)
            print(f"[{TOP_ID}] ROOM ASSIGN ({source_tag}): {mod_id} -> {best_mid} [{rssi_summary}]")
        return

    cur_rssi = fresh[cur_mid]
    if best_mid != cur_mid and (best_rssi - cur_rssi) >= BLE_HYSTERESIS_DBM:
        room_assignments[mod_id] = {
            "mid_id": best_mid,
            "mode": "auto",
            "rssi": best_rssi,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        latest_move_recommendations.pop(mod_id, None)
        save_assignments_locked()
        log_routing_event_locked("ROOM_SWITCH", mod_id, cur_mid, best_mid, "auto", source_tag, now, rssi_summary)
        print(
            f"[{TOP_ID}] ROOM SWITCH ({source_tag}): {mod_id} {cur_mid} -> {best_mid} [{rssi_summary}]"
        )


def periodic_auto_route_worker(stop_event: threading.Event):
    while not stop_event.is_set():
        stop_event.wait(AUTO_CHECK_INTERVAL_S)
        if stop_event.is_set():
            break
        now = time.time()
        with state_lock:
            prune_stale_middles_locked(now)
            all_mods = set(room_assignments.keys()) | set(ble_observations.keys())
            for mod_id in sorted(all_mods):
                if room_assignments.get(mod_id, {}).get("mode", "auto") == "auto":
                    evaluate_ble_routing_locked(mod_id, now, force_recheck=False, source_tag="TIMER")


def analyze_participant_vitals(reading: dict | None, room_temp_c: float) -> dict | None:
    if not isinstance(reading, dict):
        return None

    norm_vec = reading.get("norm_vec")
    body_temp = reading.get("mean_temp_c")

    has_norm = isinstance(norm_vec, list) and len(norm_vec) >= 3
    active_emotions: list[str] = []
    dominant_stress = "Unknown"

    if has_norm:
        z_emo = [float(norm_vec[i]) for i in range(3)]
        active_emotions = [
            EMOTION_NAMES[i]
            for i in range(3)
            if z_emo[i] >= 1.0
        ]
        if active_emotions:
            weighted_scores = [z_emo[i] * EMOTION_WEIGHTS[i] for i in range(3)]
            best_idx = max(range(3), key=lambda i: weighted_scores[i])
            dominant_stress = EMOTION_NAMES[best_idx]
        else:
            dominant_stress = "Normal"

    comfort = None
    thermal_mismatch = False
    if body_temp is not None:
        bt = float(body_temp)
        comfort = "Heat-Sensitive" if bt >= BODY_TEMP_THRESHOLD_C else "Cold-Sensitive"
        thermal_mismatch = (
            (comfort == "Heat-Sensitive" and room_temp_c >= TEMP_THRESHOLD_C)
            or (comfort == "Cold-Sensitive" and room_temp_c < TEMP_THRESHOLD_C)
        )

    return {
        "eda_peaks_2min": reading.get("eda_peaks_2min"),
        "mean_bpm": reading.get("mean_bpm"),
        "resp_rate_min": reading.get("resp_rate_min"),
        "mean_temp_c": body_temp,
        "thermal_comfort": comfort,
        "thermal_mismatch": thermal_mismatch,
        "stress_types": active_emotions,
        "dominant_stress": dominant_stress,
    }


def compute_move_recommendations_locked(middles_out: dict[str, dict]) -> dict[str, dict]:
    recommendations: dict[str, dict] = {}
    if len(middles_out) < 2:
        return recommendations

    room_temp_map: dict[str, float] = {}
    room_focus_emotion: dict[str, str] = {}
    room_crowd_map: dict[str, float] = {}

    for mid, m_val in middles_out.items():
        ctx = m_val.get("context_analysis") or {}
        room_temp_map[mid] = float(m_val.get("rm_temp_c", TEMP_THRESHOLD_C))
        room_crowd_map[mid] = float(ctx.get("crowdedness", 0.0))

        ew_vec = (ctx.get("strategy_vectors") or {}).get("Equality With Weight", [0.0, 0.0, 0.0])
        if sum(ew_vec[:3]) > 0:
            max_idx = ew_vec[:3].index(max(ew_vec[:3]))
            room_focus_emotion[mid] = EMOTION_NAMES[max_idx]

    for mid, m_val in sorted(middles_out.items()):
        ctx = m_val.get("context_analysis") or {}
        conflict_detected = bool(ctx.get("conflict_detected", False))
        conflict_outlier = ctx.get("conflict_outlier")
        current_temp = room_temp_map[mid]
        bottom_nodes = m_val.get("bottom_nodes") or {}

        for mod_id, b_info in sorted(bottom_nodes.items()):
            va = b_info.get("vital_analysis") or analyze_participant_vitals(b_info.get("latest_reading"), current_temp) or {}
            active_emotions = va.get("stress_types", [])
            comfort = va.get("thermal_comfort")
            thermal_mismatch = bool(va.get("thermal_mismatch", False))

            best_target = None
            reason_parts = []

            if conflict_detected and thermal_mismatch and active_emotions:
                for alt_mid, alt_temp in sorted(room_temp_map.items()):
                    if alt_mid == mid:
                        continue
                    temp_ok = (alt_temp < TEMP_THRESHOLD_C) if comfort == "Heat-Sensitive" else (alt_temp >= TEMP_THRESHOLD_C)
                    if temp_ok:
                        alt_emo = room_focus_emotion.get(alt_mid)
                        if alt_emo and alt_emo in active_emotions:
                            best_target = alt_mid
                            reason_parts.append(
                                f"{comfort} & {', '.join(active_emotions)} -> {alt_mid} ({alt_temp}°C, {alt_emo} focus)"
                            )
                            break
                        elif best_target is None:
                            best_target = alt_mid
                            reason_parts.append(
                                f"{comfort} ({', '.join(active_emotions)}) -> {alt_mid} ({alt_temp}°C)"
                            )

            if best_target is None and conflict_detected and conflict_outlier == mod_id:
                alt_candidates = [m for m in sorted(middles_out.keys()) if m != mid]
                if alt_candidates:
                    best_target = min(alt_candidates, key=lambda m: room_crowd_map.get(m, 0.0))
                    reason_parts.append(
                        f"Conflict Outlier (Fairness priority) -> {best_target} ({room_temp_map[best_target]}°C)"
                    )

            if best_target:
                recommendations[mod_id] = {
                    "from_mid": mid,
                    "to_mid": best_target,
                    "comfort": comfort,
                    "active_emotions": active_emotions,
                    "reason": "; ".join(reason_parts),
                }

    return recommendations


def build_global_snapshot_locked() -> dict:
    now = time.time()
    prune_stale_middles_locked(now)

    total_bottom_active = 0
    recording_mods: set[str] = set()
    all_bpm = []
    all_eda = []
    all_resp = []
    all_temp = []

    middles_out = {}
    for mid, info in sorted(middle_states.items()):
        pkt = info["packet"]
        rm_temp_c = float(pkt.get("rm_temp_c", TEMP_THRESHOLD_C))
        total_bottom_active += pkt.get("active_bottom_count", 0)
        bottom_nodes_out = {}

        for b_id, b_info in pkt.get("bottom_nodes", {}).items():
            b_copy = dict(b_info)
            if b_copy.get("is_recording"):
                recording_mods.add(b_id)
                base_elapsed = float(b_copy.get("recording_elapsed_s", 0.0))
                b_copy["recording_elapsed_s"] = round(base_elapsed + max(0.0, now - info["last_seen"]), 1)

            r = b_copy.get("latest_reading")
            b_copy["vital_analysis"] = analyze_participant_vitals(r, rm_temp_c)
            bottom_nodes_out[b_id] = b_copy

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
            "port": info.get("port", 8001),
            "last_seen_ago_s": round(now - info["last_seen"], 1),
            "rm_temp_c": rm_temp_c,
            "active_bottom_count": pkt.get("active_bottom_count", 0),
            "active_module_ids": pkt.get("active_module_ids", []),
            "recording_module_ids": pkt.get("recording_module_ids", []),
            "group_summary": pkt.get("group_summary", {}),
            "context_analysis": pkt.get("context_analysis", {}),
            "bottom_nodes": bottom_nodes_out,
        }

    move_recommendations = compute_move_recommendations_locked(middles_out)
    latest_move_recommendations.clear()
    latest_move_recommendations.update(move_recommendations)

    ble_live = {}
    for mod_id, per_mid in sorted(ble_observations.items()):
        ble_live[mod_id] = {
            mid: {
                "rssi": info["rssi"],
                "age_s": round(now - info["ts"], 1),
            }
            for mid, info in sorted(per_mid.items())
            if (now - info["ts"]) <= BLE_STALE_S and mid in middle_states
        }

    def avg(vals: list[float]) -> float | None:
        return round(sum(vals) / len(vals), 2) if vals else None

    return {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "top_id": TOP_ID,
        "auto_check_interval_s": AUTO_CHECK_INTERVAL_S,
        "connected_middle_count": len(middle_states),
        "total_active_bottom_count": total_bottom_active,
        "total_recording_count": len(recording_mods),
        "recording_module_ids": sorted(recording_mods),
        "room_assignments": room_assignments,
        "ble_rssi": ble_live,
        "move_recommendations": move_recommendations,
        "global_summary": {
            "mean_eda_peaks_2min": avg(all_eda),
            "mean_bpm": avg(all_bpm),
            "mean_resp_rate_min": avg(all_resp),
            "mean_temp_c": avg(all_temp),
        },
        "middle_nodes": middles_out,
    }


def reset_all_cluster_data() -> dict:
    """Wipes all experiment data across Top, Middle, and Bottom Pis while preserving connections."""
    with state_lock:
        for info in middle_states.values():
            pkt = info.get("packet") or {}
            for b_info in (pkt.get("bottom_nodes") or {}).values():
                if b_info.get("is_recording"):
                    return {
                        "ok": False,
                        "error": "Cannot initialize while a node is recording. Wait for capture to finish first.",
                    }
        middles_snapshot = {
            mid: (info["ip"], info.get("port", 8001))
            for mid, info in middle_states.items()
        }

    cleared_middles = []
    cleared_bottoms = []
    for mid, (m_ip, m_port) in sorted(middles_snapshot.items()):
        resp = post_json_resp(f"http://{m_ip}:{m_port}/api/reset", {}, timeout=4.0)
        if resp and resp.get("ok"):
            cleared_middles.append(mid)
            cleared_bottoms.extend(resp.get("cleared_bottoms", []))

    with state_lock:
        prev_recording_state.clear()
        last_room_signature.clear()
        last_logged_recommendations.clear()
        latest_move_recommendations.clear()

        for info in middle_states.values():
            pkt = info.get("packet")
            if isinstance(pkt, dict):
                pkt["reporting_bottom_count"] = 0
                for b_info in (pkt.get("bottom_nodes") or {}).values():
                    if isinstance(b_info, dict):
                        b_info["latest_reading"] = None

        if OUT_DIR.exists():
            for item in OUT_DIR.iterdir():
                try:
                    if item.is_dir():
                        shutil.rmtree(item)
                    else:
                        item.unlink()
                except OSError:
                    pass
        OUT_DIR.mkdir(exist_ok=True)
        RAW_SESSIONS_DIR.mkdir(exist_ok=True)
        save_assignments_locked()

        snapshot = build_global_snapshot_locked()
        LATEST_JSON.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")

    print(f"[{TOP_ID}] INITIALIZED: Cleared all experiment data (Middles={cleared_middles}, Bottoms={sorted(set(cleared_bottoms))})")
    return {
        "ok": True,
        "cleared_middles": cleared_middles,
        "cleared_bottoms": sorted(set(cleared_bottoms)),
    }


def record_experiment_logs_locked(mid: str, event_type: str, payload: dict, snapshot: dict, now: float):
    if event_type == "reset":
        return

    ts_iso = datetime.now().isoformat(timespec="seconds")
    OUT_DIR.mkdir(exist_ok=True)

    with open(RAW_PACKETS_JSONL, "a", encoding="utf-8") as jf:
        jf.write(json.dumps({"received_at": ts_iso, "middle_id": mid, "packet": payload}) + "\n")

    m_val = snapshot["middle_nodes"].get(mid, {})
    ctx = m_val.get("context_analysis") or {}
    ctrl = ctx.get("room_control") or {}
    cos = ctx.get("cosine_similarity") or {}
    s_vecs = ctx.get("strategy_vectors") or {}
    p_weights = ctx.get("priority_weights") or {}
    gs = payload.get("group_summary") or {}
    recs = snapshot.get("move_recommendations") or {}

    for b_id, b_info in (m_val.get("bottom_nodes") or {}).items():
        is_rec = bool(b_info.get("is_recording", False))
        was_rec = prev_recording_state.get(b_id, False)
        if is_rec != was_rec:
            ev_name = "CAPTURE_START" if is_rec else "CAPTURE_STOP"
            append_csv_row_locked(
                CAPTURE_EVENTS_CSV,
                CAPTURE_EVENTS_HEADER,
                [
                    ts_iso,
                    b_id,
                    mid,
                    ev_name,
                    b_info.get("recording_elapsed_s", 0.0),
                    bool(b_info.get("sim_mode", False)),
                ],
            )
            prev_recording_state[b_id] = is_rec

    sig = json.dumps({
        "rm_temp_c": m_val.get("rm_temp_c"),
        "active": m_val.get("active_module_ids"),
        "rec": m_val.get("recording_module_ids"),
        "ctrl": ctrl,
        "conflict": ctx.get("conflict_detected"),
        "outlier": ctx.get("conflict_outlier"),
        "cos": cos,
    }, sort_keys=True)

    if event_type != "sync" or last_room_signature.get(mid) != sig:
        last_room_signature[mid] = sig
        append_csv_row_locked(
            ROOM_CONTEXT_CSV,
            ROOM_CONTEXT_HEADER,
            [
                ts_iso,
                event_type,
                mid,
                payload.get("trigger_module", ""),
                m_val.get("rm_temp_c", ""),
                ctx.get("crowdedness", ""),
                payload.get("active_bottom_count", 0),
                payload.get("reporting_bottom_count", 0),
                json.dumps(m_val.get("active_module_ids", [])),
                json.dumps(m_val.get("recording_module_ids", [])),
                gs.get("mean_eda_peaks_2min", ""),
                gs.get("mean_bpm", ""),
                gs.get("mean_resp_rate_min", ""),
                gs.get("mean_temp_c", ""),
                json.dumps(s_vecs.get("Equality", [])),
                json.dumps(s_vecs.get("Equality With Weight", [])),
                json.dumps(s_vecs.get("Fairness", [])),
                cos.get("Equality", ""),
                cos.get("Equality With Weight", ""),
                cos.get("Fairness", ""),
                bool(ctx.get("conflict_detected", False)),
                ctx.get("conflict_outlier", ""),
                ctrl.get("ac_target_temp_c", ""),
                ctrl.get("device_setting", ""),
            ],
        )

    for mod_id, rec in recs.items():
        rec_sig = (rec.get("to_mid", ""), rec.get("reason", ""))
        if last_logged_recommendations.get(mod_id) != rec_sig:
            last_logged_recommendations[mod_id] = rec_sig
            assign_info = room_assignments.get(mod_id, {})
            log_routing_event_locked(
                "MOVE_RECOMMENDED",
                mod_id,
                rec.get("from_mid", ""),
                rec.get("to_mid", ""),
                assign_info.get("mode", "auto"),
                "RECOMMENDATION",
                now,
                rec.get("reason", ""),
            )

    if event_type == "reading":
        append_csv_row_locked(
            MASTER_CSV,
            MASTER_HEADER,
            [
                ts_iso,
                mid,
                payload.get("trigger_module", ""),
                payload.get("active_bottom_count", 0),
                payload.get("reporting_bottom_count", 0),
                gs.get("mean_eda_peaks_2min", ""),
                gs.get("mean_bpm", ""),
                gs.get("mean_resp_rate_min", ""),
                gs.get("mean_temp_c", ""),
                json.dumps(payload),
            ],
        )

        trig_mod = str(payload.get("trigger_module", "")).strip().upper()
        for b_id, b_info in sorted((m_val.get("bottom_nodes") or {}).items()):
            r = b_info.get("latest_reading")
            if not isinstance(r, dict):
                continue
            va = b_info.get("vital_analysis") or {}
            nv = r.get("norm_vec") if isinstance(r.get("norm_vec"), list) else []
            z_eda = nv[0] if len(nv) > 0 else ""
            z_hr = nv[1] if len(nv) > 1 else ""
            z_rr = nv[2] if len(nv) > 2 else ""
            z_temp = nv[3] if len(nv) > 3 else ""

            assign_info = room_assignments.get(b_id, {})
            obs_mid = (ble_observations.get(b_id) or {}).get(mid)
            cur_rssi = obs_mid["rssi"] if obs_mid and (now - obs_mid["ts"]) <= BLE_STALE_S else ""
            rec_for_mod = recs.get(b_id) or {}

            append_csv_row_locked(
                PARTICIPANT_SESSIONS_CSV,
                PARTICIPANT_SESSION_HEADER,
                [
                    ts_iso,
                    mid,
                    trig_mod,
                    1 if b_id == trig_mod else 0,
                    b_id,
                    assign_info.get("mode", "auto"),
                    cur_rssi,
                    bool(b_info.get("is_recording", False)),
                    bool(b_info.get("sim_mode", False)),
                    r.get("eda_peaks_2min", ""),
                    r.get("mean_bpm", ""),
                    r.get("resp_rate_min", ""),
                    r.get("mean_temp_c", ""),
                    z_eda,
                    z_hr,
                    z_rr,
                    z_temp,
                    r.get("adaptability_score", ""),
                    p_weights.get(b_id, ""),
                    va.get("thermal_comfort", ""),
                    va.get("thermal_mismatch", ""),
                    "|".join(va.get("stress_types", [])),
                    va.get("dominant_stress", ""),
                    1 if ctx.get("conflict_outlier") == b_id else 0,
                    m_val.get("rm_temp_c", ""),
                    ctx.get("crowdedness", ""),
                    ctrl.get("ac_target_temp_c", ""),
                    ctrl.get("device_setting", ""),
                    bool(ctx.get("conflict_detected", False)),
                    cos.get("Equality", ""),
                    cos.get("Equality With Weight", ""),
                    cos.get("Fairness", ""),
                    rec_for_mod.get("to_mid", ""),
                    rec_for_mod.get("reason", ""),
                ],
            )


def build_export_zip_bytes() -> bytes:
    buf = io.BytesIO()
    with state_lock:
        OUT_DIR.mkdir(exist_ok=True)
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in sorted(OUT_DIR.rglob("*")):
                if p.is_file():
                    zf.write(p, arcname=str(p.relative_to(OUT_DIR)))
    return buf.getvalue()


def render_rooms_html() -> str:
    return """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Vital Cluster — Dynamic Room Router & Context Control</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; max-width: 1460px; margin: 24px auto; padding: 0 16px; background: #111827; color: #f3f4f6; }
    h1 { font-size: 20px; margin-bottom: 6px; }
    h2 { font-size: 16px; margin: 24px 0 10px 0; color: #e5e7eb; }
    p.sub { color: #9ca3af; font-size: 13px; margin-top: 0; }
    button, a.btn { background: #2563eb; color: #fff; border: none; padding: 6px 10px; border-radius: 6px; cursor: pointer; margin-right: 6px; font-size: 12px; text-decoration: none; display: inline-block; }
    button.auto { background: #059669; }
    button.check { background: #7c3aed; margin-left: 8px; }
    a.export-btn { background: #0d9488; font-weight: 600; margin-left: 4px; }
    button.reset-btn { background: #dc2626; font-weight: 700; margin-left: 4px; }
    button.reset-btn:hover { background: #b91c1c; }
    button.move-btn { background: #d97706; font-weight: 600; margin-top: 4px; }
    button.active { outline: 2px solid #fbbf24; font-weight: 700; }
    table { width: 100%; border-collapse: collapse; margin-top: 12px; background: #1f2937; border-radius: 8px; overflow: hidden; }
    th, td { padding: 10px 10px; border-bottom: 1px solid #374151; text-align: left; font-size: 13px; vertical-align: middle; }
    th { background: #0f172a; color: #9ca3af; font-weight: 600; }
    .badge { padding: 2px 8px; border-radius: 999px; font-size: 11px; background: #374151; display: inline-block; margin-right: 4px; margin-top: 2px; }
    .badge-ok { background: #065f46; color: #a7f3d0; }
    .badge-warn { background: #991b1b; color: #fecaca; font-weight: 700; }
    .badge-rec { background: #b91c1c; color: #fef2f2; font-weight: 700; animation: pulseRec 1.4s infinite; }
    .badge-idle { background: #1e3a8a; color: #bfdbfe; }
    .badge-heat { background: #9a3412; color: #fed7aa; font-weight: 600; }
    .badge-cold { background: #075985; color: #bae6fd; font-weight: 600; }
    .badge-stress { background: #7f1d1d; color: #fecaca; font-weight: 600; }
    @keyframes pulseRec {
      0% { opacity: 1; }
      50% { opacity: 0.65; }
      100% { opacity: 1; }
    }
    .rssi { font-family: ui-monospace, SFMono-Regular, monospace; font-size: 12px; color: #93c5fd; }
    .mono { font-family: ui-monospace, SFMono-Regular, monospace; font-size: 12px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 14px; margin-top: 10px; }
    .card { background: #1f2937; border: 1px solid #374151; border-radius: 8px; padding: 14px; }
    .card-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; border-bottom: 1px solid #374151; padding-bottom: 8px; }
    .kv { display: flex; justify-content: space-between; font-size: 12px; margin: 5px 0; color: #d1d5db; }
    .kv span.lbl { color: #9ca3af; }
  </style>
</head>
<body>
  <h1>Dynamic Room Router & Context Control</h1>
  <p class="sub">
    Timer automatically checks closest room every <span id="interval">15</span>s when in <b>AUTO (BLE)</b> mode.
    <button class="check" onclick="recheckAll()">Check Closest Now</button>
    <a class="btn export-btn" href="/api/export.zip">Download All Experiment Data (.ZIP)</a>
    <button class="reset-btn" onclick="initializeAllData()">Initialize (Clear All Data)</button>
  </p>
  <div id="middles"></div>

  <table>
    <thead>
      <tr>
        <th>Participant (MOD)</th>
        <th>Capture Status</th>
        <th>Assigned Room</th>
        <th>Mode</th>
        <th>Live BLE Signal</th>
        <th>Vitals, Comfort & Stress</th>
        <th>Adaptability / Priority</th>
        <th>Movement Recommendation</th>
        <th>Switch Room</th>
      </tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>

  <h2>Room Context Vectors, Cosine Similarity & Device Settings</h2>
  <div class="cards" id="room-cards"></div>

  <script>
    let latestData = null;
    let latestFetchMs = 0;

    async function assign(modId, midId, mode) {
      await fetch('/api/assign', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({mod_id: modId, mid_id: midId, mode: mode})
      });
      refresh();
    }
    async function recheckAll() {
      await fetch('/api/recheck', { method: 'POST' });
      refresh();
    }
    async function initializeAllData() {
      const firstOk = confirm(
        "Step 1 of 2: Delete ALL recorded experiment data (CSV logs, raw 5Hz files, baseline histories, and dashboard metrics) across TOP, MIDDLE, and BOTTOM Pis?"
      );
      if (!firstOk) return;

      const secondOk = confirm(
        "Step 2 of 2 (FINAL CONFIRMATION): Are you sure you want to permanently wipe all old test data before starting the experiment? This cannot be undone."
      );
      if (!secondOk) return;

      const res = await fetch('/api/reset', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ confirmed: true })
      });
      const out = await res.json();
      if (!out.ok) {
        alert("Initialize aborted: " + (out.error || "Unknown error"));
        return;
      }
      alert(
        "Initialized successfully! Cleared data on TOP-01, Middles: [" +
        (out.cleared_middles || []).join(", ") +
        "], Bottoms: [" +
        (out.cleared_bottoms || []).join(", ") +
        "]."
      );
      refresh();
    }
    function fmtSim(v) {
      return (v === null || v === undefined) ? '-' : Number(v).toFixed(4);
    }
    function fmtVal(v, suffix = '') {
      return (v === null || v === undefined || v === '') ? '-' : `${v}${suffix}`;
    }
    async function refresh() {
      const res = await fetch('/api/status');
      latestData = await res.json();
      latestFetchMs = Date.now();
      renderDashboard();
    }
    function renderDashboard() {
      if (!latestData) return;
      const data = latestData;
      const driftSec = (Date.now() - latestFetchMs) / 1000.0;

      if (data.auto_check_interval_s) {
        document.getElementById('interval').textContent = data.auto_check_interval_s;
      }
      const mids = Object.keys(data.middle_nodes);
      if (!mids.includes('MID-01')) mids.push('MID-01');
      if (!mids.includes('MID-02')) mids.push('MID-02');
      if (!mids.includes('MID-03')) mids.push('MID-03');
      mids.sort();

      const recMods = data.recording_module_ids || [];
      const recSummaryBadge = recMods.length > 0
        ? `<span class="badge badge-rec">● Recording Now (${recMods.length}): ${recMods.join(', ')}</span>`
        : `<span class="badge">Recording Now: None</span>`;

      document.getElementById('middles').innerHTML = mids.map(m => {
        const info = data.middle_nodes[m];
        return `<span class="badge">${m}: ${info ? info.ip + ' (' + info.active_bottom_count + ' active)' : 'offline'}</span>`;
      }).join('') + ' ' + recSummaryBadge;

      const mods = new Set(Object.keys(data.room_assignments || {}));
      const latestByMod = {};
      const vitalAnalysisByMod = {};
      const priorityByMod = {};
      const nodeStateByMod = {};

      for (const [mKey, mVal] of Object.entries(data.middle_nodes || {})) {
        (mVal.active_module_ids || []).forEach(id => mods.add(id));
        const bNodes = mVal.bottom_nodes || {};
        for (const [bId, bInfo] of Object.entries(bNodes)) {
          const isRec = Boolean(bInfo.is_recording);
          nodeStateByMod[bId] = {
            online: true,
            is_recording: isRec,
            recording_elapsed_s: (bInfo.recording_elapsed_s || 0) + (isRec ? driftSec : 0),
            sim_mode: Boolean(bInfo.sim_mode),
            room: mKey,
          };
          if (bInfo.latest_reading) latestByMod[bId] = bInfo.latest_reading;
          if (bInfo.vital_analysis) vitalAnalysisByMod[bId] = bInfo.vital_analysis;
        }
        const pMap = ((mVal.context_analysis || {}).priority_weights) || {};
        for (const [pId, pVal] of Object.entries(pMap)) {
          priorityByMod[pId] = pVal;
        }
      }
      if (mods.size === 0) { mods.add('MOD-01'); mods.add('MOD-02'); mods.add('MOD-03'); }

      const recs = data.move_recommendations || {};
      const tbody = document.getElementById('rows');
      tbody.innerHTML = Array.from(mods).sort().map(mod => {
        const cur = (data.room_assignments || {})[mod] || {mid_id: 'MID-01', mode: 'auto'};
        const st = nodeStateByMod[mod];

        let captureBadge = '<span class="badge">Offline</span>';
        if (st) {
          if (st.is_recording) {
            const simTag = st.sim_mode ? ' [SIM]' : '';
            captureBadge = `<span class="badge badge-rec">● RECORDING (${Math.round(st.recording_elapsed_s)}s)${simTag}</span>`;
          } else {
            captureBadge = '<span class="badge badge-idle">Online (Idle)</span>';
          }
        }

        const rssiMap = (data.ble_rssi || {})[mod] || {};
        const rssiKeys = Object.keys(rssiMap).sort();
        const rssiText = rssiKeys.length
          ? rssiKeys.map(k => `${k}: ${rssiMap[k].rssi} dBm`).join(' | ')
          : 'No BLE beacon detected yet';

        const rData = latestByMod[mod];
        const va = vitalAnalysisByMod[mod];
        let vitalsHtml = '<span style="color:#9ca3af;">Waiting for capture...</span>';
        if (rData) {
          const eda = fmtVal(rData.eda_peaks_2min);
          const hr = fmtVal(rData.mean_bpm, ' bpm');
          const rr = fmtVal(rData.resp_rate_min, '/m');
          const bt = fmtVal(rData.mean_temp_c, '°C');

          let comfortBadge = '';
          if (va && va.thermal_comfort) {
            const cCls = va.thermal_comfort === 'Heat-Sensitive' ? 'badge-heat' : 'badge-cold';
            const warnTag = va.thermal_mismatch ? ' (!)' : '';
            comfortBadge = `<span class="badge ${cCls}">${va.thermal_comfort}${warnTag}</span>`;
          }

          let stressBadge = '<span class="badge badge-ok">Normal</span>';
          if (va) {
            const sTypes = va.stress_types || [];
            if (sTypes.length > 0) {
              stressBadge = sTypes.map(s => {
                const domTag = (sTypes.length > 1 && s === va.dominant_stress) ? ' ★' : '';
                return `<span class="badge badge-stress">${s}${domTag}</span>`;
              }).join('');
            } else if (va.dominant_stress === 'Unknown') {
              stressBadge = '<span class="badge">Stress: N/A</span>';
            }
          }

          vitalsHtml = `<div class="mono">EDA:${eda} | HR:${hr} | RR:${rr} | Temp:${bt}</div>`
            + `<div style="margin-top:3px;">${comfortBadge}${stressBadge}</div>`;
        }

        let statText = '<span style="color:#9ca3af;">Waiting for capture...</span>';
        if (rData) {
          const sc = (rData.adaptability_score !== undefined) ? rData.adaptability_score : '-';
          const pr = (priorityByMod[mod] !== undefined) ? priorityByMod[mod] : '-';
          const nv = Array.isArray(rData.norm_vec) ? `[${rData.norm_vec.join(', ')}]` : '-';
          statText = `<div class="mono">Score: ${sc} | Pri: ${pr}</div><div class="mono" style="color:#9ca3af;">Z: ${nv}</div>`;
        }

        const rec = recs[mod];
        let recHtml = '<span class="badge badge-ok">Steady (No move)</span>';
        if (rec) {
          recHtml = `<div style="color:#fbbf24;font-weight:600;">Move → ${rec.to_mid}</div>`
            + `<div style="font-size:11px;color:#d1d5db;">${rec.reason}</div>`
            + `<button class="move-btn" onclick="assign('${mod}','${rec.to_mid}','manual')">Apply Move</button>`;
        }

        const btns = mids.map(m => {
          const cls = (cur.mid_id === m && cur.mode === 'manual') ? 'active' : '';
          return `<button class="${cls}" onclick="assign('${mod}','${m}','manual')">${m}</button>`;
        }).join('') + `<button class="auto ${cur.mode === 'auto' ? 'active' : ''}" onclick="assign('${mod}','${cur.mid_id}','auto')">AUTO (BLE)</button>`;

        return `<tr>
          <td><b>${mod}</b></td>
          <td>${captureBadge}</td>
          <td>${cur.mid_id}</td>
          <td><span class="badge">${cur.mode}</span></td>
          <td class="rssi">${rssiText}</td>
          <td>${vitalsHtml}</td>
          <td>${statText}</td>
          <td>${recHtml}</td>
          <td>${btns}</td>
        </tr>`;
      }).join('');

      const cardsDiv = document.getElementById('room-cards');
      cardsDiv.innerHTML = mids.map(m => {
        const info = data.middle_nodes[m];
        if (!info) {
          return `<div class="card"><div class="card-head"><b>${m}</b><span class="badge">offline</span></div><div style="color:#9ca3af;font-size:12px;">Waiting for Middle Pi heartbeat...</div></div>`;
        }
        const ctx = info.context_analysis || {};
        const ctrl = ctx.room_control || {};
        const cos = ctx.cosine_similarity || {};
        const sVecs = ctx.strategy_vectors || {};
        const ewVec = sVecs['Equality With Weight'] || [0, 0, 0, 0];
        const statusBadge = ctx.conflict_detected
          ? `<span class="badge badge-warn">CONFLICT DETECTED (${ctx.conflict_outlier || '-'})</span>`
          : `<span class="badge badge-ok">STEADY STATE</span>`;

        const activeList = (info.active_module_ids || []).map(id => {
          const isRec = (info.recording_module_ids || []).includes(id);
          return isRec ? `<b>${id} (REC)</b>` : id;
        }).join(', ') || 'None';

        return `<div class="card">
          <div class="card-head"><b>${m} (${info.ip})</b>${statusBadge}</div>
          <div class="kv"><span class="lbl">Active MODs</span><span class="mono">${activeList}</span></div>
          <div class="kv"><span class="lbl">Room Temp / Crowdedness</span><span class="mono">${info.rm_temp_c}°C / ${ctx.crowdedness ?? 0}</span></div>
          <div class="kv"><span class="lbl">AC Target / Device Setting</span><span class="mono"><b>${ctrl.ac_target_temp_c ?? '-'}°C</b> / <b>${ctrl.device_setting ?? 'Random'}</b></span></div>
          <div class="kv"><span class="lbl">Group Context (Eq+Wt)</span><span class="mono">[${ewVec.join(', ')}]</span></div>
          <div class="kv"><span class="lbl">CosSim (Equality)</span><span class="mono">${fmtSim(cos['Equality'])}</span></div>
          <div class="kv"><span class="lbl">CosSim (Eq With Weight)</span><span class="mono">${fmtSim(cos['Equality With Weight'])}</span></div>
          <div class="kv"><span class="lbl">CosSim (Fairness)</span><span class="mono">${fmtSim(cos['Fairness'])}</span></div>
        </div>`;
      }).join('');
    }
    refresh();
    setInterval(refresh, 3000);
    setInterval(renderDashboard, 1000);
  </script>
</body>
</html>"""


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

    def _send_html(self, code: int, html: str):
        raw = html.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/api/status"):
            with state_lock:
                snapshot = build_global_snapshot_locked()
            self._send_json(200, snapshot)
        elif parsed.path == "/rooms":
            self._send_html(200, render_rooms_html())
        elif parsed.path == "/api/export.zip":
            zip_bytes = build_export_zip_bytes()
            fname = f"vital_experiment_records_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
            self.send_header("Content-Length", str(len(zip_bytes)))
            self.end_headers()
            self.wfile.write(zip_bytes)
        elif parsed.path == "/api/route":
            qs = parse_qs(parsed.query)
            mod_id = str(qs.get("mod_id", [""])[0]).strip().upper()
            if not mod_id:
                self._send_json(400, {"error": "Missing mod_id query param"})
                return
            with state_lock:
                route_info = resolve_route_locked(mod_id)
            self._send_json(200, route_info)
        else:
            self._send_json(404, {"error": "Not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8")) if length > 0 else {}
        except json.JSONDecodeError:
            self._send_json(400, {"error": "Invalid JSON"})
            return

        client_ip = self.client_address[0]
        now = time.time()

        if self.path == "/api/reset":
            res = reset_all_cluster_data()
            code = 200 if res.get("ok") else 409
            self._send_json(code, res)
            return

        if self.path == "/api/raw_session":
            mod_id = str(payload.get("mod_id", "UNKNOWN")).strip().upper()
            raw_fname = str(payload.get("filename", "")).strip()
            csv_content = str(payload.get("csv_content", ""))
            if not raw_fname or not csv_content:
                self._send_json(400, {"error": "Provide mod_id, filename, and csv_content"})
                return
            safe_name = re.sub(r"[^A-Za-z0-9_.-]", "_", Path(raw_fname).name)
            with state_lock:
                RAW_SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
                dest = RAW_SESSIONS_DIR / safe_name
                dest.write_text(csv_content, encoding="utf-8")
            print(f"[{TOP_ID}] Saved raw 5Hz session CSV from {mod_id}: {dest}")
            self._send_json(200, {"ok": True, "saved_as": str(dest)})
            return

        if self.path == "/api/recheck":
            with state_lock:
                prune_stale_middles_locked(now)
                all_mods = set(room_assignments.keys()) | set(ble_observations.keys())
                for mod_id in sorted(all_mods):
                    if room_assignments.get(mod_id, {}).get("mode", "auto") == "auto":
                        evaluate_ble_routing_locked(mod_id, now, force_recheck=True, source_tag="RECHECK")
                snapshot = build_global_snapshot_locked()
            self._send_json(200, {"ok": True, "room_assignments": snapshot["room_assignments"]})
            return

        if self.path == "/api/assign":
            mod_id = str(payload.get("mod_id", "")).strip().upper()
            mid_id = str(payload.get("mid_id", "")).strip().upper()
            mode = str(payload.get("mode", "manual")).strip().lower()
            if not mod_id or not mid_id:
                self._send_json(400, {"error": "Provide mod_id and mid_id"})
                return
            if mode not in ("manual", "auto"):
                mode = "manual"

            with state_lock:
                prev_mid = (room_assignments.get(mod_id) or {}).get("mid_id", "")
                room_assignments[mod_id] = {
                    "mid_id": mid_id,
                    "mode": mode,
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
                if prev_mid != mid_id:
                    latest_move_recommendations.pop(mod_id, None)
                if mode == "auto":
                    evaluate_ble_routing_locked(mod_id, now, force_recheck=True, source_tag="AUTO-BTN")
                else:
                    save_assignments_locked()
                    log_routing_event_locked("MANUAL_ASSIGN", mod_id, prev_mid, mid_id, "manual", "UI_OR_API", now)
                    print(f"[{TOP_ID}] ROOM ASSIGN (MANUAL): {mod_id} -> {mid_id}")
                route_info = resolve_route_locked(mod_id)

            self._send_json(200, route_info)
            return

        if self.path == "/api/ble_scan":
            mid_id = str(payload.get("middle_id", "")).strip().upper()
            readings = payload.get("readings", {})
            if not mid_id or not isinstance(readings, dict):
                self._send_json(400, {"error": "Invalid ble_scan payload"})
                return

            ts_iso = datetime.now().isoformat(timespec="seconds")
            with state_lock:
                for mod_raw, rssi_val in readings.items():
                    mod_id = str(mod_raw).strip().upper()
                    try:
                        rssi = float(rssi_val)
                    except (TypeError, ValueError):
                        continue
                    ble_observations.setdefault(mod_id, {})[mid_id] = {"rssi": rssi, "ts": now}
                    assign_info = room_assignments.get(mod_id, {})
                    append_csv_row_locked(
                        BLE_RSSI_CSV,
                        BLE_RSSI_HEADER,
                        [
                            ts_iso,
                            mod_id,
                            mid_id,
                            rssi,
                            assign_info.get("mid_id", ""),
                            assign_info.get("mode", "auto"),
                        ],
                    )
                    evaluate_ble_routing_locked(mod_id, now, force_recheck=False, source_tag="BLE-SCAN")

            self._send_json(200, {"ok": True})
            return

        if self.path != "/api/middle":
            self._send_json(404, {"error": "Not found"})
            return

        mid = str(payload.get("middle_id", "")).strip().upper()
        if not mid:
            self._send_json(400, {"error": "Missing middle_id"})
            return

        event_type = payload.get("event", "unknown")
        listen_port = int(payload.get("listen_port", 8001))

        with state_lock:
            is_new = mid not in middle_states
            middle_states[mid] = {
                "last_seen": now,
                "ip": client_ip,
                "port": listen_port,
                "packet": payload,
            }
            snapshot = build_global_snapshot_locked()
            LATEST_JSON.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
            record_experiment_logs_locked(mid, event_type, payload, snapshot, now)

        if is_new:
            print(f"[{TOP_ID}] Middle Pi connected: {mid} ({client_ip}:{listen_port})")

        if event_type == "reading":
            print("\n" + "=" * 60)
            print(f"[{TOP_ID}] UPDATE from {mid} (trigger: {payload.get('trigger_module')})")
            for m_key, m_val in snapshot["middle_nodes"].items():
                ctx = m_val.get("context_analysis") or {}
                print(
                    f"  • {m_key} (RmTemp={m_val['rm_temp_c']}°C): {m_val['active_bottom_count']} bottom Pis {m_val['active_module_ids']} "
                    f"| Ctrl={ctx.get('room_control')} | Conflict={ctx.get('conflict_detected')}"
                )
            if snapshot["move_recommendations"]:
                print(f"  => MOVE RECOMMENDATIONS: {snapshot['move_recommendations']}")
            print("=" * 60 + "\n")

        self._send_json(200, {"ok": True, "top_id": TOP_ID})


def main():
    OUT_DIR.mkdir(exist_ok=True)
    RAW_SESSIONS_DIR.mkdir(exist_ok=True)
    load_assignments()
    stop_event = threading.Event()
    threading.Thread(target=periodic_auto_route_worker, args=(stop_event,), daemon=True).start()

    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), TopHandler)
    print(f"[{TOP_ID}] Top Coordinator listening on 0.0.0.0:{LISTEN_PORT}")
    print(f"[{TOP_ID}] Live JSON endpoint : http://0.0.0.0:{LISTEN_PORT}/api/status")
    print(f"[{TOP_ID}] Export ZIP endpoint: http://0.0.0.0:{LISTEN_PORT}/api/export.zip")
    print(f"[{TOP_ID}] Room Switcher UI   : http://0.0.0.0:{LISTEN_PORT}/rooms (Auto-check every {AUTO_CHECK_INTERVAL_S:.0f}s)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down Top Node.")
    finally:
        stop_event.set()
        server.server_close()


if __name__ == "__main__":
    main()
