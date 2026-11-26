#!/usr/bin/env python3
# Top-Layer Raspberry Pi Coordinator (1 Top Pi <- 3 Middle Pis <- 0~10 Bottom Pis each)
# Acts as Central Room Directory: supports periodic BLE RSSI Auto-Routing + Manual/API Override.
# Prunes offline Middle Pis automatically, computes cross-room movement recommendations,
# and displays live recording status, room context & controls on /rooms.

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
from urllib.parse import parse_qs, urlparse

# --- CONFIGURATION ---
TOP_ID = os.getenv("TOP_ID") or socket.gethostname().upper()
LISTEN_PORT = int(os.getenv("TOP_PORT", "8000"))
OUT_DIR = Path("data")
MASTER_CSV = OUT_DIR / "TOP_master_readings.csv"
LATEST_JSON = OUT_DIR / "TOP_latest_state.json"
ASSIGNMENTS_JSON = OUT_DIR / "TOP_room_assignments.json"

BLE_STALE_S = 20.0
BLE_HYSTERESIS_DBM = float(os.getenv("BLE_HYSTERESIS_DBM", "3.0"))
AUTO_CHECK_INTERVAL_S = float(os.getenv("AUTO_CHECK_INTERVAL_S", "15.0"))
MIDDLE_STALE_S = float(os.getenv("MIDDLE_STALE_S", "35.0"))

TEMP_THRESHOLD_C = 23.0
BODY_TEMP_THRESHOLD_C = 36.0
EMOTION_NAMES = ["Anxiety", "Displeasure", "Anger"]

MASTER_HEADER = [
    "top_received_at", "middle_id", "trigger_module",
    "active_bottom_count", "reporting_bottom_count",
    "group_mean_eda", "group_mean_bpm", "group_mean_resp", "group_mean_temp_c",
    "raw_json"
]

state_lock = threading.Lock()
middle_states: dict[str, dict] = {}
room_assignments: dict[str, dict] = {}
ble_observations: dict[str, dict[str, dict]] = {}


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
    prune_stale_middles_locked(time.time())
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

    m_info = middle_states.get(target_mid)
    middle_url = f"http://{m_info['ip']}:{m_info.get('port', 8001)}" if m_info else ""

    return {
        "ok": True,
        "mod_id": mod_id,
        "mid_id": target_mid,
        "middle_url": middle_url,
        "mode": mode,
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
            default_mid = default_middle_id_locked()
            room_assignments[mod_id] = {
                "mid_id": default_mid,
                "mode": "auto",
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
            save_assignments_locked()
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
            save_assignments_locked()
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
        save_assignments_locked()
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
            r = b_info.get("latest_reading") or {}
            norm_vec = r.get("norm_vec")
            body_temp = r.get("mean_temp_c")

            active_emotions = []
            if isinstance(norm_vec, list) and len(norm_vec) >= 3:
                active_emotions = [
                    EMOTION_NAMES[i]
                    for i in range(3)
                    if float(norm_vec[i]) >= 1.0
                ]

            comfort = None
            if body_temp is not None:
                comfort = "Heat-Sensitive" if float(body_temp) >= BODY_TEMP_THRESHOLD_C else "Cold-Sensitive"

            thermal_mismatch = (
                (comfort == "Heat-Sensitive" and current_temp >= TEMP_THRESHOLD_C)
                or (comfort == "Cold-Sensitive" and current_temp < TEMP_THRESHOLD_C)
            )

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
        total_bottom_active += pkt.get("active_bottom_count", 0)
        for b_id, b_info in pkt.get("bottom_nodes", {}).items():
            if b_info.get("is_recording"):
                recording_mods.add(b_id)
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
            "port": info.get("port", 8001),
            "last_seen_ago_s": round(now - info["last_seen"], 1),
            "rm_temp_c": pkt.get("rm_temp_c", TEMP_THRESHOLD_C),
            "active_bottom_count": pkt.get("active_bottom_count", 0),
            "active_module_ids": pkt.get("active_module_ids", []),
            "recording_module_ids": pkt.get("recording_module_ids", []),
            "group_summary": pkt.get("group_summary", {}),
            "context_analysis": pkt.get("context_analysis", {}),
            "bottom_nodes": pkt.get("bottom_nodes", {}),
        }

    move_recommendations = compute_move_recommendations_locked(middles_out)

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


def render_rooms_html() -> str:
    return """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Vital Cluster — Dynamic Room Router & Context Control</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; max-width: 1200px; margin: 24px auto; padding: 0 16px; background: #111827; color: #f3f4f6; }
    h1 { font-size: 20px; margin-bottom: 6px; }
    h2 { font-size: 16px; margin: 24px 0 10px 0; color: #e5e7eb; }
    p.sub { color: #9ca3af; font-size: 13px; margin-top: 0; }
    table { width: 100%; border-collapse: collapse; margin-top: 12px; background: #1f2937; border-radius: 8px; overflow: hidden; }
    th, td { padding: 10px 12px; border-bottom: 1px solid #374151; text-align: left; font-size: 13px; vertical-align: middle; }
    th { background: #0f172a; color: #9ca3af; font-weight: 600; }
    button { background: #2563eb; color: #fff; border: none; padding: 6px 10px; border-radius: 6px; cursor: pointer; margin-right: 4px; font-size: 12px; }
    button.auto { background: #059669; }
    button.check { background: #7c3aed; margin-left: 8px; }
    button.move-btn { background: #d97706; font-weight: 600; margin-top: 4px; }
    button.active { outline: 2px solid #fbbf24; font-weight: 700; }
    .badge { padding: 2px 8px; border-radius: 999px; font-size: 11px; background: #374151; display: inline-block; margin-right: 4px; }
    .badge-ok { background: #065f46; color: #a7f3d0; }
    .badge-warn { background: #991b1b; color: #fecaca; font-weight: 700; }
    .badge-rec { background: #b91c1c; color: #fef2f2; font-weight: 700; animation: pulseRec 1.4s infinite; }
    .badge-idle { background: #1e3a8a; color: #bfdbfe; }
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
    function fmtSim(v) {
      return (v === null || v === undefined) ? '-' : Number(v).toFixed(4);
    }
    async function refresh() {
      const res = await fetch('/api/status');
      const data = await res.json();
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
      const priorityByMod = {};
      const nodeStateByMod = {};

      for (const [mKey, mVal] of Object.entries(data.middle_nodes || {})) {
        (mVal.active_module_ids || []).forEach(id => mods.add(id));
        const bNodes = mVal.bottom_nodes || {};
        for (const [bId, bInfo] of Object.entries(bNodes)) {
          nodeStateByMod[bId] = {
            online: true,
            is_recording: Boolean(bInfo.is_recording),
            recording_elapsed_s: bInfo.recording_elapsed_s || 0,
            sim_mode: Boolean(bInfo.sim_mode),
            room: mKey,
          };
          if (bInfo.latest_reading) latestByMod[bId] = bInfo.latest_reading;
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
                room_assignments[mod_id] = {
                    "mid_id": mid_id,
                    "mode": mode,
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
                if mode == "auto":
                    evaluate_ble_routing_locked(mod_id, now, force_recheck=True, source_tag="AUTO-BTN")
                else:
                    save_assignments_locked()
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

            with state_lock:
                for mod_raw, rssi_val in readings.items():
                    mod_id = str(mod_raw).strip().upper()
                    try:
                        rssi = float(rssi_val)
                    except (TypeError, ValueError):
                        continue
                    ble_observations.setdefault(mod_id, {})[mid_id] = {"rssi": rssi, "ts": now}
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

            if event_type == "reading":
                gs = payload.get("group_summary", {})
                write_header = not MASTER_CSV.exists()
                with open(MASTER_CSV, "a", newline="") as mf:
                    mw = csv.writer(mf)
                    if write_header:
                        mw.writerow(MASTER_HEADER)
                    mw.writerow([
                        datetime.now().isoformat(timespec="seconds"),
                        mid,
                        payload.get("trigger_module", ""),
                        payload.get("active_bottom_count", 0),
                        payload.get("reporting_bottom_count", 0),
                        gs.get("mean_eda_peaks_2min", ""),
                        gs.get("mean_bpm", ""),
                        gs.get("mean_resp_rate_min", ""),
                        gs.get("mean_temp_c", ""),
                        json.dumps(payload),
                    ])

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
    load_assignments()
    stop_event = threading.Event()
    threading.Thread(target=periodic_auto_route_worker, args=(stop_event,), daemon=True).start()

    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), TopHandler)
    print(f"[{TOP_ID}] Top Coordinator listening on 0.0.0.0:{LISTEN_PORT}")
    print(f"[{TOP_ID}] Live JSON endpoint: http://0.0.0.0:{LISTEN_PORT}/api/status")
    print(f"[{TOP_ID}] Room Switcher UI  : http://0.0.0.0:{LISTEN_PORT}/rooms (Auto-check every {AUTO_CHECK_INTERVAL_S:.0f}s)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down Top Node.")
    finally:
        stop_event.set()
        server.server_close()


if __name__ == "__main__":
    main()