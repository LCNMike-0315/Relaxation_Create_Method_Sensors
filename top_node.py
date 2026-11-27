
#!/usr/bin/env python3
# Top-Layer Cluster Coordinator & Dynamic Room Router (1 Top Pi <- 1~3 Middle Pis <- 0~10 Bottom Pis each)
# Aggregates room context vectors, tracks BLE proximity across rooms, recommends participant moves
# with a round-based post-move cooldown filter, logs experiment CSVs, and serves /rooms UI.

from __future__ import annotations

import csv
import io
import json
import os
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# --- CONFIGURATION ---
TOP_ID = os.getenv("TOP_ID") or socket.gethostname().upper()
LISTEN_PORT = int(os.getenv("TOP_PORT", "8000"))
OUT_DIR = Path("data")
EVENTS_CSV = OUT_DIR / f"{TOP_ID}_cluster_events.csv"

MIDDLE_STALE_S = float(os.getenv("MIDDLE_STALE_S", "30.0"))
BOTTOM_STALE_S = float(os.getenv("BOTTOM_STALE_S", "30.0"))
BLE_STALE_S = float(os.getenv("BLE_STALE_S", "20.0"))
AUTO_BLE_INTERVAL_S = float(os.getenv("AUTO_BLE_INTERVAL_S", "15.0"))
BLE_HYSTERESIS_DBM = float(os.getenv("BLE_HYSTERESIS_DBM", "4.0"))

# Post-move cooldown filter: suppress repeat move suggestions for N capture rounds
MOVE_COOLDOWN_ROUNDS = int(os.getenv("MOVE_COOLDOWN_ROUNDS", "3"))

COSINE_CONFLICT_THRESHOLD = float(os.getenv("COSINE_CONFLICT_THRESHOLD", "0.95"))
BODY_TEMP_THRESHOLD_C = float(os.getenv("BODY_TEMP_THRESHOLD_C", "36.0"))


def _reading_signature(r: dict | None) -> str:
    if not isinstance(r, dict) or not r:
        return ""
    if r.get("timestamp"):
        return str(r["timestamp"])
    return json.dumps(
        {
            "eda": r.get("eda_peaks_2min"),
            "bpm": r.get("mean_bpm"),
            "rr": r.get("resp_rate_min"),
            "temp": r.get("mean_temp_c"),
            "score": r.get("adaptability_score"),
        },
        sort_keys=True,
    )


class MoveCooldownTracker:
    """Prevents recommending another room move until N capture rounds complete after a move."""

    def __init__(self):
        # mod_id -> {"from_room": str, "to_room": str, "rounds": int, "last_reading_sig": str}
        self._state: dict[str, dict] = {}

    def record_move(
        self,
        mod_id: str,
        old_room: str | None,
        new_room: str | None,
        current_reading: dict | None = None,
    ):
        if not old_room or not new_room or old_room == "UNASSIGNED" or old_room == new_room:
            return
        self._state[mod_id] = {
            "from_room": old_room,
            "to_room": new_room,
            "rounds": 0,
            "last_reading_sig": _reading_signature(current_reading),
        }
        print(
            f"[{TOP_ID}] Cooldown started for {mod_id}: moved {old_room} -> {new_room} "
            f"(locked for {MOVE_COOLDOWN_ROUNDS} rounds)"
        )

    def record_capture_round(self, mod_id: str, reading: dict | None):
        entry = self._state.get(mod_id)
        if not entry:
            return
        sig = _reading_signature(reading)
        if not sig:
            return
        if sig != entry["last_reading_sig"]:
            entry["last_reading_sig"] = sig
            entry["rounds"] += 1
            print(
                f"[{TOP_ID}] Cooldown round progress for {mod_id}: "
                f"{entry['rounds']}/{MOVE_COOLDOWN_ROUNDS}"
            )

    def check(self, mod_id: str) -> tuple[bool, str]:
        entry = self._state.get(mod_id)
        if not entry:
            return False, ""
        rounds = entry["rounds"]
        if rounds >= MOVE_COOLDOWN_ROUNDS:
            del self._state[mod_id]
            return False, ""
        label = f"Cooldown: Moved -> {entry['to_room']} ({rounds}/{MOVE_COOLDOWN_ROUNDS} rounds)"
        return True, label

    def clear(self):
        self._state.clear()


state_lock = threading.Lock()
middle_nodes: dict[str, dict] = {}
participants: dict[str, dict] = {}
ble_table: dict[str, dict[str, dict]] = {}  # mod_id -> mid_id -> {"rssi": float, "ts": float}
move_cooldown = MoveCooldownTracker()


def post_json(url: str, payload: dict, timeout: float = 3.0) -> bool:
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


def append_event_csv(row: dict):
    OUT_DIR.mkdir(exist_ok=True)
    file_exists = EVENTS_CSV.exists()
    fieldnames = [
        "timestamp",
        "event",
        "middle_id",
        "module_id",
        "assigned_room",
        "rm_temp_c",
        "conflict_detected",
        "conflict_outlier",
        "recommended_room",
        "reason",
    ]
    with EVENTS_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


def ensure_participant_locked(mod_id: str) -> dict:
    if mod_id not in participants:
        participants[mod_id] = {
            "module_id": mod_id,
            "ip": "",
            "assigned_room": "UNASSIGNED",
            "mode": "auto",
            "is_recording": False,
            "recording_started_at": None,
            "recording_elapsed_s": 0.0,
            "sim_mode": False,
            "last_seen": time.time(),
            "latest_reading": None,
        }
    return participants[mod_id]


def assign_room_locked(mod_id: str, new_room: str, source: str = ""):
    p = ensure_participant_locked(mod_id)
    old_room = p.get("assigned_room") or "UNASSIGNED"
    if not new_room or new_room == old_room:
        return
    p["assigned_room"] = new_room
    move_cooldown.record_move(mod_id, old_room, new_room, p.get("latest_reading"))
    if old_room != "UNASSIGNED" and old_room in middle_nodes:
        old_mid = middle_nodes[old_room]
        mid_url = old_mid.get("url", "").rstrip("/")
        if mid_url:
            threading.Thread(
                target=post_json,
                args=(f"{mid_url}/api/detach", {"module_id": mod_id}, 2.5),
                daemon=True,
            ).start()
    print(f"[{TOP_ID}] Room assigned ({source}): {mod_id} {old_room} -> {new_room}")


def get_fresh_ble_locked(mod_id: str, now: float) -> dict[str, float]:
    raw = ble_table.get(mod_id, {})
    fresh: dict[str, float] = {}
    for mid_id, info in list(raw.items()):
        if (now - info["ts"]) <= BLE_STALE_S:
            fresh[mid_id] = float(info["rssi"])
        else:
            del raw[mid_id]
    return fresh


def run_auto_ble_routing_locked(now: float, target_mod: str | None = None) -> int:
    moved_count = 0
    mod_ids = [target_mod] if target_mod else sorted(participants.keys())
    for mod_id in mod_ids:
        p = participants.get(mod_id)
        if not p or p.get("mode", "auto") != "auto" or p.get("is_recording", False):
            continue

        fresh_rssi = get_fresh_ble_locked(mod_id, now)
        if not fresh_rssi:
            continue

        best_mid, best_rssi = max(fresh_rssi.items(), key=lambda kv: kv[1])
        cur_room = p.get("assigned_room", "UNASSIGNED")
        if cur_room == "UNASSIGNED" or cur_room not in fresh_rssi:
            assign_room_locked(mod_id, best_mid, source="auto-ble")
            moved_count += 1
        elif best_mid != cur_room:
            cur_rssi = fresh_rssi[cur_room]
            if (best_rssi - cur_rssi) >= BLE_HYSTERESIS_DBM:
                assign_room_locked(mod_id, best_mid, source="auto-ble")
                moved_count += 1
    return moved_count


def score_candidate_room(mod_id: str, p: dict, mid_id: str, mid_info: dict) -> float:
    ctx = mid_info.get("context_analysis") or {}
    rm_temp = float(ctx.get("rm_temp_c", mid_info.get("rm_temp_c", 23.0)))
    crowdedness = float(ctx.get("crowdedness", mid_info.get("active_bottom_count", 0)))
    eq_wt = (ctx.get("strategy_vectors") or {}).get("Equality With Weight") or [0.0, 0.0, 0.0, 0.0]

    r = p.get("latest_reading") or {}
    nv = r.get("norm_vec") or [0.0, 0.0, 0.0, 0.0]
    body_temp = float(r.get("mean_temp_c") or BODY_TEMP_THRESHOLD_C)

    if body_temp >= BODY_TEMP_THRESHOLD_C:
        thermal_pen = max(0.0, rm_temp - 22.0) * 0.6
    else:
        thermal_pen = max(0.0, 24.0 - rm_temp) * 0.6

    emo_overlap = sum(float(nv[i]) * float(eq_wt[i]) for i in range(min(3, len(nv), len(eq_wt))))
    conflict_pen = 1.5 if ctx.get("conflict_detected") else 0.0

    return round(thermal_pen + emo_overlap * 0.5 + crowdedness * 0.35 + conflict_pen, 4)


def compute_recommendation_locked(mod_id: str) -> dict:
    p = participants.get(mod_id)
    if not p:
        return {"should_move": False, "target_room": None, "label": "Steady (No move)", "reason": ""}

    in_cooldown, cooldown_label = move_cooldown.check(mod_id)
    if in_cooldown:
        return {
            "should_move": False,
            "target_room": None,
            "cooldown_active": True,
            "label": f"Steady ({cooldown_label})",
            "reason": cooldown_label,
        }

    cur_room = p.get("assigned_room", "UNASSIGNED")
    if cur_room == "UNASSIGNED" or cur_room not in middle_nodes:
        return {"should_move": False, "target_room": None, "label": "Steady (No move)", "reason": ""}

    if p.get("is_recording", False):
        return {
            "should_move": False,
            "target_room": None,
            "label": "Locked (Recording in progress)",
            "reason": "",
        }

    r = p.get("latest_reading")
    if not isinstance(r, dict):
        return {"should_move": False, "target_room": None, "label": "Steady (No move)", "reason": ""}

    cur_mid = middle_nodes[cur_room]
    cur_ctx = cur_mid.get("context_analysis") or {}
    conflict_detected = bool(cur_ctx.get("conflict_detected", False))
    conflict_outlier = cur_ctx.get("conflict_outlier")

    other_rooms = [m_id for m_id in sorted(middle_nodes.keys()) if m_id != cur_room]
    if not other_rooms or not conflict_detected:
        return {"should_move": False, "target_room": None, "label": "Steady (No move)", "reason": ""}

    if conflict_outlier and conflict_outlier != mod_id and int(cur_ctx.get("crowdedness", 1)) > 1:
        return {"should_move": False, "target_room": None, "label": "Steady (No move)", "reason": ""}

    best_room = min(
        other_rooms,
        key=lambda m_id: score_candidate_room(mod_id, p, m_id, middle_nodes[m_id]),
    )
    best_temp = middle_nodes[best_room].get("rm_temp_c", 23.0)
    reason = f"Conflict Outlier (Fairness priority) -> {best_room} ({best_temp}°C)"
    return {
        "should_move": True,
        "target_room": best_room,
        "cooldown_active": False,
        "label": f"Move -> {best_room}",
        "reason": reason,
    }


def build_cluster_snapshot_locked() -> dict:
    now = time.time()
    mids_out = {}
    for mid_id in sorted(middle_nodes.keys()):
        m = middle_nodes[mid_id]
        stale = (now - m["last_seen"]) > MIDDLE_STALE_S
        assigned_here = sorted(
            mod_id for mod_id, p in participants.items() if p.get("assigned_room") == mid_id
        )
        mids_out[mid_id] = {
            "middle_id": mid_id,
            "ip": m.get("ip", ""),
            "url": m.get("url", ""),
            "online": not stale,
            "last_seen_ago_s": round(now - m["last_seen"], 1),
            "rm_temp_c": m.get("rm_temp_c", 23.0),
            "rm_temp_source": m.get("rm_temp_source", "default"),
            "active_bottom_count": len(assigned_here),
            "active_module_ids": assigned_here,
            "recording_module_ids": m.get("recording_module_ids", []),
            "context_analysis": m.get("context_analysis", {}),
        }

    mods_out = {}
    recording_now = []
    for mod_id in sorted(participants.keys()):
        p = participants[mod_id]
        online = (now - p["last_seen"]) <= BOTTOM_STALE_S
        is_rec = bool(p.get("is_recording", False))
        started_at = p.get("recording_started_at")
        if is_rec:
            recording_now.append(mod_id)
            if started_at is not None:
                live_elapsed = max(float(p.get("recording_elapsed_s", 0.0)), now - float(started_at))
            else:
                live_elapsed = float(p.get("recording_elapsed_s", 0.0))
        else:
            live_elapsed = 0.0

        ble_signals = get_fresh_ble_locked(mod_id, now)
        rec = compute_recommendation_locked(mod_id)
        mods_out[mod_id] = {
            "module_id": mod_id,
            "ip": p.get("ip", ""),
            "online": online,
            "last_seen_ago_s": round(now - p["last_seen"], 1),
            "assigned_room": p.get("assigned_room", "UNASSIGNED"),
            "mode": p.get("mode", "auto"),
            "is_recording": is_rec,
            "recording_elapsed_s": round(live_elapsed, 1),
            "sim_mode": bool(p.get("sim_mode", False)),
            "ble_signals": ble_signals,
            "latest_reading": p.get("latest_reading"),
            "recommendation": rec,
        }

    directory = {mid_id: m["url"] for mid_id, m in mids_out.items() if m.get("url")}

    return {
        "top_id": TOP_ID,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "cooldown_config": {"rounds": MOVE_COOLDOWN_ROUNDS},
        "recording_now": recording_now,
        "directory": directory,
        "middle_nodes": mids_out,
        "participants": mods_out,
    }


def auto_ble_worker(stop_event: threading.Event):
    while not stop_event.is_set():
        with state_lock:
            run_auto_ble_routing_locked(time.time())
        stop_event.wait(AUTO_BLE_INTERVAL_S)


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Vital Cluster — Dynamic Room Router</title>
<style>
  body { background:#0b1120; color:#e2e8f0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; margin:0; padding:18px 22px; }
  h1 { font-size:20px; margin:0 0 4px 0; color:#f8fafc; }
  .sub { font-size:12px; color:#94a3b8; margin-bottom:12px; }
  .toolbar { display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin-bottom:12px; }
  button { border:none; border-radius:6px; padding:7px 13px; font-weight:600; font-size:12px; cursor:pointer; color:#fff; }
  .btn-purple { background:#7c3aed; }
  .btn-green { background:#059669; }
  .btn-red { background:#dc2626; }
  .btn-blue { background:#2563eb; padding:5px 9px; font-size:11px; }
  .btn-amber { background:#d97706; padding:5px 10px; font-size:11px; margin-top:4px; }
  .btn-auto { background:#0d9488; padding:5px 9px; font-size:11px; }
  .btn-dim { opacity:0.55; }
  .pills { display:flex; flex-wrap:wrap; gap:8px; margin-bottom:14px; font-size:11px; }
  .pill { background:#1e293b; border:1px solid #334155; border-radius:999px; padding:4px 10px; color:#cbd5e1; }
  table { width:100%; border-collapse:collapse; background:#111827; border:1px solid #1f2937; border-radius:8px; overflow:hidden; font-size:12px; margin-bottom:20px; }
  th { background:#1e293b; color:#94a3b8; text-align:left; padding:9px 10px; font-size:11px; border-bottom:1px solid #334155; }
  td { padding:10px; border-bottom:1px solid #1f2937; vertical-align:middle; }
  .badge { display:inline-block; padding:2px 7px; border-radius:999px; font-size:10px; font-weight:700; margin-right:4px; margin-top:3px; }
  .b-online { background:#1e3a8a; color:#93c5fd; }
  .b-rec { background:#7f1d1d; color:#fca5a5; }
  .b-cold { background:#0369a1; color:#e0f2fe; }
  .b-emo { background:#b91c1c; color:#fee2e2; }
  .b-norm { background:#065f46; color:#d1fae5; }
  .b-steady { background:#065f46; color:#6ee7b7; }
  .b-conflict { background:#991b1b; color:#fecaca; }
  .cards { display:grid; grid-template-columns:repeat(auto-fit, minmax(290px, 1fr)); gap:14px; }
  .card { background:#111827; border:1px solid #1f2937; border-radius:8px; padding:12px 14px; }
  .card-hdr { display:flex; justify-content:space-between; align-items:center; margin-bottom:10px; font-weight:700; font-size:13px; }
  .kv { display:flex; justify-content:space-between; font-size:11px; padding:3px 0; color:#cbd5e1; }
  .kv span:first-child { color:#94a3b8; }
  .steady-txt { color:#34d399; font-weight:600; }
  .cooldown-txt { color:#fbbf24; font-weight:600; }
  .move-txt { color:#fbbf24; font-weight:700; }
  .muted { color:#64748b; }
</style>
</head>
<body>
  <h1>Dynamic Room Router & Context Control</h1>
  <div class="sub" id="subline">Timer automatically checks closest room every 15s when in AUTO (BLE) mode (locked during recording).</div>
  <div class="toolbar">
    <button class="btn-purple" onclick="postAction('/api/check_closest', {})">Check Closest Now</button>
    <button class="btn-green" onclick="window.location='/api/export_zip'">Download All Experiment Data (.ZIP)</button>
    <button class="btn-red" onclick="resetAll()">Initialize (Clear All Data)</button>
  </div>
  <div class="pills" id="pills"></div>
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
    <tbody id="mod-rows"></tbody>
  </table>
  <h2 style="font-size:14px;margin:0 0 10px 0;color:#cbd5e1;">Room Context Vectors, Cosine Similarity & Device Settings</h2>
  <div class="cards" id="room-cards"></div>

<script>
async function postAction(url, body) {
  await fetch(url, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body || {})
  });
  refresh();
}
function resetAll() {
  if (!confirm('Clear all experiment readings, room assignments, and move cooldowns?')) return;
  if (!confirm('Confirm again: Initialize and delete current session data?')) return;
  postAction('/api/reset', {});
}
function fmtBle(ble) {
  const keys = Object.keys(ble || {}).sort();
  if (!keys.length) return '<span class="muted">No BLE beacon detected yet</span>';
  return keys.map(k => `${k}: ${ble[k]} dBm`).join(' | ');
}
function fmtVitals(r) {
  if (!r) return '<span class="muted">Waiting for capture...</span>';
  const eda = r.eda_peaks_2min ?? '-';
  const hr = r.mean_bpm != null ? Number(r.mean_bpm).toFixed(1) + ' bpm' : '-';
  const rr = r.resp_rate_min != null ? Math.round(r.resp_rate_min) + '/m' : '-';
  const temp = r.mean_temp_c != null ? r.mean_temp_c + '° C' : '-';
  const sens = (Number(r.mean_temp_c || 36) < 36.0) ? 'Cold-Sensitive' : 'Warm-Sensitive';
  const emo = r.dominant_emotion || 'Normal';
  const emoCls = emo === 'Normal' ? 'b-norm' : 'b-emo';
  return `<div>EDA:${eda} | HR:${hr} | RR:${rr} | Temp:${temp}</div>
          <div><span class="badge b-cold">${sens}</span><span class="badge ${emoCls}">${emo}</span></div>`;
}
function fmtAdapt(r, priMap, modId) {
  if (!r) return '<span class="muted">Waiting for capture...</span>';
  const sc = r.adaptability_score != null ? Number(r.adaptability_score).toFixed(3) : '-';
  const pri = priMap && priMap[modId] != null ? Number(priMap[modId]).toFixed(3) : '-';
  const z = Array.isArray(r.norm_vec) ? '[' + r.norm_vec.map(v => Number(v).toFixed(2)).join(', ') + ']' : '-';
  return `<div>Score: ${sc} | Pri: ${pri}</div><div class="muted">Z: ${z}</div>`;
}
function tickRecordingTimers() {
  const nowMs = Date.now();
  document.querySelectorAll('.b-rec[data-rec-base]').forEach(el => {
    const base = Number(el.getAttribute('data-rec-base') || 0);
    const fetched = Number(el.getAttribute('data-rec-fetched') || nowMs);
    const sec = Math.max(0, Math.floor(base + (nowMs - fetched) / 1000));
    el.textContent = `Recording (${sec}s)`;
  });
}
async function refresh() {
  const res = await fetch('/api/state');
  const data = await res.json();
  const fetchedAtMs = Date.now();
  const mids = Object.values(data.middle_nodes || {});
  const mods = Object.values(data.participants || {});
  const cd = data.cooldown_config || {rounds: 3};

  document.getElementById('subline').textContent =
    `Timer checks closest room every 15s in AUTO (BLE) mode (locked during recording). Post-move cooldown: ${cd.rounds} rounds.`;

  const pills = mids.map(m =>
    `<span class="pill">${m.middle_id}: ${m.ip} (${m.active_bottom_count} active)</span>`
  );
  const recTxt = (data.recording_now && data.recording_now.length) ? data.recording_now.join(', ') : 'None';
  pills.push(`<span class="pill">Recording Now: ${recTxt}</span>`);
  document.getElementById('pills').innerHTML = pills.join('');

  const midIds = mids.map(m => m.middle_id);
  const rowsHtml = mods.map(p => {
    const r = p.latest_reading;
    const assigned = p.assigned_room || 'UNASSIGNED';
    const midCtx = (data.middle_nodes[assigned] || {}).context_analysis || {};
    const priMap = midCtx.priority_weights || {};
    const baseSec = Number(p.recording_elapsed_s || 0);
    const statusBadge = p.is_recording
      ? `<span class="badge b-rec" data-rec-base="${baseSec}" data-rec-fetched="${fetchedAtMs}">Recording (${Math.floor(baseSec)}s)</span>`
      : `<span class="badge b-online">${p.online ? 'Online (Idle)' : 'Offline'}</span>`;

    const rec = p.recommendation || {};
    let recHtml = `<span class="steady-txt">${rec.label || 'Steady (No move)'}</span>`;
    if (rec.cooldown_active) {
      recHtml = `<span class="cooldown-txt">${rec.label}</span>`;
    } else if (rec.should_move && rec.target_room) {
      recHtml = `<div class="move-txt">${rec.label}</div>
                 <div class="muted" style="font-size:11px;">${rec.reason || ''}</div>
                 <button class="btn-amber" onclick="postAction('/api/move', {module_id:'${p.module_id}', target_room:'${rec.target_room}'})">Apply Move</button>`;
    }

    const roomBtns = midIds.map(mId =>
      `<button class="btn-blue ${assigned === mId && p.mode === 'manual' ? '' : 'btn-dim'}"
        onclick="postAction('/api/switch_room', {module_id:'${p.module_id}', room:'${mId}', mode:'manual'})">${mId}</button>`
    ).join(' ');
    const autoBtn = `<button class="btn-auto ${p.mode === 'auto' ? '' : 'btn-dim'}"
      onclick="postAction('/api/switch_room', {module_id:'${p.module_id}', mode:'auto'})">AUTO (BLE)</button>`;

    return `<tr>
      <td><strong>${p.module_id}</strong></td>
      <td>${statusBadge}</td>
      <td><strong>${assigned}</strong></td>
      <td>${p.mode}</td>
      <td>${fmtBle(p.ble_signals)}</td>
      <td>${fmtVitals(r)}</td>
      <td>${fmtAdapt(r, priMap, p.module_id)}</td>
      <td>${recHtml}</td>
      <td>${roomBtns} ${autoBtn}</td>
    </tr>`;
  }).join('');
  document.getElementById('mod-rows').innerHTML = rowsHtml || '<tr><td colspan="9" class="muted">No participants connected yet.</td></tr>';

  const cardsHtml = mids.map(m => {
    const ctx = m.context_analysis || {};
    const conflict = Boolean(ctx.conflict_detected);
    const outlier = ctx.conflict_outlier || '';
    const badge = conflict
      ? `<span class="badge b-conflict">CONFLICT DETECTED (${outlier})</span>`
      : `<span class="badge b-steady">STEADY STATE</span>`;
    const activeMods = (m.active_module_ids && m.active_module_ids.length) ? m.active_module_ids.join(', ') : 'None';
    const crowd = (m.active_bottom_count / 10).toFixed(1);
    const ctrl = ctx.room_control || {};
    const ew = (ctx.strategy_vectors || {})['Equality With Weight'] || [0,0,0,0];
    const cos = ctx.cosine_similarity || {};
    const srcTag = m.rm_temp_source === 'microbit' ? ' (micro:bit)' : '';

    return `<div class="card">
      <div class="card-hdr"><span>${m.middle_id} (${m.ip})</span>${badge}</div>
      <div class="kv"><span>Active MODs</span><span>${activeMods}</span></div>
      <div class="kv"><span>Room Temp / Crowdedness</span><span>${m.rm_temp_c}° C${srcTag} / ${crowd}</span></div>
      <div class="kv"><span>AC Target / Device Setting</span><span>${ctrl.ac_target_temp_c ?? 22}°C / ${ctrl.device_setting ?? 'Standby'}</span></div>
      <div class="kv"><span>Group Context (Eq+Wt)</span><span>[${ew.join(', ')}]</span></div>
      <div class="kv"><span>CosSim (Equality)</span><span>${cos.Equality ?? '-'}</span></div>
      <div class="kv"><span>CosSim (Eq With Weight)</span><span>${cos['Equality With Weight'] ?? '-'}</span></div>
      <div class="kv"><span>CosSim (Fairness)</span><span>${cos.Fairness ?? '-'}</span></div>
    </div>`;
  }).join('');
  document.getElementById('room-cards').innerHTML = cardsHtml;
}
refresh();
setInterval(refresh, 2000);
setInterval(tickRecordingTimers, 1000);
</script>
</body>
</html>
"""


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

    def _send_html(self, html: str):
        raw = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        if path in ("/", "/rooms"):
            self._send_html(DASHBOARD_HTML)
            return

        if path in ("/api/state", "/api/rooms", "/api/status"):
            with state_lock:
                snap = build_cluster_snapshot_locked()
            self._send_json(200, snap)
            return

        if path in ("/api/directory", "/api/module"):
            mod_id = (qs.get("module_id") or [""])[0].strip().upper()
            with state_lock:
                snap = build_cluster_snapshot_locked()
                p_info = snap["participants"].get(mod_id, {})
                assigned = p_info.get("assigned_room", "UNASSIGNED")
                mid_url = snap["directory"].get(assigned, "")
                rec = p_info.get("recommendation", {"should_move": False, "target_room": None})
            self._send_json(
                200,
                {
                    "top_id": TOP_ID,
                    "directory": snap["directory"],
                    "assigned_room": assigned,
                    "middle_url": mid_url,
                    "recommendation": rec,
                },
            )
            return

        if path == "/api/export_zip":
            OUT_DIR.mkdir(exist_ok=True)
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for p in sorted(OUT_DIR.glob("*")):
                    if p.is_file():
                        zf.write(p, arcname=p.name)
                with state_lock:
                    snap = build_cluster_snapshot_locked()
                zf.writestr("cluster_snapshot.json", json.dumps(snap, indent=2))
            raw = buf.getvalue()
            fname = f"vital_experiment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return

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

        if self.path == "/api/ble_scan":
            mid_id = str(payload.get("middle_id", "")).strip().upper()
            readings = payload.get("readings") or {}
            if not mid_id or not isinstance(readings, dict):
                self._send_json(400, {"error": "Invalid ble_scan payload"})
                return
            with state_lock:
                for raw_mod, rssi in readings.items():
                    mod_id = str(raw_mod).strip().upper()
                    ensure_participant_locked(mod_id)
                    ble_table.setdefault(mod_id, {})[mid_id] = {
                        "rssi": float(rssi),
                        "ts": now,
                    }
            self._send_json(200, {"ok": True})
            return

        if self.path == "/api/middle":
            mid_id = str(payload.get("middle_id", "")).strip().upper()
            if not mid_id:
                self._send_json(400, {"error": "Missing middle_id"})
                return
            listen_port = int(payload.get("listen_port", 8001))
            event_type = str(payload.get("event", "sync"))
            trigger_mod = str(payload.get("trigger_module", "")).strip().upper()

            with state_lock:
                middle_nodes[mid_id] = {
                    "middle_id": mid_id,
                    "ip": client_ip,
                    "url": f"http://{client_ip}:{listen_port}",
                    "last_seen": now,
                    "rm_temp_c": payload.get("rm_temp_c", 23.0),
                    "rm_temp_source": payload.get("rm_temp_source", "default"),
                    "recording_module_ids": payload.get("recording_module_ids", []),
                    "context_analysis": payload.get("context_analysis", {}),
                }

                b_nodes = payload.get("bottom_nodes") or {}
                for raw_mod, b_info in b_nodes.items():
                    mod_id = str(raw_mod).strip().upper()
                    p = ensure_participant_locked(mod_id)
                    p["ip"] = b_info.get("ip", p["ip"])
                    p["last_seen"] = now
                    p["sim_mode"] = bool(b_info.get("sim_mode", False))

                    prev_rec = bool(p.get("is_recording", False))
                    new_rec = bool(b_info.get("is_recording", False))
                    rep_elapsed = float(b_info.get("recording_elapsed_s", 0.0))
                    p["is_recording"] = new_rec
                    if new_rec:
                        if not prev_rec or p.get("recording_started_at") is None or rep_elapsed > 0:
                            p["recording_started_at"] = now - rep_elapsed
                        p["recording_elapsed_s"] = max(rep_elapsed, now - float(p["recording_started_at"]))
                    else:
                        p["recording_started_at"] = None
                        p["recording_elapsed_s"] = 0.0

                    if p["assigned_room"] == "UNASSIGNED":
                        assign_room_locked(mod_id, mid_id, source="initial-attach")

                    latest_r = b_info.get("latest_reading")
                    if isinstance(latest_r, dict):
                        p["latest_reading"] = latest_r
                        move_cooldown.record_capture_round(mod_id, latest_r)

                if event_type == "reading" and trigger_mod:
                    rec = compute_recommendation_locked(trigger_mod)
                    ctx = payload.get("context_analysis") or {}
                    append_event_csv(
                        {
                            "timestamp": payload.get("timestamp", datetime.now().isoformat(timespec="seconds")),
                            "event": event_type,
                            "middle_id": mid_id,
                            "module_id": trigger_mod,
                            "assigned_room": participants.get(trigger_mod, {}).get("assigned_room", ""),
                            "rm_temp_c": payload.get("rm_temp_c", ""),
                            "conflict_detected": ctx.get("conflict_detected", False),
                            "conflict_outlier": ctx.get("conflict_outlier", ""),
                            "recommended_room": rec.get("target_room") or "",
                            "reason": rec.get("reason") or rec.get("label") or "",
                        }
                    )

            self._send_json(200, {"ok": True, "top_id": TOP_ID})
            return

        if self.path == "/api/move":
            mod_id = str(payload.get("module_id", "")).strip().upper()
            target_room = str(payload.get("target_room", "")).strip().upper()
            if not mod_id or not target_room:
                self._send_json(400, {"error": "Missing module_id or target_room"})
                return
            with state_lock:
                p = ensure_participant_locked(mod_id)
                p["mode"] = "manual"
                assign_room_locked(mod_id, target_room, source="apply-move")
                append_event_csv(
                    {
                        "timestamp": datetime.now().isoformat(timespec="seconds"),
                        "event": "apply_move",
                        "middle_id": target_room,
                        "module_id": mod_id,
                        "assigned_room": target_room,
                        "reason": "User clicked Apply Move",
                    }
                )
            self._send_json(200, {"ok": True, "module_id": mod_id, "assigned_room": target_room})
            return

        if self.path == "/api/switch_room":
            mod_id = str(payload.get("module_id", "")).strip().upper()
            mode = str(payload.get("mode", "manual")).strip().lower()
            room = str(payload.get("room", "")).strip().upper()
            if not mod_id:
                self._send_json(400, {"error": "Missing module_id"})
                return
            with state_lock:
                p = ensure_participant_locked(mod_id)
                p["mode"] = "auto" if mode == "auto" else "manual"
                if p["mode"] == "manual" and room:
                    assign_room_locked(mod_id, room, source="manual-switch")
                elif p["mode"] == "auto":
                    run_auto_ble_routing_locked(now, target_mod=mod_id)
            self._send_json(200, {"ok": True, "module_id": mod_id, "mode": p["mode"]})
            return

        if self.path == "/api/check_closest":
            with state_lock:
                moved = run_auto_ble_routing_locked(now)
            self._send_json(200, {"ok": True, "moved": moved})
            return

        if self.path == "/api/reset":
            with state_lock:
                mid_urls = [m["url"] for m in middle_nodes.values() if m.get("url")]
                for p in participants.values():
                    p["latest_reading"] = None
                    p["is_recording"] = False
                    p["recording_started_at"] = None
                    p["recording_elapsed_s"] = 0.0
                move_cooldown.clear()
            for url in mid_urls:
                post_json(f"{url}/api/reset", {}, timeout=2.5)
            self._send_json(200, {"ok": True})
            return

        self._send_json(404, {"error": "Not found"})


def main():
    OUT_DIR.mkdir(exist_ok=True)
    stop_event = threading.Event()
    threading.Thread(target=auto_ble_worker, args=(stop_event,), daemon=True).start()

    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), TopHandler)
    print(
        f"[{TOP_ID}] Top Cluster Coordinator listening on 0.0.0.0:{LISTEN_PORT} "
        f"(Move cooldown: {MOVE_COOLDOWN_ROUNDS} rounds)"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down Top Node.")
    finally:
        stop_event.set()
        server.server_close()


if __name__ == "__main__":
    main()
