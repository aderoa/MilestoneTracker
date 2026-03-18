#!/usr/bin/env python3
"""
NBA Milestone Detector — Nightly Update Script
================================================
Fetches live 2025-26 box scores from a published Google Sheet,
updates career states from the historical baseline, detects
milestones, and outputs milestones.json for the web app.

Usage:
    python update_milestones.py

Requirements:
    - Python 3.7+
    - requests (pip install requests)
    - milestone_baseline.bin in the same folder

Output:
    - milestones.json (upload to GitHub repo)
"""

import csv
import gzip
import io
import json
import os
import sys
import copy
from datetime import datetime
from collections import defaultdict

# === CONFIG ===
SHEET_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vSp9Dyp62wra-_9vCmOlSzuelR8RkigcQsRX8MJs0s9Npabi7r0eVFA6deVdmd19X5DJc5V5Ci2m-nc"
    "/pub?gid=0&single=true&output=csv"
)
BASELINE_FILE = "milestone_baseline.bin"
OUTPUT_FILE = "milestones.json"
SEASON_START = datetime(2025, 10, 1)

# === MILESTONE THRESHOLDS ===
CAREER_PTS_MARKS = [1000, 2000, 3000, 4000, 5000, 7500, 10000, 12500, 15000, 17500, 20000, 22500, 25000, 27500, 30000, 35000, 40000]
CAREER_REB_MARKS = [1000, 2000, 3000, 4000, 5000, 7500, 10000, 12500, 15000]
CAREER_AST_MARKS = [1000, 2000, 3000, 4000, 5000, 7500, 10000, 12500]
CAREER_STL_MARKS = [500, 1000, 1500, 2000, 2500, 3000]
CAREER_BLK_MARKS = [500, 1000, 1500, 2000, 2500, 3000]
CAREER_TPM_MARKS = [500, 1000, 1500, 2000, 2500, 3000, 3500]
CAREER_GP_MARKS  = [250, 500, 750, 1000, 1100, 1200, 1300, 1400, 1500, 1600]
TEAM_PTS_MARKS   = [1000, 2000, 3000, 5000, 7500, 10000, 15000, 20000, 25000]
TEAM_GP_MARKS    = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]

# Streak thresholds that are noteworthy
STREAK_THRESHOLDS = {
    "str_pts20": [10, 15, 20, 25, 30, 40, 50],
    "str_pts25": [10, 15, 20, 25, 30, 40],
    "str_pts30": [5, 10, 15, 20, 25, 30],
    "str_reb10": [5, 10, 15, 20, 25, 30],
    "str_ast10": [5, 10, 15, 20, 25, 30],
    "str_dd":    [5, 10, 15, 20, 25, 30],
    "str_td":    [2, 3, 4, 5, 7, 10],
    "str_tpm":   [50, 75, 100, 125, 150, 175, 200, 250, 300],
}

STREAK_LABELS = {
    "str_pts20": "consecutive games with 20+ PTS",
    "str_pts25": "consecutive games with 25+ PTS",
    "str_pts30": "consecutive games with 30+ PTS",
    "str_reb10": "consecutive games with 10+ REB",
    "str_ast10": "consecutive games with 10+ AST",
    "str_dd":    "consecutive double-doubles",
    "str_td":    "consecutive triple-doubles",
    "str_tpm":   "consecutive games with a 3-pointer",
}

# Team milestone thresholds
TEAM_SCORE_HIGHS = [130, 140, 150, 160]
TEAM_OPP_LOWS = [90, 85, 80]
TEAM_TPM_GAME = [20, 25]
TEAM_MARGIN = [30, 40, 50]
TEAM_WIN_MARKS = [10, 20, 30, 40, 50, 60, 65, 70, 73]
TEAM_SEASON_TPM = [500, 750, 1000, 1250, 1500]


def safe_int(v):
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return 0


def load_baseline():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), BASELINE_FILE)
    if not os.path.exists(path):
        print(f"ERROR: {BASELINE_FILE} not found.")
        sys.exit(1)
    print(f"Loading {BASELINE_FILE}...")
    with gzip.open(path, "rb") as f:
        data = json.loads(f.read())
    print(f"  {len(data)} players loaded")
    return data


def fetch_2526_games():
    try:
        import requests
    except ImportError:
        print("ERROR: pip install requests")
        sys.exit(1)

    print(f"Fetching 2025-26 box scores...")
    resp = requests.get(SHEET_CSV_URL, timeout=30)
    resp.raise_for_status()

    reader = csv.reader(io.StringIO(resp.text))
    try:
        next(reader); next(reader)
    except StopIteration:
        pass

    blocks = []
    current_block = None

    for row in reader:
        if len(row) < 23:
            continue
        date_str = row[0].strip()
        player = row[1].strip()
        if not date_str or "/" not in date_str:
            continue
        try:
            dt = datetime.strptime(date_str, "%m/%d/%Y")
        except ValueError:
            continue
        if dt < SEASON_START:
            continue

        team = row[22].strip() if len(row) > 22 else ""

        if player == "TOTALS":
            if current_block:
                blocks.append(current_block)
            current_block = None
            continue
        if player in ("PLAYER", ""):
            continue

        # Parse full box score
        game_data = {
            "player": player, "dt": dt, "team": team,
            "min": row[2].strip(),
            "fgm": safe_int(row[3]), "fga": safe_int(row[4]),
            "tpm": safe_int(row[6]), "tpa": safe_int(row[7]),
            "ftm": safe_int(row[9]), "fta": safe_int(row[10]),
            "oreb": safe_int(row[12]), "dreb": safe_int(row[13]),
            "reb": safe_int(row[14]), "ast": safe_int(row[15]),
            "stl": safe_int(row[16]), "blk": safe_int(row[17]),
            "tov": safe_int(row[18]), "pf": safe_int(row[19]),
            "pts": safe_int(row[20]),
        }

        if current_block is None or current_block[0] != date_str or current_block[1] != team:
            if current_block:
                blocks.append(current_block)
            current_block = (date_str, team, [])

        current_block[2].append(game_data)

    if current_block:
        blocks.append(current_block)

    # Pair opponents
    games_by_date = defaultdict(list)
    i = 0
    while i < len(blocks) - 1:
        d1, t1, p1 = blocks[i]
        d2, t2, p2 = blocks[i + 1]
        if d1 == d2:
            for g in p1:
                g["opp"] = t2
                games_by_date[d1].append(g)
            for g in p2:
                g["opp"] = t1
                games_by_date[d1].append(g)
            i += 2
        else:
            i += 1

    total = sum(len(v) for v in games_by_date.values())
    print(f"  {total:,} player-games across {len(games_by_date)} dates")
    return games_by_date


def crossed(old_val, new_val, marks):
    """Return list of thresholds crossed between old and new value."""
    return [m for m in marks if old_val < m <= new_val]


def detect_milestones(state, games_by_date):
    """Process each game date, update state, detect milestones."""
    print("Detecting milestones...")
    all_milestones = {}  # date_str -> [milestone, ...]

    # Sort dates chronologically
    sorted_dates = sorted(games_by_date.keys(),
                          key=lambda d: datetime.strptime(d, "%m/%d/%Y"))

    # Track season stats for quirky milestones
    season_stats = defaultdict(lambda: {
        "gp": 0, "pts": 0, "reb": 0, "ast": 0, "stl": 0, "blk": 0,
        "tpm": 0, "fgm": 0, "fga": 0, "ftm": 0, "fta": 0, "tov": 0, "pf": 0,
        "pts_high": 0, "reb_high": 0, "ast_high": 0,
        "team": ""
    })

    for date_str in sorted_dates:
        day_games = games_by_date[date_str]
        day_milestones = []
        dt = datetime.strptime(date_str, "%m/%d/%Y")
        iso_date = dt.strftime("%Y-%m-%d")

        for g in day_games:
            name = g["player"]
            pts, reb, ast = g["pts"], g["reb"], g["ast"]
            stl, blk, tpm = g["stl"], g["blk"], g["tpm"]
            fgm, fga, ftm, fta = g["fgm"], g["fga"], g["ftm"], g["fta"]
            team, opp = g["team"], g.get("opp", "")

            # Initialize state for new players
            if name not in state:
                state[name] = {
                    "gp": 0, "pts": 0, "reb": 0, "ast": 0, "stl": 0, "blk": 0,
                    "tpm": 0, "fgm": 0, "fga": 0, "ftm": 0, "fta": 0,
                    "teams": {},
                    "str_pts20": 0, "str_pts25": 0, "str_pts30": 0,
                    "str_reb10": 0, "str_ast10": 0,
                    "str_dd": 0, "str_td": 0, "str_tpm": 0, "str_fta0": 0,
                }

            s = state[name]
            old = {k: v for k, v in s.items() if k != "teams"}
            old_team_pts = s["teams"].get(team, {}).get("pts", 0)
            old_team_gp = s["teams"].get(team, {}).get("gp", 0)

            # Update cumulative
            s["gp"] += 1
            s["pts"] += pts
            s["reb"] += reb
            s["ast"] += ast
            s["stl"] += stl
            s["blk"] += blk
            s["tpm"] += tpm
            s["fgm"] += fgm
            s["fga"] += fga
            s["ftm"] += ftm
            s["fta"] += fta

            # Per-team
            if team not in s["teams"]:
                s["teams"][team] = {"gp": 0, "pts": 0}
            s["teams"][team]["gp"] += 1
            s["teams"][team]["pts"] += pts

            # Update streaks
            s["str_pts20"] = s["str_pts20"] + 1 if pts >= 20 else 0
            s["str_pts25"] = s["str_pts25"] + 1 if pts >= 25 else 0
            s["str_pts30"] = s["str_pts30"] + 1 if pts >= 30 else 0
            s["str_reb10"] = s["str_reb10"] + 1 if reb >= 10 else 0
            s["str_ast10"] = s["str_ast10"] + 1 if ast >= 10 else 0
            dd_cats = sum(1 for v in [pts, reb, ast, stl, blk] if v >= 10)
            s["str_dd"] = s["str_dd"] + 1 if dd_cats >= 2 else 0
            s["str_td"] = s["str_td"] + 1 if dd_cats >= 3 else 0
            s["str_tpm"] = s["str_tpm"] + 1 if tpm >= 1 else 0
            s["str_fta0"] = s["str_fta0"] + 1 if fta == 0 else 0

            # Update season stats
            ss = season_stats[name]
            ss["gp"] += 1
            ss["pts"] += pts; ss["reb"] += reb; ss["ast"] += ast
            ss["stl"] += stl; ss["blk"] += blk; ss["tpm"] += tpm
            ss["fgm"] += fgm; ss["fga"] += fga; ss["ftm"] += ftm; ss["fta"] += fta
            ss["tov"] += g["tov"]; ss["pf"] += g["pf"]
            ss["team"] = team
            if pts > ss["pts_high"]:
                ss["pts_high"] = pts
            if reb > ss["reb_high"]:
                ss["reb_high"] = reb
            if ast > ss["ast_high"]:
                ss["ast_high"] = ast

            base_info = {
                "player": name, "team": team, "opp": opp,
                "pts": pts, "reb": reb, "ast": ast,
                "stl": stl, "blk": blk, "tpm": tpm,
                "line": f"{pts} PTS, {reb} REB, {ast} AST"
            }

            # === 1. CUMULATIVE MILESTONES ===
            for marks, stat, label in [
                (CAREER_PTS_MARKS, "pts", "career points"),
                (CAREER_REB_MARKS, "reb", "career rebounds"),
                (CAREER_AST_MARKS, "ast", "career assists"),
                (CAREER_STL_MARKS, "stl", "career steals"),
                (CAREER_BLK_MARKS, "blk", "career blocks"),
                (CAREER_TPM_MARKS, "tpm", "career 3-pointers"),
                (CAREER_GP_MARKS,  "gp",  "career games"),
            ]:
                for m in crossed(old[stat], s[stat], marks):
                    day_milestones.append({
                        **base_info,
                        "type": "cumulative",
                        "cat": label,
                        "milestone": f"{m:,} {label}",
                        "value": s[stat],
                        "threshold": m,
                        "priority": 1 if m >= 10000 else 2,
                    })

            # Team-specific
            new_team_pts = s["teams"][team]["pts"]
            new_team_gp = s["teams"][team]["gp"]
            for m in crossed(old_team_pts, new_team_pts, TEAM_PTS_MARKS):
                day_milestones.append({
                    **base_info,
                    "type": "cumulative",
                    "cat": f"points with {team}",
                    "milestone": f"{m:,} points with {team}",
                    "value": new_team_pts,
                    "threshold": m,
                    "priority": 2,
                })
            for m in crossed(old_team_gp, new_team_gp, TEAM_GP_MARKS):
                day_milestones.append({
                    **base_info,
                    "type": "cumulative",
                    "cat": f"games with {team}",
                    "milestone": f"{m:,} games with {team}",
                    "value": new_team_gp,
                    "threshold": m,
                    "priority": 2,
                })

            # === 2. STREAK MILESTONES ===
            for streak_key, thresholds in STREAK_THRESHOLDS.items():
                old_streak = old.get(streak_key, 0)
                new_streak = s[streak_key]
                for t in thresholds:
                    if old_streak < t <= new_streak:
                        day_milestones.append({
                            **base_info,
                            "type": "streak",
                            "cat": STREAK_LABELS.get(streak_key, streak_key),
                            "milestone": f"{t} {STREAK_LABELS.get(streak_key, streak_key)}",
                            "value": new_streak,
                            "threshold": t,
                            "priority": 1 if t >= 20 else 2,
                        })

            # === 3. QUIRKY / SEASON ===
            # Season high in points (only if 30+)
            if pts >= 30 and pts == ss["pts_high"] and ss["gp"] > 1:
                day_milestones.append({
                    **base_info,
                    "type": "season",
                    "cat": "season high",
                    "milestone": f"New season-high {pts} points",
                    "value": pts,
                    "threshold": pts,
                    "priority": 3,
                })

            # 0 FGA game (quirky)
            if fga == 0 and s["gp"] > 50:
                day_milestones.append({
                    **base_info,
                    "type": "quirky",
                    "cat": "zero FGA",
                    "milestone": f"0 field goal attempts",
                    "value": 0,
                    "threshold": 0,
                    "priority": 3,
                })

            # Triple-double
            if dd_cats >= 3:
                day_milestones.append({
                    **base_info,
                    "type": "single_game",
                    "cat": "triple-double",
                    "milestone": f"Triple-double: {pts}/{reb}/{ast}",
                    "value": 0,
                    "threshold": 0,
                    "priority": 4,
                })

            # 5x5
            if all(v >= 5 for v in [pts, reb, ast, stl, blk]):
                day_milestones.append({
                    **base_info,
                    "type": "single_game",
                    "cat": "5x5",
                    "milestone": f"5x5: {pts}/{reb}/{ast}/{stl}/{blk}",
                    "value": 0,
                    "threshold": 0,
                    "priority": 3,
                })

            # 50-point game
            if pts >= 50:
                day_milestones.append({
                    **base_info,
                    "type": "single_game",
                    "cat": "50-point game",
                    "milestone": f"{pts}-point game",
                    "value": pts,
                    "threshold": 50,
                    "priority": 3,
                })

            # 20/20 game
            if pts >= 20 and reb >= 20:
                day_milestones.append({
                    **base_info,
                    "type": "single_game",
                    "cat": "20/20 game",
                    "milestone": f"20/20 game: {pts} PTS / {reb} REB",
                    "value": 0,
                    "threshold": 0,
                    "priority": 3,
                })

        if day_milestones:
            # Sort: priority asc, then by threshold desc
            day_milestones.sort(key=lambda m: (m["priority"], -m.get("threshold", 0)))
            all_milestones[iso_date] = day_milestones

    player_ms = sum(len(v) for v in all_milestones.values())
    print(f"  {player_ms} player milestones across {len(all_milestones)} dates")

    # === TEAM MILESTONES ===
    print("  Detecting team milestones...")
    # Aggregate team game totals per date
    team_games = {}  # (date_str, team) -> {pts, tpm, opp}
    for date_str in sorted_dates:
        dt = datetime.strptime(date_str, "%m/%d/%Y")
        iso_date = dt.strftime("%Y-%m-%d")
        for g in games_by_date[date_str]:
            team = g["team"]
            opp = g.get("opp", "")
            k = (iso_date, team)
            if k not in team_games:
                team_games[k] = {"pts": 0, "tpm": 0, "opp": opp}
            team_games[k]["pts"] += g["pts"]
            team_games[k]["tpm"] += g["tpm"]

    # Process team games chronologically
    team_season_state = {}  # team -> {gp, wins, tpm}
    team_ms_count = 0

    for k in sorted(team_games.keys()):
        iso_date, team = k
        g = team_games[k]
        opp = g["opp"]
        opp_k = (iso_date, opp)
        opp_pts = team_games[opp_k]["pts"] if opp_k in team_games else 0

        if team not in team_season_state:
            team_season_state[team] = {"gp": 0, "wins": 0, "tpm": 0}
        ts = team_season_state[team]
        old_wins, old_tpm_s = ts["wins"], ts["tpm"]

        ts["gp"] += 1
        ts["tpm"] += g["tpm"]
        if g["pts"] > opp_pts and g["pts"] > 0:
            ts["wins"] += 1
        margin = g["pts"] - opp_pts

        base_t = {
            "player": team, "team": team, "opp": opp,
            "pts": g["pts"], "reb": 0, "ast": 0, "stl": 0, "blk": 0, "tpm": g["tpm"],
            "line": f"{g['pts']} PTS (opp: {opp_pts})"
        }
        day_ms = []

        for m in crossed(old_wins, ts["wins"], TEAM_WIN_MARKS):
            day_ms.append({**base_t, "type": "team", "cat": "season wins",
                "milestone": f"{team}: {m} wins this season",
                "value": ts["wins"], "threshold": m,
                "priority": 1 if m >= 60 else 2})

        for m in crossed(old_tpm_s, ts["tpm"], TEAM_SEASON_TPM):
            day_ms.append({**base_t, "type": "team", "cat": "season 3-pointers",
                "milestone": f"{team}: {m:,} team 3-pointers this season",
                "value": ts["tpm"], "threshold": m, "priority": 2})

        for t in TEAM_SCORE_HIGHS:
            if g["pts"] >= t:
                day_ms.append({**base_t, "type": "team", "cat": "team scoring",
                    "milestone": f"{team} scored {g['pts']} points",
                    "value": g["pts"], "threshold": t, "priority": 3})
                break

        if opp_pts > 0:
            for t in TEAM_OPP_LOWS:
                if opp_pts <= t:
                    day_ms.append({**base_t, "type": "team", "cat": "team defense",
                        "milestone": f"{team} held {opp} to {opp_pts} points",
                        "value": opp_pts, "threshold": t, "priority": 3})
                    break

        for t in TEAM_TPM_GAME:
            if g["tpm"] >= t:
                day_ms.append({**base_t, "type": "team", "cat": "team 3-pointers",
                    "milestone": f"{team} made {g['tpm']} 3-pointers in one game",
                    "value": g["tpm"], "threshold": t, "priority": 3})
                break

        if margin > 0:
            for t in TEAM_MARGIN:
                if margin >= t:
                    day_ms.append({**base_t, "type": "team", "cat": "blowout win",
                        "milestone": f"{team} won by {margin} points ({g['pts']}-{opp_pts})",
                        "value": margin, "threshold": t, "priority": 3})
                    break

        if day_ms:
            if iso_date not in all_milestones:
                all_milestones[iso_date] = []
            all_milestones[iso_date].extend(day_ms)
            team_ms_count += len(day_ms)

    # Sort milestones within each date
    for iso_date in all_milestones:
        all_milestones[iso_date].sort(key=lambda m: (m["priority"], -m.get("threshold", 0)))

    total_ms = sum(len(v) for v in all_milestones.values())
    print(f"  {team_ms_count} team milestones")
    print(f"  {total_ms} total milestones across {len(all_milestones)} dates")
    return all_milestones


def save_output(milestones):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUTPUT_FILE)
    with open(path, "w") as f:
        json.dump(milestones, f, separators=(",", ":"))
    size_kb = os.path.getsize(path) / 1024
    print(f"\nSaved {OUTPUT_FILE} ({size_kb:.0f} KB)")

    # Show recent milestones summary
    dates = sorted(milestones.keys(), reverse=True)
    print(f"\nMost recent milestones:")
    for d in dates[:5]:
        ms = milestones[d]
        print(f"\n  {d} ({len(ms)} milestones):")
        for m in ms[:8]:
            emoji = {"cumulative": "🎯", "streak": "🔥", "quirky": "🤪", "single_game": "⭐", "season": "📈", "team": "🏀"}.get(m["type"], "•")
            print(f"    {emoji} {m['player']} ({m['team']} vs {m['opp']}): {m['milestone']} [{m['line']}]")
        if len(ms) > 8:
            print(f"    ... +{len(ms) - 8} more")


def main():
    print("=" * 56)
    print("  NBA MILESTONE DETECTOR — Update Script")
    print("=" * 56)
    print()

    state = load_baseline()
    games_by_date = fetch_2526_games()
    milestones = detect_milestones(state, games_by_date)
    save_output(milestones)
    rebuild_query_data(games_by_date)
    print("\nDone! ✓")


QUERY_BASELINE_FILE = "query_baseline.bin"
QUERY_OUTPUT_FILE = "query_data.bin"

PTS_THRESH = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
REB_THRESH = [3, 5, 8, 10, 12, 15, 20]
AST_THRESH = [3, 5, 8, 10, 12, 15, 20]
STL_THRESH = [1, 2, 3, 4, 5]
BLK_THRESH = [1, 2, 3, 4, 5]
TPM_THRESH = [1, 2, 3, 4, 5, 6]


def rebuild_query_data(games_by_date):
    """Rebuild query_data.bin from query_baseline + 2025-26 games."""
    qb_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), QUERY_BASELINE_FILE)
    if not os.path.exists(qb_path):
        print(f"  {QUERY_BASELINE_FILE} not found, skipping query data rebuild")
        return

    print(f"\nRebuilding query data...")
    with gzip.open(qb_path, "rb") as f:
        qb = json.loads(f.read())

    career = qb["career"]
    sc = qb["streak_cur"]
    sb = qb["streak_best"]
    gh = qb["game_high"]
    tp = {tuple(k.split("|||")): v for k, v in qb["tp"].items()}
    ts_hist = {tuple(k.split("|||")): v for k, v in qb["ts"].items()}
    bio = qb["bio"]
    countries = qb["countries"]
    colleges = qb["colleges"]
    country_idx = {c: i for i, c in enumerate(countries)}
    college_idx = {c: i for i, c in enumerate(colleges)}
    states = qb.get("states", [])
    bio_states = qb.get("bio_states", {})
    state_idx = {s: i for i, s in enumerate(states)}

    # Team season for 2025-26
    ts_2526 = {}

    # Pre-compute team scores per game for win determination
    team_game_scores = {}  # (iso, team) -> total_pts
    for date_str in sorted(games_by_date.keys(), key=lambda d: datetime.strptime(d, "%m/%d/%Y")):
        dt = datetime.strptime(date_str, "%m/%d/%Y")
        iso = dt.strftime("%Y-%m-%d")
        for g in games_by_date[date_str]:
            k = (iso, g["team"])
            team_game_scores[k] = team_game_scores.get(k, 0) + g["pts"]

    # Sort dates and process
    sorted_dates = sorted(games_by_date.keys(),
                          key=lambda d: datetime.strptime(d, "%m/%d/%Y"))

    for date_str in sorted_dates:
        dt = datetime.strptime(date_str, "%m/%d/%Y")
        iso = dt.strftime("%Y-%m-%d")

        for g in games_by_date[date_str]:
            name = g["player"]
            pts, reb, ast = g["pts"], g["reb"], g["ast"]
            stl, blk, tpm = g["stl"], g["blk"], g["tpm"]
            fgm, fga = g["fgm"], g["fga"]
            ftm, fta = g.get("ftm", 0), g.get("fta", 0)
            team = g["team"]
            opp = g.get("opp", "")
            my_score = team_game_scores.get((iso, team), 0)
            opp_score = team_game_scores.get((iso, opp), 0)
            won = 1 if my_score > opp_score and opp_score > 0 else 0

            # Career
            if name not in career:
                career[name] = {"gp": 0, "pts": 0, "reb": 0, "ast": 0, "stl": 0, "blk": 0,
                                "tpm": 0, "fgm": 0, "fga": 0, "ftm": 0, "fta": 0, "tov": 0, "pf": 0, "wins": 0, "fy": 2026, "ly": 2026}
            c = career[name]
            tov = g.get("tov", 0); pf = g.get("pf", 0)
            c["gp"] += 1; c["pts"] += pts; c["reb"] += reb; c["ast"] += ast
            c["stl"] += stl; c["blk"] += blk; c["tpm"] += tpm
            c["fgm"] += fgm; c["fga"] += fga; c["ftm"] += ftm; c["fta"] += fta
            c["tov"] = c.get("tov", 0) + tov; c["pf"] = c.get("pf", 0) + pf
            c["wins"] = c.get("wins", 0) + won
            c["ly"] = 2026

            # Game highs
            if name not in gh:
                gh[name] = {"pts": 0, "reb": 0, "ast": 0, "stl": 0, "blk": 0, "tpm": 0}
            g_h = gh[name]
            if pts > g_h["pts"]: g_h["pts"] = pts
            if reb > g_h["reb"]: g_h["reb"] = reb
            if ast > g_h["ast"]: g_h["ast"] = ast
            if stl > g_h["stl"]: g_h["stl"] = stl
            if blk > g_h["blk"]: g_h["blk"] = blk
            if tpm > g_h["tpm"]: g_h["tpm"] = tpm

            # Team-player
            k = (name, team)
            if k not in tp:
                tp[k] = {"gp": 0, "pts": 0, "reb": 0, "ast": 0, "tpm": 0, "stl": 0, "blk": 0, "wins": 0}
            tp[k]["gp"] += 1; tp[k]["pts"] += pts; tp[k]["reb"] += reb
            tp[k]["ast"] += ast; tp[k]["tpm"] += tpm
            tp[k]["stl"] = tp[k].get("stl", 0) + stl; tp[k]["blk"] = tp[k].get("blk", 0) + blk
            tp[k]["wins"] = tp[k].get("wins", 0) + won

            # Streaks
            if name not in sc:
                sc[name] = {}; sb[name] = {}
            s_c = sc[name]; s_b = sb[name]
            dd = sum(1 for v in [pts, reb, ast, stl, blk] if v >= 10)
            for t in PTS_THRESH:
                k2 = f"p{t}"; s_c[k2] = s_c.get(k2, 0) + 1 if pts >= t else 0; s_b[k2] = max(s_b.get(k2, 0), s_c[k2])
            for t in REB_THRESH:
                k2 = f"r{t}"; s_c[k2] = s_c.get(k2, 0) + 1 if reb >= t else 0; s_b[k2] = max(s_b.get(k2, 0), s_c[k2])
            for t in AST_THRESH:
                k2 = f"a{t}"; s_c[k2] = s_c.get(k2, 0) + 1 if ast >= t else 0; s_b[k2] = max(s_b.get(k2, 0), s_c[k2])
            for t in STL_THRESH:
                k2 = f"s{t}"; s_c[k2] = s_c.get(k2, 0) + 1 if stl >= t else 0; s_b[k2] = max(s_b.get(k2, 0), s_c[k2])
            for t in BLK_THRESH:
                k2 = f"b{t}"; s_c[k2] = s_c.get(k2, 0) + 1 if blk >= t else 0; s_b[k2] = max(s_b.get(k2, 0), s_c[k2])
            for t in TPM_THRESH:
                k2 = f"t{t}"; s_c[k2] = s_c.get(k2, 0) + 1 if tpm >= t else 0; s_b[k2] = max(s_b.get(k2, 0), s_c[k2])
            s_c["dd"] = s_c.get("dd", 0) + 1 if dd >= 2 else 0; s_b["dd"] = max(s_b.get("dd", 0), s_c["dd"])
            s_c["td"] = s_c.get("td", 0) + 1 if dd >= 3 else 0; s_b["td"] = max(s_b.get("td", 0), s_c["td"])
            s_c["w"] = s_c.get("w", 0) + 1 if won else 0; s_b["w"] = max(s_b.get("w", 0), s_c["w"])

            # Team season 2025-26
            if team:
                if team not in ts_2526:
                    ts_2526[team] = {"gp_set": set(), "pts": 0, "tpm": 0, "reb": 0, "ast": 0, "wins": 0}
                ts_2526[team]["pts"] += pts; ts_2526[team]["tpm"] += tpm
                ts_2526[team]["reb"] += reb; ts_2526[team]["ast"] += ast
                if iso not in ts_2526[team]["gp_set"]:
                    ts_2526[team]["gp_set"].add(iso)
                    ts_2526[team]["wins"] += won
                ts_2526[team]["gp_set"].add(iso)

    # Build output arrays
    P_SKEYS = ["p5","p10","p15","p20","p25","p30","p35","p40","p45","p50",
               "r3","r5","r8","r10","r12","r15","r20",
               "a3","a5","a8","a10","a12","a15","a20",
               "s1","s2","s3","s4","s5",
               "b1","b2","b3","b4","b5",
               "t1","t2","t3","t4","t5","t6",
               "dd","td","w"]

    players_arr = []
    for name, c in career.items():
        b = bio.get(name, {})
        s_b_p = sb.get(name, {})
        g_h = gh.get(name, {"pts": 0, "reb": 0, "ast": 0, "stl": 0, "blk": 0, "tpm": 0})
        cid = country_idx.get(b.get("country", ""), -1)
        colid = college_idx.get(b.get("college", ""), -1)
        pos = b.get("pos", "")
        ht = b.get("height", "")
        draft = 0
        try: draft = int(b.get("draft", "0"))
        except: pass
        pick = 0
        try: pick = int(b.get("pick", "0"))
        except: pass
        row = [name, c["gp"], c["pts"], c["reb"], c["ast"], c["stl"], c["blk"], c["tpm"],
               c["fgm"], c["fga"], c["ftm"], c["fta"], c.get("tov", 0), c.get("pf", 0), c.get("wins", 0), c["fy"], c["ly"],
               cid, colid, pos, ht, draft, pick]
        for sk in P_SKEYS:
            row.append(s_b_p.get(sk, 0))
        row.extend([g_h["pts"], g_h["reb"], g_h["ast"], g_h["stl"], g_h["blk"], g_h["tpm"]])
        players_arr.append(row)

    tp_arr = [[k[0], k[1], v["gp"], v["pts"], v["reb"], v["ast"], v["tpm"], v.get("stl",0), v.get("blk",0), v.get("wins",0)] for k, v in tp.items()]

    # Merge historical team seasons + 2025-26
    ts_arr = []
    for k, v in ts_hist.items():
        team, syr_str = k
        syr = int(syr_str)
        ts_arr.append([team, syr, v["gp"], v["pts"], v["tpm"], v["reb"], v["ast"], v.get("wins", 0)])
    for team, v in ts_2526.items():
        gp = len(v["gp_set"])
        if gp > 0:
            ts_arr.append([team, 2026, gp, v["pts"], v["tpm"], v["reb"], v["ast"], v.get("wins", 0)])

    streak_config = {
        "pts": {"label": "Points", "thresholds": PTS_THRESH},
        "reb": {"label": "Rebounds", "thresholds": REB_THRESH},
        "ast": {"label": "Assists", "thresholds": AST_THRESH},
        "stl": {"label": "Steals", "thresholds": STL_THRESH},
        "blk": {"label": "Blocks", "thresholds": BLK_THRESH},
        "tpm": {"label": "3-Pointers Made", "thresholds": TPM_THRESH},
        "dd": {"label": "Double-Doubles", "thresholds": [1]},
        "td": {"label": "Triple-Doubles", "thresholds": [1]},
        "w": {"label": "Wins", "thresholds": [1]},
    }

    # Build player_states lookup
    player_states = {}
    for p in players_arr:
        name = p[0]
        st = bio_states.get(name, "")
        player_states[name] = state_idx.get(st, -1)

    output = {"p": players_arr, "tp": tp_arr, "ts": ts_arr,
              "countries": countries, "colleges": colleges,
              "states": states, "player_states": player_states,
              "streak_config": streak_config, "streak_keys": P_SKEYS}

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), QUERY_OUTPUT_FILE)
    json_str = json.dumps(output, separators=(",", ":"))
    with gzip.open(out_path, "wb", compresslevel=9) as f:
        f.write(json_str.encode("utf-8"))
    print(f"  Saved {QUERY_OUTPUT_FILE} ({os.path.getsize(out_path)/1024:.0f} KB)")
    print(f"  {len(players_arr)} players, {len(tp_arr)} team-player, {len(ts_arr)} team-seasons")


if __name__ == "__main__":
    main()
