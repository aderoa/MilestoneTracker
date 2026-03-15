#!/usr/bin/env python3
"""
NBA Records — Nightly Leaderboard Update
Fetches 2025-26 box scores, merges with historical baseline,
recomputes all leaderboards, outputs leaderboards.json.
"""
import csv, gzip, io, json, os, sys
from datetime import datetime
from collections import defaultdict

SHEET_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vSp9Dyp62wra-_9vCmOlSzuelR8RkigcQsRX8MJs0s9Npabi7r0eVFA6deVdmd19X5DJc5V5Ci2m-nc"
    "/pub?gid=0&single=true&output=csv"
)
BASELINE_FILE = "records_baseline.bin"
OUTPUT_FILE = "leaderboards.json"
TOP = 50
STREAK_KEYS = ["pts20","pts25","pts30","reb10","ast10","dd","td","tpm1"]

def safe_int(v):
    try: return int(float(v))
    except: return 0

def load_baseline():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), BASELINE_FILE)
    if not os.path.exists(path):
        print(f"ERROR: {BASELINE_FILE} not found."); sys.exit(1)
    with gzip.open(path, "rb") as f:
        data = json.loads(f.read())
    print(f"Baseline: {len(data['career'])} players")
    return data

def fetch_2526():
    import requests
    print("Fetching 2025-26 box scores...")
    resp = requests.get(SHEET_CSV_URL, timeout=30); resp.raise_for_status()
    reader = csv.reader(io.StringIO(resp.text))
    try: next(reader); next(reader)
    except: pass
    blocks = []; cb = None
    for row in reader:
        if len(row) < 23: continue
        ds = row[0].strip(); pl = row[1].strip()
        if not ds or "/" not in ds: continue
        try: dt = datetime.strptime(ds, "%m/%d/%Y")
        except: continue
        if dt < datetime(2025, 10, 1): continue
        tm = row[22].strip()
        if pl == "TOTALS":
            if cb: blocks.append(cb)
            cb = None; continue
        if pl in ("PLAYER", ""): continue
        gd = {"player": pl, "dt": dt, "team": tm,
              "pts": safe_int(row[20]), "reb": safe_int(row[14]), "ast": safe_int(row[15]),
              "stl": safe_int(row[16]), "blk": safe_int(row[17]), "tpm": safe_int(row[6]),
              "fgm": safe_int(row[3]), "fga": safe_int(row[4])}
        if cb is None or cb[0] != ds or cb[1] != tm:
            if cb: blocks.append(cb)
            cb = (ds, tm, [])
        cb[2].append(gd)
    if cb: blocks.append(cb)
    games = []
    i = 0
    while i < len(blocks) - 1:
        d1,t1,p1 = blocks[i]; d2,t2,p2 = blocks[i+1]
        if d1 == d2:
            for g in p1: g["opp"] = t2; games.append(g)
            for g in p2: g["opp"] = t1; games.append(g)
            i += 2
        else: i += 1
    print(f"  {len(games)} player-games")
    return games

def rebuild(baseline, games):
    career = baseline["career"]
    sc = baseline["streak_current"]
    sb = baseline["streak_best"]
    tp = {tuple(k.split("|||")): v for k, v in baseline["team_player"].items()}

    # Process 2025-26 games sorted by date
    games.sort(key=lambda g: g["dt"])
    ts_state = {}  # team -> {gp, pts, tpm}
    for g in games:
        name = g["player"]; tm = g["team"]
        pts,reb,ast,stl,blk,tpm = g["pts"],g["reb"],g["ast"],g["stl"],g["blk"],g["tpm"]
        if name not in career:
            career[name] = {"gp":0,"pts":0,"reb":0,"ast":0,"stl":0,"blk":0,"tpm":0,"fgm":0,"fga":0,"first_yr":2026,"last_yr":2026}
        c = career[name]; c["gp"]+=1;c["pts"]+=pts;c["reb"]+=reb;c["ast"]+=ast;c["stl"]+=stl;c["blk"]+=blk;c["tpm"]+=tpm
        c["fgm"]+=g["fgm"];c["fga"]+=g["fga"];c["last_yr"]=2026

        k = (name, tm)
        if k not in tp: tp[k] = {"gp":0,"pts":0}
        tp[k]["gp"]+=1; tp[k]["pts"]+=pts

        if name not in sc:
            sc[name] = {k:0 for k in STREAK_KEYS}
            sb[name] = {k:0 for k in STREAK_KEYS}
        dd = sum(1 for v in [pts,reb,ast,stl,blk] if v>=10)
        checks = {"pts20":pts>=20,"pts25":pts>=25,"pts30":pts>=30,"reb10":reb>=10,"ast10":ast>=10,"dd":dd>=2,"td":dd>=3,"tpm1":tpm>=1}
        for sk, p in checks.items():
            sc[name][sk] = sc[name][sk]+1 if p else 0
            sb[name][sk] = max(sb[name][sk], sc[name][sk])

        if tm not in ts_state: ts_state[tm] = {"gp":0,"pts":0,"tpm":0}
        # avoid double counting per game
        # (simplified: we count player contributions)
        ts_state[tm]["pts"] += pts; ts_state[tm]["tpm"] += tpm

    # Build leaderboards
    print("Building leaderboards...")
    career_leaders = {}
    for stat in ["pts","reb","ast","stl","blk","tpm","gp"]:
        top = sorted(career.items(), key=lambda x:-x[1][stat])[:TOP]
        career_leaders[stat] = [{"name":n,"value":c[stat],"gp":c["gp"],
            "years":f"{c.get('first_yr',0)}-{c.get('last_yr',0)}"} for n,c in top]

    streak_labels = {"pts20":"Consecutive 20+ PTS games","pts25":"Consecutive 25+ PTS games",
        "pts30":"Consecutive 30+ PTS games","reb10":"Consecutive 10+ REB games",
        "ast10":"Consecutive 10+ AST games","dd":"Consecutive double-doubles",
        "td":"Consecutive triple-doubles","tpm1":"Consecutive games with a 3-pointer"}
    streak_leaders = {}
    for sk in STREAK_KEYS:
        top = sorted(sb.items(), key=lambda x:-x[1].get(sk,0))[:TOP]
        streak_leaders[sk] = [{"name":n,"value":s[sk],"gp":career.get(n,{}).get("gp",0),
            "years":career.get(n,{}).get("first_yr",0)} for n,s in top if s[sk]>0]

    top_tp = sorted(tp.items(), key=lambda x:-x[1]["pts"])[:TOP]
    team_pts = [{"name":k[0],"team":k[1],"value":v["pts"],"gp":v["gp"]} for k,v in top_tp]
    top_tg = sorted(tp.items(), key=lambda x:-x[1]["gp"])[:TOP]
    team_gp = [{"name":k[0],"team":k[1],"value":v["gp"],"pts":v["pts"]} for k,v in top_tg]

    # Team season - simplified (just 2025-26 added to historical)
    # For a full rebuild we'd need all historical team seasons
    # Keep existing season data from leaderboards.json if available
    existing = {}
    existing_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUTPUT_FILE)
    if os.path.exists(existing_path):
        with open(existing_path) as f:
            existing = json.load(f)

    return {
        "career": career_leaders, "streaks": streak_leaders, "streak_labels": streak_labels,
        "team_pts": team_pts, "team_gp": team_gp,
        "season_pts": existing.get("season_pts", []),
        "season_tpm": existing.get("season_tpm", []),
    }

def main():
    print("=" * 50)
    print("  NBA RECORDS — Leaderboard Update")
    print("=" * 50)
    bl = load_baseline()
    games = fetch_2526()
    lb = rebuild(bl, games)
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUTPUT_FILE)
    with open(path, "w") as f:
        json.dump(lb, f, separators=(",",":"))
    print(f"Saved {OUTPUT_FILE} ({os.path.getsize(path)/1024:.0f} KB)")
    for n,v in list(lb["career"]["pts"])[:3]:
        pass
    print("\nTop 3 scorers:")
    for e in lb["career"]["pts"][:3]:
        print(f"  {e['name']}: {e['value']:,}")
    print("\nDone! ✓")

if __name__ == "__main__":
    main()
