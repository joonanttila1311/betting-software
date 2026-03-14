"""
app.py  –  MLB Vedonlyönti-UI  v7.1 (Pure wOBA & Engine v5.0 Integration)
========================================================================
Puhdistettu versio: Poistettu vanha L/R-kätisyyslaskenta.
Integroitu suoraan laskentamoottoriin v5.0 dynaamisilla wOBA-arvoilla.
"""

import sqlite3
import pandas as pd
import streamlit as st
import json
from pathlib import Path
from laskentamoottori import laske_todennakoisyys, DB_POLKU

st.set_page_config(page_title="MLB Pro Engine", page_icon="⚾", layout="wide")

MLB_JOUKKUEET = {
    "AZ": "Arizona Diamondbacks", "ARI": "Arizona Diamondbacks", 
    "ATL": "Atlanta Braves", "BAL": "Baltimore Orioles",
    "BOS": "Boston Red Sox", "CHC": "Chicago Cubs", "CWS": "Chicago White Sox",
    "CIN": "Cincinnati Reds", "CLE": "Cleveland Guardians", "COL": "Colorado Rockies",
    "DET": "Detroit Tigers", "HOU": "Houston Astros", "KC": "Kansas City Royals",
    "LAA": "Los Angeles Angels", "LAD": "Los Angeles Dodgers", "MIA": "Miami Marlins",
    "MIL": "Milwaukee Brewers", "MIN": "Minnesota Twins", "NYM": "New York Mets",
    "NYY": "New York Yankees", "OAK": "Oakland Athletics", "ATH": "Athletics", 
    "PHI": "Philadelphia Phillies", "PIT": "Pittsburgh Pirates", "SD": "San Diego Padres",
    "SF": "San Francisco Giants", "SEA": "Seattle Mariners", "STL": "St. Louis Cardinals",
    "TB": "Tampa Bay Rays", "TEX": "Texas Rangers", "TOR": "Toronto Blue Jays",
    "WSH": "Washington Nationals", "WAS": "Washington Nationals"
}

JSON_POLKU = "rosterit_2026.json"
LIIGA_WOBA_KA = 0.310

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Source+Serif+4:ital,wght@0,300;0,600;1,300&display=swap');
    html, body, [data-testid="stAppViewContainer"] { background-color: #0d0d0d; color: #e8e0d0; }
    h1, h2, h3 { font-family: 'Bebas Neue', sans-serif; }
    .main-title { font-family: 'Bebas Neue', sans-serif; font-size: 3.5rem; color: #f0e6c8; text-align: center; margin-bottom: 0; }
    .main-subtitle { font-family: 'Source Serif 4', serif; font-style: italic; color: #7a6e5f; text-align: center; margin-bottom: 2rem; }
    .divider { border-top: 1px solid #2e2a24; margin: 1.5rem 0; }
    .result-card { background: #141210; border: 1px solid #2e2a24; border-radius: 6px; padding: 1.5rem; text-align: center; }
    .result-team { font-family: 'Bebas Neue', sans-serif; font-size: 1.8rem; color: #e8e0d0; }
    .fip-badge { display: inline-block; background: #121e10; border: 1px solid #203a20; padding: 0.2rem 0.5rem; font-size: 0.8rem; color: #4bc84b; margin: 0.5rem 0; }
    .result-pct { font-family: 'Bebas Neue', sans-serif; font-size: 3.5rem; line-height: 1; }
    .result-odds { font-size: 1.5rem; color: #c8a84b; font-weight: bold; }
    .ou-card { background: #0e1208; border: 1px solid #2a3820; border-radius: 6px; padding: 1.5rem; text-align: center; margin-bottom: 1rem; }
    .ou-runs { font-family: 'Bebas Neue', sans-serif; font-size: 4rem; color: #7ec870; line-height: 1; }
    div.stButton > button { width: 100%; background-color: #c8a84b; color: #000; font-family: 'Bebas Neue', sans-serif; font-size: 1.5rem; }
    div.stButton > button:hover { background-color: #e0bf60; }
    .warn { color: #e8b84b; font-size: 0.85rem; margin-top: -10px; margin-bottom: 10px; }
    </style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-title">MLB PRO ENGINE</p><p class="main-subtitle">Time Decay xFIP · Dynamic wOBA Engine v5.0 · 2026</p>', unsafe_allow_html=True)

# ────────────────────────────────────────────────────────────────────────────
# DATAN LATAUS
# ────────────────────────────────────────────────────────────────────────────

@st.cache_data
def lataa_tiimit():
    conn = sqlite3.connect(DB_POLKU)
    df = pd.read_sql_query("SELECT DISTINCT Team FROM bullpen_statcast ORDER BY Team", conn)
    conn.close()
    tiimit_lista = [f"{MLB_JOUKKUEET.get(t, t)} ({t})" for t in df["Team"].tolist()]
    return sorted(tiimit_lista)

@st.cache_data
def lataa_syottajat():
    conn = sqlite3.connect(DB_POLKU)
    try:
        df = pd.read_sql_query("SELECT Name, Team, xFIP_All, xFIP_vs_L, xFIP_vs_R, IP_per_Start, p_throws FROM syottajat_statcast ORDER BY Name", conn)
    except:
        df = pd.read_sql_query("SELECT Name, Team, xFIP_All, xFIP_vs_L, xFIP_vs_R, IP_per_Start FROM syottajat_statcast ORDER BY Name", conn)
        df['p_throws'] = 'R'
    conn.close()
    
    optiot = {}
    for _, r in df.iterrows():
        katisyys = r.get('p_throws', 'R')
        if pd.isna(katisyys): katisyys = 'R'
            
        avain = f"{r['Name']} ({r['Team']}) | {katisyys}HP | xFIP: {r['xFIP_All']:.2f}"
        
        optiot[avain] = {
            "xFIP_All": r["xFIP_All"],
            "vs_L": r["xFIP_vs_L"], 
            "vs_R": r["xFIP_vs_R"], 
            "IP": r["IP_per_Start"], 
            "Name": r["Name"],
            "Katisyys": katisyys
        }
    return optiot

@st.cache_data
def lataa_bullpenit():
    conn = sqlite3.connect(DB_POLKU)
    # LISÄTTY: Haetaan myös Bullpen_xFIP_All
    df = pd.read_sql_query("SELECT Team, Bullpen_xFIP_All, Bullpen_xFIP_vs_L, Bullpen_xFIP_vs_R FROM bullpen_statcast", conn)
    conn.close()
    df = df.set_index("Team")
    # LISÄTTY: Tallennetaan "All" sanakirjaan
    return {team: {"All": row["Bullpen_xFIP_All"], "vs_L": row["Bullpen_xFIP_vs_L"], "vs_R": row["Bullpen_xFIP_vs_R"]} for team, row in df.iterrows()}

@st.cache_data
def lataa_rosterit():
    if not Path(JSON_POLKU).exists(): return {}
    with open(JSON_POLKU, encoding="utf-8") as f:
        return json.load(f)

@st.cache_data
def lataa_lyojat():
    try:
        conn = sqlite3.connect(DB_POLKU)
        df = pd.read_sql_query("SELECT Batter_ID, wOBA_All, wOBA_vs_L, wOBA_vs_R FROM lyojat_statcast", conn)
        conn.close()
        df["Batter_ID"] = df["Batter_ID"].astype(int)
        return df.set_index("Batter_ID")
    except Exception:
        return pd.DataFrame()

# Alustus
tiimit = lataa_tiimit()
optiot_syottajat = lataa_syottajat()
bp_dict = lataa_bullpenit()
rosterit = lataa_rosterit()
df_lyojat = lataa_lyojat()

if not tiimit or not optiot_syottajat or not rosterit: 
    st.error("Dataa puuttuu! Varmista, että tietokanta ja JSON-tiedosto ovat olemassa.")
    st.stop()

# ────────────────────────────────────────────────────────────────────────────
# APUFUNKTIOT
# ────────────────────────────────────────────────────────────────────────────

def pura_joukkue(valinta):
    osat = valinta.split(" (")
    return osat[0], osat[1].replace(")", "") if len(osat) > 1 else osat[0]

def laske_joukkueen_woba(yh_nimet: list, pe_nimet: list, joukkue_lyh: str, vastus_sp_kasisyys: str) -> float:
    split = f"wOBA_vs_{vastus_sp_kasisyys}"
    joukkue_roster = rosterit.get(joukkue_lyh, [])
    nimi_id = {p["name"]: p["id"] for p in joukkue_roster}

    def hae_arvot(nimet):
        lst = []
        for n in nimet:
            p_clean = n.split(" (")[0] # "Judge, Aaron (NYY)" -> "Judge, Aaron"
            pid = nimi_id.get(p_clean)
            if pid and not df_lyojat.empty and pid in df_lyojat.index:
                v = df_lyojat.loc[pid].get(split, df_lyojat.loc[pid].get("wOBA_All"))
                lst.append(float(v) if pd.notna(v) else LIIGA_WOBA_KA)
            else:
                lst.append(LIIGA_WOBA_KA)
        return lst

    yh_lst = hae_arvot(yh_nimet)
    pe_lst = hae_arvot(pe_nimet)
    yh_ka = sum(yh_lst) / len(yh_lst) if yh_lst else LIIGA_WOBA_KA
    pe_ka = sum(pe_lst) / len(pe_lst) if pe_lst else LIIGA_WOBA_KA
    
    return round((yh_ka * 0.90) + (pe_ka * 0.10), 3)

def filtteroi_syottajat(joukkue_lyh):
    omat = [k for k in optiot_syottajat.keys() if f"({joukkue_lyh})" in k]
    return omat if omat else ["(Ei syöttäjiä)"]

# ────────────────────────────────────────────────────────────────────────────
# KÄYTTÖLIITTYMÄ (UI)
# ────────────────────────────────────────────────────────────────────────────

c1, c2, c3 = st.columns([10, 1, 10])

with c1:
    st.markdown("### 🏠 KOTIJOUKKUE")
    koti_valinta = st.selectbox("Joukkue", tiimit, index=0, key="k_team")
    koti_koko, koti_lyh = pura_joukkue(koti_valinta)
    koti_sp_nimi = st.selectbox("Aloitussyöttäjä", filtteroi_syottajat(koti_lyh), key="k_sp")
    
    st.markdown("<br><b>Kotijoukkueen Lyöjät:</b>", unsafe_allow_html=True)
    koti_roster = [f"{p['name']} ({koti_lyh})" for p in rosterit.get(koti_lyh, [])]
    koti_yh = st.multiselect("Aloittava Yhdeksikkö (9)", koti_roster, default=koti_roster[:min(9, len(koti_roster))], key="k_yh")
    koti_pe_opt = [n for n in koti_roster if n not in koti_yh]
    koti_pe = st.multiselect("Penkki (10% paino)", koti_pe_opt, default=koti_pe_opt[:min(4, len(koti_pe_opt))], key="k_pe")

with c3:
    st.markdown("### ✈️ VIERASJOUKKUE")
    vieras_valinta = st.selectbox("Joukkue", tiimit, index=1 if len(tiimit)>1 else 0, key="v_team")
    vieras_koko, vieras_lyh = pura_joukkue(vieras_valinta)
    vieras_sp_nimi = st.selectbox("Aloitussyöttäjä", filtteroi_syottajat(vieras_lyh), key="v_sp")
    
    st.markdown("<br><b>Vierasjoukkueen Lyöjät:</b>", unsafe_allow_html=True)
    vieras_roster = [f"{p['name']} ({vieras_lyh})" for p in rosterit.get(vieras_lyh, [])]
    vieras_yh = st.multiselect("Aloittava Yhdeksikkö (9)", vieras_roster, default=vieras_roster[:min(9, len(vieras_roster))], key="v_yh")
    vieras_pe_opt = [n for n in vieras_roster if n not in vieras_yh]
    vieras_pe = st.multiselect("Penkki (10% paino)", vieras_pe_opt, default=vieras_pe_opt[:min(4, len(vieras_pe_opt))], key="v_pe")

# ────────────────────────────────────────────────────────────────────────────
# LASKENTA
# ────────────────────────────────────────────────────────────────────────────

if st.button("⚡ LASKE TODENNÄKÖISYYS"):
    koti_sp_data = optiot_syottajat[koti_sp_nimi]
    vieras_sp_data = optiot_syottajat[vieras_sp_nimi]
    
    koti_bp_data = bp_dict.get(koti_lyh, {"All": 3.80, "vs_L": 3.80, "vs_R": 3.80})
    vieras_bp_data = bp_dict.get(vieras_lyh, {"All": 3.80, "vs_L": 3.80, "vs_R": 3.80})
    
    koti_sp_arm = koti_sp_data.get("Katisyys", "R")
    vieras_sp_arm = vieras_sp_data.get("Katisyys", "R")

    # Dynaaminen wOBA vastustajan SP:n mukaan
    koti_woba = laske_joukkueen_woba(koti_yh, koti_pe, koti_lyh, vieras_sp_arm)
    vieras_woba = laske_joukkueen_woba(vieras_yh, vieras_pe, vieras_lyh, koti_sp_arm)

    # MOOTTORIKUTSU v5.0
    tulos = laske_todennakoisyys(
        koti_koko, vieras_koko,
        koti_sp=koti_sp_data, 
        koti_bp=koti_bp_data, 
        koti_woba=koti_woba,
        vieras_sp=vieras_sp_data, 
        vieras_bp=vieras_bp_data, 
        vieras_woba=vieras_woba
    )

    k_pct, v_pct = tulos["koti_voitto_tod"] * 100, tulos["vieras_voitto_tod"] * 100
    k_odds, v_odds = 1/tulos["koti_voitto_tod"], 1/tulos["vieras_voitto_tod"]

    # Tulosten näyttäminen
    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    col1, col2, col3 = st.columns([10, 1, 10])
    
    with col1:
        st.markdown(f"""<div class="result-card"><div class="result-team">{koti_koko}</div>
        <div class="fip-badge">{koti_sp_data['Name']}</div><br>
        <span style="color:#7a6e5f;font-size:0.85rem">
        Hyökkäyksen wOBA: <b>{koti_woba:.3f}</b><br>
        SP xFIP: {tulos['koti_sp_dyn']:.2f} | BP xFIP: {tulos['koti_bp_dyn']:.2f}<br>
        <b>Yhdistetty xFIP: {tulos['koti_total_xfip']:.2f}</b></span>
        <b>Momentum-etu: {tulos['momentum_edge'] * 100:.2f} %</b></span>
        <div class="result-pct" style="color:{'#c8a84b' if k_pct>=v_pct else '#e8e0d0'}">{k_pct:.1f}%</div>
        <div class="result-odds">{k_odds:.2f}</div></div>""", unsafe_allow_html=True)
        
    with col3:
        st.markdown(f"""<div class="result-card"><div class="result-team">{vieras_koko}</div>
        <div class="fip-badge">{vieras_sp_data['Name']}</div><br>
        <span style="color:#7a6e5f;font-size:0.85rem">
        Hyökkäyksen wOBA: <b>{vieras_woba:.3f}</b><br>
        SP xFIP: {tulos['vieras_sp_dyn']:.2f} | BP xFIP: {tulos['vieras_bp_dyn']:.2f}<br>
        <b>Yhdistetty xFIP: {tulos['vieras_total_xfip']:.2f}</b></span>
        <b>Momentum-etu: {-tulos['momentum_edge'] * 100:.2f} %</b></span>
        <div class="result-pct" style="color:{'#c8a84b' if v_pct>k_pct else '#e8e0d0'}">{v_pct:.1f}%</div>
        <div class="result-odds">{v_odds:.2f}</div></div>""", unsafe_allow_html=True)

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown(f'<div class="ou-card"><div style="color:#5a7a50;letter-spacing:0.2em;">JUOKSUODOTTAMA (O/U)</div><div class="ou-runs">{tulos["total_odotus"]:.1f}</div><div>{koti_lyh}: {tulos["k_odotus"]:.1f} &nbsp;|&nbsp; {vieras_lyh}: {tulos["v_odotus"]:.1f}</div></div>', unsafe_allow_html=True)
