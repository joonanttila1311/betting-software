"""
app.py  –  MLB Vedonlyönti-UI  v10.0 (API Lyöjä-automaatio & Warning Fix)
====================================
"""

import csv
import json
import sqlite3
import unicodedata
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytz
import statsapi
import streamlit as st

from laskentamoottori import laske_todennakoisyys, DB_POLKU, STADION_DATA

# ────────────────────────────────────────────────────────────────────────────
# SIVU-ASETUKSET
# ────────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="MLB Pro Engine", page_icon="⚾", layout="wide")

# ────────────────────────────────────────────────────────────────────────────
# VAKIOT
# ────────────────────────────────────────────────────────────────────────────

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
    "WSH": "Washington Nationals", "WAS": "Washington Nationals",
}

JSON_POLKU    = "rosterit_2026.json"
CSV_POLKU     = "tallennetut_pelit.csv"
LIIGA_WOBA_KA = 0.310

CSV_SARAKKEET = [
    "Pvm", "Koti", "Vieras",
    "Koti %", "Vieras %",
    "Koti kerroin", "Vieras kerroin",
    "Koti SP xFIP", "Koti BP xFIP", "Koti wOBA", "Koti ISO", "Koti SP K-BB%",
    "Koti SP IP", "Koti SP IP/GS",
    "Vieras SP xFIP", "Vieras BP xFIP", "Vieras wOBA", "Vieras ISO", "Vieras SP K-BB%",
    "Vieras SP IP", "Vieras SP IP/GS",
    "O/U odotus", "Koti odotus", "Vieras odotus",
    "Koti tulos", "Vieras tulos",
]

TUULI_OPTIOT = [
    "Sivutuuli / Tyyni",
    "Ulos katsomoon",
    "Sisään pesälle",
]

# ────────────────────────────────────────────────────────────────────────────
# CSS
# ────────────────────────────────────────────────────────────────────────────

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
.detail-card { background: #0f0f12; border: 1px solid #2a2830; border-radius: 6px; padding: 1.2rem 1.4rem; margin-top: 0.5rem; }
.detail-title { font-family: 'Bebas Neue', sans-serif; font-size: 1.1rem; letter-spacing: 0.1em; color: #c8a84b; margin-bottom: 0.7rem; border-bottom: 1px solid #2a2830; padding-bottom: 0.4rem; }
.detail-row { display: flex; justify-content: space-between; padding: 0.28rem 0; border-bottom: 1px solid #1e1c22; font-size: 0.88rem; }
.detail-row:last-child { border-bottom: none; }
.detail-key { color: #7a6e5f; }
.detail-val { color: #e8e0d0; font-weight: 500; }
.detail-val.green { color: #4bc84b; }
.detail-val.gold  { color: #c8a84b; }
.weather-box { background: #0d1210; border: 1px solid #1e2e20; border-radius: 6px; padding: 1rem 1.4rem; margin-bottom: 1rem; }
.weather-title { font-family: 'Bebas Neue', sans-serif; font-size: 1rem; letter-spacing: 0.12em; color: #5a7a50; margin-bottom: 0.6rem; }
.stadion-info { font-size: 0.88rem; color: #7a6e5f; margin-bottom: 0.7rem; line-height: 1.6; }
.stadion-info b { color: #a0c890; }
.dome-badge { display: inline-block; background: #101828; border: 1px solid #1e3050; padding: 0.1rem 0.5rem; font-size: 0.78rem; color: #5090d0; border-radius: 3px; margin-left: 0.4rem; }
.outdoor-badge { display: inline-block; background: #0e1a10; border: 1px solid #1e3820; padding: 0.1rem 0.5rem; font-size: 0.78rem; color: #60a860; border-radius: 3px; margin-left: 0.4rem; }
.save-btn > div > button { background-color: #1a3a1a !important; color: #7ec870 !important; border: 1px solid #2a5a2a !important; font-size: 1.1rem !important; }
.save-btn > div > button:hover { background-color: #224a22 !important; }
</style>
""", unsafe_allow_html=True)

# ────────────────────────────────────────────────────────────────────────────
# OTSIKKO
# ────────────────────────────────────────────────────────────────────────────

st.markdown(
    '<p class="main-title">MLB PRO ENGINE</p>'
    '<p class="main-subtitle">Time Decay xFIP · Dynamic wOBA Engine v5.0 · 2026</p>',
    unsafe_allow_html=True,
)

# ────────────────────────────────────────────────────────────────────────────
# DATAN LATAUS JA PUHDISTUS
# ────────────────────────────────────────────────────────────────────────────

def poista_aksentit(teksti: str) -> str:
    """Poistaa erikoismerkit ja aksentit (esim. García -> Garcia)."""
    if not teksti: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', teksti) if unicodedata.category(c) != 'Mn')

def pura_joukkue(valinta: str) -> tuple[str, str]:
    osat = valinta.split(" (")
    return osat[0], osat[1].replace(")", "") if len(osat) > 1 else osat[0]

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
        df = pd.read_sql_query(
            "SELECT Name, Team, xFIP_All, xFIP_vs_L, xFIP_vs_R, IP, IP_per_Start, p_throws, K_BB_pct FROM syottajat_statcast ORDER BY Name", conn
        )
    except Exception:
        df = pd.read_sql_query(
            "SELECT Name, Team, xFIP_All, xFIP_vs_L, xFIP_vs_R, IP, IP_per_Start FROM syottajat_statcast ORDER BY Name", conn
        )
        df["p_throws"] = "R"
        df["K_BB_pct"] = 0.150
    conn.close()

    optiot = {}
    for _, r in df.iterrows():
        katisyys = r.get("p_throws", "R")
        if pd.isna(katisyys): katisyys = "R"
        avain = f"{r['Name']} | {katisyys}HP | xFIP: {r['xFIP_All']:.2f}"
        optiot[avain] = {
            "xFIP_All": r["xFIP_All"], "vs_L": r["xFIP_vs_L"], "vs_R": r["xFIP_vs_R"],
            "IP": r["IP_per_Start"], "IP_total": r.get("IP", 0.0),  
            "Name": r["Name"], "Katisyys": katisyys, "K_BB_pct": r.get("K_BB_pct", 0.150)
        }
    return optiot

@st.cache_data
def lataa_bullpenit():
    conn = sqlite3.connect(DB_POLKU)
    try:
        df = pd.read_sql_query("SELECT Team, Bullpen_xFIP_All, Bullpen_xFIP_vs_L, Bullpen_xFIP_vs_R, Bullpen_K_BB_pct FROM bullpen_statcast", conn)
    except Exception:
        df = pd.read_sql_query("SELECT Team, Bullpen_xFIP_All, Bullpen_xFIP_vs_L, Bullpen_xFIP_vs_R FROM bullpen_statcast", conn)
        df["Bullpen_K_BB_pct"] = 0.150
    conn.close()
    df = df.set_index("Team")
    return {team: {"All": row["Bullpen_xFIP_All"], "vs_L": row["Bullpen_xFIP_vs_L"], "vs_R": row["Bullpen_xFIP_vs_R"], "K_BB_pct": row.get("Bullpen_K_BB_pct", 0.150)} for team, row in df.iterrows()}

@st.cache_data
def lataa_rosterit():
    if not Path(JSON_POLKU).exists(): return {}
    with open(JSON_POLKU, encoding="utf-8") as f: return json.load(f)

@st.cache_data
def lataa_lyojat():
    try:
        conn = sqlite3.connect(DB_POLKU)
        df = pd.read_sql_query("SELECT Batter_ID, wOBA_All, wOBA_vs_L, wOBA_vs_R, ISO FROM lyojat_statcast", conn)
        conn.close()
        df["Batter_ID"] = df["Batter_ID"].astype(int)
        return df.set_index("Batter_ID")
    except Exception:
        return pd.DataFrame()

@st.cache_data
def hae_mlb_paiva(valittu_pvm):
    paiva_str = valittu_pvm.strftime("%Y-%m-%d")
    try:
        schedule = statsapi.schedule(date=paiva_str)
        pelit = []
        for peli in schedule:
            pelin_nimi = f"{peli['away_name']} @ {peli['home_name']} ({peli['game_id']})"
            pelit.append({
                "label": pelin_nimi, "game_pk": peli['game_id'],
                "home_team": peli['home_name'], "away_team": peli['away_name']
            })
        return pelit
    except Exception as e:
        return []

# Alustus
tiimit           = lataa_tiimit()
optiot_syottajat = lataa_syottajat()
bp_dict          = lataa_bullpenit()
rosterit         = lataa_rosterit()
df_lyojat        = lataa_lyojat()

if not tiimit or not optiot_syottajat or not rosterit:
    st.error("Dataa puuttuu! Varmista, että tietokanta ja JSON-tiedosto ovat olemassa.")
    st.stop()

kaikki_lyojat_id = {p["name"]: p["id"] for pelaajat in rosterit.values() for p in pelaajat}
kaikki_lyojat_nimet = sorted(list(kaikki_lyojat_id.keys()))


# ────────────────────────────────────────────────────────────────────────────
# ÄLYKÄS TUONTIPANEELI (MLB Stats API)
# ────────────────────────────────────────────────────────────────────────────
st.markdown('<div style="background-color:#1a1a1e; padding:15px; border-radius:10px; border:1px solid #c8a84b22; margin-bottom:20px;">', unsafe_allow_html=True)
st.markdown("<h3 style='margin-top:0; color:#c8a84b; font-size:1.2rem;'>🚀 MLB AUTOMAATTINEN TUONTI</h3>", unsafe_allow_html=True)

col_date, col_peli, col_nappi = st.columns([1, 2, 1])

us_it_aika = datetime.now(pytz.timezone('US/Eastern'))
with col_date: valittu_pvm = st.date_input("Valitse päivä (USA)", value=us_it_aika.date())

with col_peli:
    paivan_pelit = hae_mlb_paiva(valittu_pvm)
    peli_optiot = [p['label'] for p in paivan_pelit]
    valittu_peli_label = st.selectbox("Valitse ottelu", ["--- Valitse peli listalta ---"] + peli_optiot)

with col_nappi:
    st.write(" ") 
    if st.button("📥 TUO TIEDOT"):
        if valittu_peli_label != "--- Valitse peli listalta ---":
            peli_data = next(p for p in paivan_pelit if p['label'] == valittu_peli_label)
            game_pk = peli_data['game_pk']
            
            k_lyh_api = ""
            v_lyh_api = ""
            
            # 1. Joukkueiden asetus
            for t_label in tiimit:
                if peli_data['home_team'] in t_label: 
                    st.session_state['k_team'] = t_label
                    k_lyh_api = pura_joukkue(t_label)[1]
                if peli_data['away_team'] in t_label: 
                    st.session_state['v_team'] = t_label
                    v_lyh_api = pura_joukkue(t_label)[1]
            
            # 2. Syöttäjien & Kokoonpanojen asetus (Fuzzy Match & Unicodedata)
            try:
                peli_full = statsapi.get('game', {'gamePk': game_pk})
                
                # Syöttäjät
                k_sp_nimi = peli_full['gameData']['probablePitchers'].get('home', {}).get('fullName', "")
                v_sp_nimi = peli_full['gameData']['probablePitchers'].get('away', {}).get('fullName', "")
                
                def etsi_pelaaja_alypyylilla(api_nimi, optiot_avaimet):
                    if not api_nimi: return None
                    puhdas_api = poista_aksentit(api_nimi).lower().replace(",", "").replace(".", "").replace(" jr", "")
                    osat = puhdas_api.split()
                    for avain in optiot_avaimet:
                        puhdas_avain = poista_aksentit(avain).lower().replace(",", "").replace(".", "")
                        if all(osa in puhdas_avain for osa in osat):
                            return avain
                    return None

                k_match = etsi_pelaaja_alypyylilla(k_sp_nimi, optiot_syottajat.keys())
                if k_match: st.session_state['k_sp'] = k_match

                v_match = etsi_pelaaja_alypyylilla(v_sp_nimi, optiot_syottajat.keys())
                if v_match: st.session_state['v_sp'] = v_match
                
                # Kokoonpanot (Batters)
                live_data = peli_full.get('liveData', {}).get('boxscore', {}).get('teams', {})
                home_box = live_data.get('home', {})
                away_box = live_data.get('away', {})
                
                def get_lineup_names(box_team):
                    batting_order = box_team.get('battingOrder', [])
                    names = []
                    players_dict = box_team.get('players', {})
                    for pid in batting_order:
                        player_key = f"ID{pid}"
                        if player_key in players_dict:
                            names.append(players_dict[player_key]['person']['fullName'])
                    return names

                home_names = get_lineup_names(home_box)
                away_names = get_lineup_names(away_box)
                
                # Aseta Koti Lyöjät
                if len(home_names) > 0 and k_lyh_api:
                    k_yh_key = f"k_yh_{k_lyh_api}"
                    k_pe_key = f"k_pe_{k_lyh_api}"
                    
                    k_api_yh = [etsi_pelaaja_alypyylilla(n, kaikki_lyojat_nimet) for n in home_names]
                    k_api_yh = [n for n in k_api_yh if n is not None][:9]
                    st.session_state[k_yh_key] = k_api_yh
                    
                    k_def_names = [p['name'] for p in rosterit.get(k_lyh_api, [])]
                    k_api_pe = [n for n in k_def_names if n not in k_api_yh][:4]
                    st.session_state[k_pe_key] = k_api_pe
                    
                # Aseta Vieras Lyöjät
                if len(away_names) > 0 and v_lyh_api:
                    v_yh_key = f"v_yh_{v_lyh_api}"
                    v_pe_key = f"v_pe_{v_lyh_api}"
                    
                    v_api_yh = [etsi_pelaaja_alypyylilla(n, kaikki_lyojat_nimet) for n in away_names]
                    v_api_yh = [n for n in v_api_yh if n is not None][:9]
                    st.session_state[v_yh_key] = v_api_yh
                    
                    v_def_names = [p['name'] for p in rosterit.get(v_lyh_api, [])]
                    v_api_pe = [n for n in v_def_names if n not in v_api_yh][:4]
                    st.session_state[v_pe_key] = v_api_pe
                    
            except: pass

            st.success(f"Tiedot haettu! Voit nyt tarkistaa ja muuttaa niitä alla.")
        else:
            st.warning("Valitse ensin peli listalta.")
st.markdown('</div>', unsafe_allow_html=True)


# ────────────────────────────────────────────────────────────────────────────
# APUFUNKTIOT JA CALLBACKIT
# ────────────────────────────────────────────────────────────────────────────

def laske_joukkueen_woba(yh_nimet: list, pe_nimet: list, vastus_sp_katisyys: str) -> tuple[float, float]:
    split = "wOBA_All" if vastus_sp_katisyys == "All" else f"wOBA_vs_{vastus_sp_katisyys}"
    LIIGA_ISO_KA = 0.150

    def hae_arvot(nimet):
        woba_lst, iso_lst = [], []
        for puhtaanimi in nimet:
            pid = kaikki_lyojat_id.get(puhtaanimi)
            if pid and not df_lyojat.empty and pid in df_lyojat.index:
                w_v = df_lyojat.loc[pid].get(split, df_lyojat.loc[pid].get("wOBA_All"))
                i_v = df_lyojat.loc[pid].get("ISO", LIIGA_ISO_KA)
                woba_lst.append(float(w_v) if pd.notna(w_v) else LIIGA_WOBA_KA)
                iso_lst.append(float(i_v) if pd.notna(i_v) else LIIGA_ISO_KA)
            else:
                woba_lst.append(LIIGA_WOBA_KA)
                iso_lst.append(LIIGA_ISO_KA)
        return woba_lst, iso_lst

    yh_woba, yh_iso = hae_arvot(yh_nimet)
    pe_woba, pe_iso = hae_arvot(pe_nimet)
    
    puuttuvat_yh = max(0, 9 - len(yh_woba))
    yh_woba.extend([LIIGA_WOBA_KA] * puuttuvat_yh)
    yh_iso.extend([LIIGA_ISO_KA] * puuttuvat_yh)
    yh_w_ka = sum(yh_woba) / len(yh_woba) if yh_woba else LIIGA_WOBA_KA
    yh_i_ka = sum(yh_iso) / len(yh_iso) if yh_iso else LIIGA_ISO_KA
    
    puuttuvat_pe = max(0, 4 - len(pe_woba))
    pe_woba.extend([LIIGA_WOBA_KA] * puuttuvat_pe)
    pe_iso.extend([LIIGA_ISO_KA] * puuttuvat_pe)
    pe_w_ka = sum(pe_woba) / len(pe_woba) if pe_woba else LIIGA_WOBA_KA
    pe_i_ka = sum(pe_iso) / len(pe_iso) if pe_iso else LIIGA_ISO_KA
    
    lopullinen_woba = round((yh_w_ka * 0.90) + (pe_w_ka * 0.10), 3)
    lopullinen_iso = round((yh_i_ka * 0.90) + (pe_i_ka * 0.10), 3)
    
    return lopullinen_woba, lopullinen_iso

def hae_kaikki_syottajat() -> list[str]:
    kaikki = list(optiot_syottajat.keys())
    return kaikki if kaikki else ["(Ei syöttäjiä)"]

def est_select_all(l_key):
    prev_key = l_key + "_prev"
    current = st.session_state[l_key]
    prev = st.session_state.get(prev_key, current)
    uusien_maara = len(set(current) - set(prev))
    if uusien_maara > 1:
        st.session_state[l_key] = prev
        st.toast("🛡️ 'Select All' -painallus estetty! Aiemmat valintasi on turvattu.", icon="🚫")
    else:
        st.session_state[prev_key] = current

def varmista_csv():
    if not Path(CSV_POLKU).exists():
        with open(CSV_POLKU, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_SARAKKEET)
            writer.writeheader()

def tallenna_peli(rivi: dict):
    varmista_csv()
    with open(CSV_POLKU, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_SARAKKEET)
        writer.writerow(rivi)

def lataa_seuranta_df() -> pd.DataFrame:
    varmista_csv()
    df = pd.read_csv(CSV_POLKU, dtype=str)
    for col in ("Koti tulos", "Vieras tulos"):
        if col not in df.columns: df[col] = ""
    for col in CSV_SARAKKEET:
        if col not in df.columns: df[col] = ""
    return df[CSV_SARAKKEET]

def tallenna_seuranta_df(df: pd.DataFrame):
    df.to_csv(CSV_POLKU, index=False, encoding="utf-8")

# ────────────────────────────────────────────────────────────────────────────
# VÄLILEHDET
# ────────────────────────────────────────────────────────────────────────────

tab_analyysi, tab_seuranta = st.tabs(["⚾ Uusi Analyysi", "📂 Seuranta"])

with tab_analyysi:

    # Asetetaan Oletusjoukkueet Session Stateen (Poistaa keltaisen varoituksen!)
    if "k_team" not in st.session_state:
        st.session_state["k_team"] = tiimit[0]
    if "v_team" not in st.session_state:
        st.session_state["v_team"] = tiimit[1] if len(tiimit) > 1 else tiimit[0]

    c1, c2, c3 = st.columns([10, 1, 10])

    with c1:
        st.markdown("### 🏠 KOTIJOUKKUE")
        koti_valinta  = st.selectbox("Joukkue", tiimit, key="k_team")
        koti_koko, koti_lyh = pura_joukkue(koti_valinta)
        koti_sp_nimi  = st.selectbox("Aloitussyöttäjä", hae_kaikki_syottajat(), key="k_sp")

        if koti_sp_nimi in optiot_syottajat:
            sp_data = optiot_syottajat[koti_sp_nimi]
            st.markdown(
                f"""<div style='background-color:#141210; border:1px solid #2e2a24; border-radius:5px; padding:8px; margin-bottom:10px; display:flex; justify-content:space-around; text-align:center; font-size:0.85rem;'>
                    <div><span style='color:#7a6e5f;'>xFIP All</span><br><b style='color:#e8e0d0;'>{sp_data['xFIP_All']:.2f}</b></div>
                    <div><span style='color:#7a6e5f;'>K-BB%</span><br><b style='color:#4bc84b;'>{sp_data.get('K_BB_pct', 0.150)*100:.1f}%</b></div>
                    <div><span style='color:#7a6e5f;'>vs L</span><br><b style='color:#e8e0d0;'>{sp_data['vs_L']:.2f}</b></div>
                    <div><span style='color:#7a6e5f;'>vs R</span><br><b style='color:#e8e0d0;'>{sp_data['vs_R']:.2f}</b></div>
                    <div><span style='color:#7a6e5f;'>Kokonais-IP</span><br><b style='color:#c8a84b;'>{sp_data['IP_total']:.1f}</b></div>
                </div>""", unsafe_allow_html=True
            )
            if sp_data["IP_total"] < 30.0:
                st.markdown(
                    f"<div style='background-color:#2a1010; padding:10px; border-radius:5px; border:1px solid #c84b4b; font-size:0.85rem; color:#e87070; margin-bottom:10px; line-height:1.4;'>"
                    f"⚠️ <b>VAROITUS (Pieni otanta):</b> Syöttäjällä on tietokannassa vain <b>{sp_data['IP_total']:.1f}</b> heitettyä vuoroparia. "
                    f"Tämä xFIP-lukema ei ole vakautunut ja voi olla pelkkä suonenveto. Ammattilaiset välttävät näitä kohteita (No Bet).</div>", 
                    unsafe_allow_html=True
                )
            if sp_data["IP"] < 3.0:
                st.markdown(
                    f"<div style='background-color:#2a2410; padding:10px; border-radius:5px; border:1px solid #c8a84b; font-size:0.85rem; color:#e8b84b; margin-bottom:10px; line-height:1.4;'>"
                    f"⚠️ <b>VAROITUS (Opener):</b> Syöttäjän keskimääräinen IP/GS on vain <b>{sp_data['IP']:.2f}</b>. "
                    f"Jos pelissä on nimetty Bulk Pitcher (pääsyöttäjä), vaihda hänet. Puhdasta bullpen-peliä varten voit ohittaa tämän.</div>", 
                    unsafe_allow_html=True
                )

        st.markdown("<br><b>Kotijoukkueen Lyöjät:</b>", unsafe_allow_html=True)
        
        # LYÖJÄT: KOTI
        koti_oletus_nimet = [p['name'] for p in rosterit.get(koti_lyh, [])]
        koti_yh_key = f"k_yh_{koti_lyh}"
        
        # Jos automaatio ei ole vielä asettanut arvoa muistiin, laitetaan oletus
        if koti_yh_key not in st.session_state:
            st.session_state[koti_yh_key] = koti_oletus_nimet[: min(9, len(koti_oletus_nimet))]
            
        koti_yh = st.multiselect("Aloittava Yhdeksikkö (9)", kaikki_lyojat_nimet, max_selections=9, key=koti_yh_key, on_change=est_select_all, args=(koti_yh_key,))
        
        koti_pe_opt = [n for n in kaikki_lyojat_nimet if n not in koti_yh]
        koti_pe_key = f"k_pe_{koti_lyh}"
        
        if koti_pe_key not in st.session_state:
            st.session_state[koti_pe_key] = [n for n in koti_oletus_nimet if n not in koti_yh][: min(4, max(0, len(koti_oletus_nimet)-len(koti_yh)))]
            
        koti_pe = st.multiselect("Penkki (10% paino)", koti_pe_opt, max_selections=5, key=koti_pe_key, on_change=est_select_all, args=(koti_pe_key,))

    with c3:
        st.markdown("### ✈️ VIERASJOUKKUE")
        vieras_valinta = st.selectbox("Joukkue", tiimit, key="v_team")
        vieras_koko, vieras_lyh = pura_joukkue(vieras_valinta)
        vieras_sp_nimi = st.selectbox("Aloitussyöttäjä", hae_kaikki_syottajat(), key="v_sp")

        if vieras_sp_nimi in optiot_syottajat:
            sp_data = optiot_syottajat[vieras_sp_nimi]
            st.markdown(
                f"""<div style='background-color:#141210; border:1px solid #2e2a24; border-radius:5px; padding:8px; margin-bottom:10px; display:flex; justify-content:space-around; text-align:center; font-size:0.85rem;'>
                    <div><span style='color:#7a6e5f;'>xFIP All</span><br><b style='color:#e8e0d0;'>{sp_data['xFIP_All']:.2f}</b></div>
                    <div><span style='color:#7a6e5f;'>K-BB%</span><br><b style='color:#4bc84b;'>{sp_data.get('K_BB_pct', 0.150)*100:.1f}%</b></div>
                    <div><span style='color:#7a6e5f;'>vs L</span><br><b style='color:#e8e0d0;'>{sp_data['vs_L']:.2f}</b></div>
                    <div><span style='color:#7a6e5f;'>vs R</span><br><b style='color:#e8e0d0;'>{sp_data['vs_R']:.2f}</b></div>
                    <div><span style='color:#7a6e5f;'>Kokonais-IP</span><br><b style='color:#c8a84b;'>{sp_data['IP_total']:.1f}</b></div>
                </div>""", unsafe_allow_html=True
            )
            if sp_data["IP_total"] < 30.0:
                st.markdown(
                    f"<div style='background-color:#2a1010; padding:10px; border-radius:5px; border:1px solid #c84b4b; font-size:0.85rem; color:#e87070; margin-bottom:10px; line-height:1.4;'>"
                    f"⚠️ <b>VAROITUS (Pieni otanta):</b> Syöttäjällä on tietokannassa vain <b>{sp_data['IP_total']:.1f}</b> heitettyä vuoroparia. "
                    f"Tämä xFIP-lukema ei ole vakautunut ja voi olla pelkkä suonenveto. Ammattilaiset välttävät näitä kohteita (No Bet).</div>", 
                    unsafe_allow_html=True
                )
            elif sp_data["IP"] < 3.0:
                st.markdown(
                    f"<div style='background-color:#2a2410; padding:10px; border-radius:5px; border:1px solid #c8a84b; font-size:0.85rem; color:#e8b84b; margin-bottom:10px; line-height:1.4;'>"
                    f"⚠️ <b>VAROITUS (Opener):</b> Syöttäjän keskimääräinen IP/GS on vain <b>{sp_data['IP']:.2f}</b>. "
                    f"Jos pelissä on nimetty Bulk Pitcher (pääsyöttäjä), vaihda hänet. Puhdasta bullpen-peliä varten voit ohittaa tämän.</div>", 
                    unsafe_allow_html=True
                )

        st.markdown("<br><b>Vierasjoukkueen Lyöjät:</b>", unsafe_allow_html=True)
        
        # LYÖJÄT: VIERAS
        vieras_oletus_nimet = [p['name'] for p in rosterit.get(vieras_lyh, [])]
        vieras_yh_key = f"v_yh_{vieras_lyh}"
        
        if vieras_yh_key not in st.session_state:
            st.session_state[vieras_yh_key] = vieras_oletus_nimet[: min(9, len(vieras_oletus_nimet))]
            
        vieras_yh = st.multiselect("Aloittava Yhdeksikkö (9)", kaikki_lyojat_nimet, max_selections=9, key=vieras_yh_key, on_change=est_select_all, args=(vieras_yh_key,))
        
        vieras_pe_opt = [n for n in kaikki_lyojat_nimet if n not in vieras_yh]
        vieras_pe_key = f"v_pe_{vieras_lyh}"
        
        if vieras_pe_key not in st.session_state:
            st.session_state[vieras_pe_key] = [n for n in vieras_oletus_nimet if n not in vieras_yh][: min(4, max(0, len(vieras_oletus_nimet)-len(vieras_yh)))]
            
        vieras_pe = st.multiselect("Penkki (10% paino)", vieras_pe_opt, max_selections=17, key=vieras_pe_key, on_change=est_select_all, args=(vieras_pe_key,))

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    stadion_info = STADION_DATA.get(koti_lyh, {"Stadion": "Tuntematon", "PF": 1.00, "Dome": False})
    on_dome = stadion_info.get("Dome", False)
    pf_val  = stadion_info.get("PF", 1.00)

    dome_badge = '<span class="dome-badge">🔒 Dome (Katto)</span>' if on_dome else '<span class="outdoor-badge">🌿 Ulkoilma</span>'

    st.markdown(
        f"<div class='weather-box'>"
        f"<div class='weather-title'>🏟️ STADION &amp; OLOSUHTEET</div>"
        f"<div class='stadion-info'>🏟️ Stadion: <b>{stadion_info['Stadion']}</b> {dome_badge} &nbsp;&nbsp;|&nbsp;&nbsp; Park Factor: <b>{pf_val:.2f}</b></div>",
        unsafe_allow_html=True,
    )

    saa_c1, saa_c2, saa_c3 = st.columns(3)
    with saa_c1: lampotila_f = st.number_input("🌡️ Lämpötila (°F)", min_value=10, max_value=120, value=68, step=1, key="lampotila", disabled=on_dome)
    with saa_c2: tuuli_mph = st.number_input("💨 Tuuli (mph)", min_value=0.0, max_value=60.0, value=0.0, step=1.0, key="tuuli_mph", disabled=on_dome)
    with saa_c3: tuuli_suunta = st.selectbox("🧭 Tuulen suunta", TUULI_OPTIOT, index=0, key="tuuli_suunta", disabled=on_dome)

    if on_dome: st.markdown("<p class='warn'>🔒 Dome-stadion – sää ei vaikuta peliin, kentät poistettu käytöstä.</p>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if st.button("⚡ LASKE TODENNÄKÖISYYS"):
        st.session_state["saved_inputs"] = {
            "koti_koko":       koti_koko, "koti_lyh": koti_lyh, "koti_sp_nimi": koti_sp_nimi, "koti_yh": koti_yh, "koti_pe": koti_pe,
            "vieras_koko":     vieras_koko, "vieras_lyh": vieras_lyh, "vieras_sp_nimi": vieras_sp_nimi, "vieras_yh": vieras_yh, "vieras_pe": vieras_pe,
            "lampotila_c":     20 if on_dome else int(round((lampotila_f - 32) * 5/9)),
            "tuuli_ms":        0.0 if on_dome else float(tuuli_mph * 0.44704),
            "tuuli_suunta":    "Sivutuuli / Tyyni" if on_dome else tuuli_suunta,
        }
        if "tallennettu_viesti" in st.session_state: del st.session_state["tallennettu_viesti"]

    if "saved_inputs" in st.session_state:
        inp = st.session_state["saved_inputs"]
        koti_sp_data   = optiot_syottajat[inp["koti_sp_nimi"]]
        vieras_sp_data = optiot_syottajat[inp["vieras_sp_nimi"]]
        koti_bp_data   = bp_dict.get(inp["koti_lyh"],   {"All": 3.80, "vs_L": 3.80, "vs_R": 3.80, "K_BB_pct": 0.150})
        vieras_bp_data = bp_dict.get(inp["vieras_lyh"], {"All": 3.80, "vs_L": 3.80, "vs_R": 3.80, "K_BB_pct": 0.150})

        koti_sp_arm   = koti_sp_data.get("Katisyys", "R")
        vieras_sp_arm = vieras_sp_data.get("Katisyys", "R")

        koti_woba_sp, koti_iso   = laske_joukkueen_woba(inp["koti_yh"], inp["koti_pe"], vieras_sp_arm)
        vieras_woba_sp, vieras_iso = laske_joukkueen_woba(inp["vieras_yh"], inp["vieras_pe"], koti_sp_arm)
        koti_woba_bp, _   = laske_joukkueen_woba(inp["koti_yh"], inp["koti_pe"], "All")
        vieras_woba_bp, _ = laske_joukkueen_woba(inp["vieras_yh"], inp["vieras_pe"], "All")

        tulos = laske_todennakoisyys(
            inp["koti_koko"], inp["vieras_koko"],
            koti_sp=koti_sp_data, koti_bp=koti_bp_data, koti_woba=koti_woba_sp,
            vieras_sp=vieras_sp_data, vieras_bp=vieras_bp_data, vieras_woba=vieras_woba_sp,
            koti_woba_bp=koti_woba_bp, vieras_woba_bp=vieras_woba_bp,
            lampotila_c=inp["lampotila_c"], tuuli_ms=inp["tuuli_ms"], tuuli_suunta=inp["tuuli_suunta"],
            koti_lyh=inp["koti_lyh"], koti_iso=koti_iso, vieras_iso=vieras_iso
        )

        k_pct  = tulos["koti_voitto_tod"]   * 100
        v_pct  = tulos["vieras_voitto_tod"] * 100
        k_odds = 1 / tulos["koti_voitto_tod"]
        v_odds = 1 / tulos["vieras_voitto_tod"]

        st.session_state["viimeisin_tulos"] = {
            "Pvm":            str(date.today()), "Koti": inp["koti_koko"], "Vieras": inp["vieras_koko"],
            "Koti %":         f"{k_pct:.1f}", "Vieras %": f"{v_pct:.1f}",
            "Koti kerroin":   f"{k_odds:.2f}", "Vieras kerroin": f"{v_odds:.2f}",
            "Koti SP xFIP":   f"{tulos['koti_sp_dyn']:.2f}", "Koti BP xFIP": f"{tulos['koti_bp_dyn']:.2f}",
            "Koti wOBA":      f"{tulos['koti_woba_total']:.3f}", "Koti ISO": f"{koti_iso:.3f}",
            "Koti SP K-BB%":  f"{koti_sp_data.get('K_BB_pct', 0.15)*100:.1f}%", "Koti SP IP": f"{koti_sp_data['IP_total']:.1f}", "Koti SP IP/GS": f"{koti_sp_data['IP']:.2f}",
            "Vieras SP xFIP": f"{tulos['vieras_sp_dyn']:.2f}", "Vieras BP xFIP": f"{tulos['vieras_bp_dyn']:.2f}",
            "Vieras wOBA":    f"{tulos['vieras_woba_total']:.3f}", "Vieras ISO": f"{vieras_iso:.3f}",
            "Vieras SP K-BB%":f"{vieras_sp_data.get('K_BB_pct', 0.15)*100:.1f}%", "Vieras SP IP": f"{vieras_sp_data['IP_total']:.1f}", "Vieras SP IP/GS": f"{vieras_sp_data['IP']:.2f}",
            "O/U odotus":     f"{tulos['total_odotus']:.1f}", "Koti odotus": f"{tulos['k_odotus']:.1f}", "Vieras odotus": f"{tulos['v_odotus']:.1f}",
            "Koti tulos":     "", "Vieras tulos": ""
        }

        st.markdown('<hr class="divider">', unsafe_allow_html=True)
        col1, col2, col3 = st.columns([10, 1, 10])

        with col1:
            st.markdown(
                f"""<div class="result-card">
                <div class="result-team">{inp['koti_koko']}</div>
                <div class="fip-badge">{koti_sp_data['Name']}</div><br>
                <span style="color:#7a6e5f;font-size:0.85rem">
                Hyökkäyksen wOBA: <b>{tulos['koti_woba_total']:.3f}</b><br>
                SP xFIP: {tulos['koti_sp_dyn']:.2f} | BP xFIP: {tulos['koti_bp_dyn']:.2f}<br>
                <b>Yhdistetty xFIP: {tulos['koti_total_xfip']:.2f}</b><br>
                <b>Momentum-etu: {tulos['momentum_edge'] * 100:.2f} %</b></span>
                <div class="result-pct" style="color:{'#c8a84b' if k_pct >= v_pct else '#e8e0d0'}">{k_pct:.1f}%</div>
                <div class="result-odds">{k_odds:.2f}</div></div>""",
                unsafe_allow_html=True,
            )

        with col3:
            st.markdown(
                f"""<div class="result-card">
                <div class="result-team">{inp['vieras_koko']}</div>
                <div class="fip-badge">{vieras_sp_data['Name']}</div><br>
                <span style="color:#7a6e5f;font-size:0.85rem">
                Hyökkäyksen wOBA: <b>{tulos['vieras_woba_total']:.3f}</b><br>
                SP xFIP: {tulos['vieras_sp_dyn']:.2f} | BP xFIP: {tulos['vieras_bp_dyn']:.2f}<br>
                <b>Yhdistetty xFIP: {tulos['vieras_total_xfip']:.2f}</b><br>
                <b>Momentum-etu: {-tulos['momentum_edge'] * 100:.2f} %</b></span>
                <div class="result-pct" style="color:{'#c8a84b' if v_pct > k_pct else '#e8e0d0'}">{v_pct:.1f}%</div>
                <div class="result-odds">{v_odds:.2f}</div></div>""",
                unsafe_allow_html=True,
            )

        st.markdown('<hr class="divider">', unsafe_allow_html=True)
        st.markdown(
            f'<div class="ou-card">'
            f'<div style="color:#5a7a50;letter-spacing:0.2em;">JUOKSUODOTTAMA (O/U)</div>'
            f'<div class="ou-runs">{tulos["total_odotus"]:.1f}</div>'
            f'<div>{inp["koti_lyh"]}: {tulos["k_odotus"]:.1f} &nbsp;|&nbsp; {inp["vieras_lyh"]}: {tulos["v_odotus"]:.1f}</div></div>',
            unsafe_allow_html=True,
        )

        st.markdown('<hr class="divider">', unsafe_allow_html=True)
        st.markdown("<p style='font-family:Bebas Neue,sans-serif;font-size:1.4rem;color:#7a6e5f;letter-spacing:0.15em;margin-bottom:0.5rem;'>LISÄTIEDOT</p>", unsafe_allow_html=True)
        det_c1, det_c2, det_c3 = st.columns([10, 1, 10])

        def detail_card_html(joukkue, sp_xfip, sp_kbb, bp_xfip, woba, iso_arvo, sp_arm, stadion_nimi=None):
            stadion_rivi = f"<div class='detail-row'><span class='detail-key'>Stadion</span><span class='detail-val'>{stadion_nimi}</span></div>" if stadion_nimi else ""
            return (
                f"<div class='detail-card'><div class='detail-title'>{joukkue}</div>"
                f"<div class='detail-row'><span class='detail-key'>Aloittajan xFIP</span><span class='detail-val green'>{sp_xfip:.2f}</span></div>"
                f"<div class='detail-row'><span class='detail-key'>Aloittajan K-BB%</span><span class='detail-val green'>{sp_kbb*100:.1f}%</span></div>"
                f"<div class='detail-row'><span class='detail-key'>Bullpen xFIP</span><span class='detail-val green'>{bp_xfip:.2f}</span></div>"
                f"<div class='detail-row'><span class='detail-key'>Hyökkäys wOBA<span style='font-size:0.78em;color:#5a5450;'>&nbsp;(vs {sp_arm}HP)</span></span><span class='detail-val gold'>{woba:.3f}</span></div>"
                f"<div class='detail-row'><span class='detail-key'>Tyrmäysvoima (ISO)</span><span class='detail-val gold'>{iso_arvo:.3f}</span></div>"
                f"{stadion_rivi}</div>"
            )

        with det_c1:
            st.markdown(detail_card_html(inp["koti_koko"], tulos["koti_sp_dyn"], koti_sp_data.get("K_BB_pct", 0.150), tulos["koti_bp_dyn"], koti_woba_sp, koti_iso, vieras_sp_arm, stadion_nimi=tulos.get("stadion_nimi")), unsafe_allow_html=True)

        with det_c3:
            st.markdown(detail_card_html(inp["vieras_koko"], tulos["vieras_sp_dyn"], vieras_sp_data.get("K_BB_pct", 0.150), tulos["vieras_bp_dyn"], vieras_woba_sp, vieras_iso, koti_sp_arm, stadion_nimi=None), unsafe_allow_html=True)

        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
        _, save_col, _ = st.columns([3, 4, 3])
        with save_col:
            st.markdown("<div class='save-btn'>", unsafe_allow_html=True)
            if st.button("💾 Tallenna seurantaan", key="tallenna_btn"):
                rivi = st.session_state.get("viimeisin_tulos")
                if rivi:
                    tallenna_peli(rivi)
                    st.session_state["tallennettu_viesti"] = f"✅ Tallennettu: {rivi['Koti']} vs {rivi['Vieras']} ({rivi['Pvm']})"
            st.markdown("</div>", unsafe_allow_html=True)
            if "tallennettu_viesti" in st.session_state: st.success(st.session_state["tallennettu_viesti"])

with tab_seuranta:
    st.markdown("<h3 style='font-family:Bebas Neue,sans-serif;letter-spacing:0.12em;color:#c8a84b;'>📂 TALLENNETUT PELIT</h3>", unsafe_allow_html=True)
    varmista_csv()
    df_seuranta = lataa_seuranta_df()

    if df_seuranta.empty: st.info("Ei tallennettuja pelejä. Analysoi ottelu Uusi Analyysi -välilehdellä ja paina '💾 Tallenna seurantaan'.")
    else:
        st.markdown(f"<p style='color:#7a6e5f;font-size:0.85rem;'>{len(df_seuranta)} ottelua tallennettu. Syötä lopputulokset suoraan taulukkoon ja muutokset tallentuvat automaattisesti.</p>", unsafe_allow_html=True)

        muokattu = st.data_editor(
            df_seuranta, width="stretch", num_rows="dynamic",
            column_config={
                "Pvm": st.column_config.TextColumn("Pvm", width="small"), "Koti": st.column_config.TextColumn("Koti", width="medium"), "Vieras": st.column_config.TextColumn("Vieras", width="medium"),
                "Koti %": st.column_config.TextColumn("Koti %", width="small"), "Vieras %": st.column_config.TextColumn("Vieras %", width="small"),
                "Koti kerroin": st.column_config.TextColumn("Koti k.", width="small"), "Vieras kerroin": st.column_config.TextColumn("Vieras k.", width="small"),
                "Koti SP xFIP": st.column_config.TextColumn("K SP", width="small"), "Koti BP xFIP": st.column_config.TextColumn("K BP", width="small"),
                "Koti wOBA": st.column_config.TextColumn("K wOBA", width="small"), "Koti ISO": st.column_config.TextColumn("K ISO", width="small"), "Koti SP K-BB%": st.column_config.TextColumn("K K-BB%", width="small"),
                "Koti SP IP": st.column_config.TextColumn("K IP", width="small"), "Koti SP IP/GS": st.column_config.TextColumn("K IP/GS", width="small"),
                "Vieras SP xFIP": st.column_config.TextColumn("V SP", width="small"), "Vieras BP xFIP": st.column_config.TextColumn("V BP", width="small"),
                "Vieras wOBA": st.column_config.TextColumn("V wOBA", width="small"), "Vieras ISO": st.column_config.TextColumn("V ISO", width="small"), "Vieras SP K-BB%": st.column_config.TextColumn("V K-BB%", width="small"),
                "Vieras SP IP": st.column_config.TextColumn("V IP", width="small"), "Vieras SP IP/GS": st.column_config.TextColumn("V IP/GS", width="small"),
                "O/U odotus": st.column_config.TextColumn("O/U", width="small"), "Koti odotus": st.column_config.TextColumn("Koti O", width="small"), "Vieras odotus": st.column_config.TextColumn("Vieras O", width="small"),
                "Koti tulos": st.column_config.TextColumn("✏ Koti R", width="small"), "Vieras tulos": st.column_config.TextColumn("✏ Vieras R", width="small"),
            },
            key="seuranta_editor",
        )

        df_seuranta, muokattu = df_seuranta.fillna(""), muokattu.fillna("")
        if not muokattu.equals(df_seuranta):
            tallenna_seuranta_df(muokattu)
            st.success("✅ Muutokset tallennettu.")
            st.rerun()