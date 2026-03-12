"""
app.py  –  MLB Vedonlyönti-UI  v6.0 (Platoon Splits Edition)
"""

import sqlite3
import pandas as pd
import streamlit as st
from laskentamoottori import laske_todennakoisyys, lataa_data, DB_POLKU

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
    </style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-title">MLB PRO ENGINE</p><p class="main-subtitle">Time Decay & Platoon Splits Integration · v6.0</p>', unsafe_allow_html=True)

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
    df = pd.read_sql_query("SELECT Name, Team, xFIP_All, xFIP_vs_L, xFIP_vs_R, IP_per_Start FROM syottajat_statcast ORDER BY Name", conn)
    conn.close()
    optiot = {}
    for _, r in df.iterrows():
        # Avain käyttöliittymän pudotusvalikkoon, arvo on sanakirja tilastoja
        avain = f"{r['Name']} ({r['Team']}) | xFIP: {r['xFIP_All']:.2f}"
        optiot[avain] = {"vs_L": r["xFIP_vs_L"], "vs_R": r["xFIP_vs_R"], "IP": r["IP_per_Start"]}
    return optiot

@st.cache_data
def lataa_bullpenit():
    conn = sqlite3.connect(DB_POLKU)
    df = pd.read_sql_query("SELECT Team, Bullpen_xFIP_vs_L, Bullpen_xFIP_vs_R FROM bullpen_statcast", conn)
    conn.close()
    df = df.set_index("Team")
    return {team: {"vs_L": row["Bullpen_xFIP_vs_L"], "vs_R": row["Bullpen_xFIP_vs_R"]} for team, row in df.iterrows()}

tiimit = lataa_tiimit()
optiot_syottajat = lataa_syottajat()
bp_dict = lataa_bullpenit()

if not tiimit or not optiot_syottajat: st.stop()

def pura_joukkue(valinta):
    osat = valinta.split(" (")
    return osat[0], osat[1].replace(")", "") if len(osat) > 1 else osat[0]

c1, c2, c3 = st.columns([10, 1, 10])

with c1:
    st.markdown("### 🏠 KOTIJOUKKUE")
    koti_valinta = st.selectbox("Joukkue", tiimit, index=0, key="k_team")
    koti_koko, koti_lyh = pura_joukkue(koti_valinta)
    koti_sp_nimi = st.selectbox("Aloitussyöttäjä", list(optiot_syottajat.keys()), key="k_sp")
    
    st.markdown("<br><b>Kotijoukkueen Lyöjät (Kohtaavat vierassyöttäjän):</b>", unsafe_allow_html=True)
    koti_lhb = st.slider(f"Vasenkätisiä lyöjiä (LHB) - {koti_lyh}", 0, 9, 3, key="k_lhb")
    koti_rhb = 9 - koti_lhb
    st.caption(f"Oikeakätisiä lyöjiä (RHB): **{koti_rhb}**")

with c3:
    st.markdown("### ✈️ VIERASJOUKKUE")
    vieras_valinta = st.selectbox("Joukkue", tiimit, index=1 if len(tiimit)>1 else 0, key="v_team")
    vieras_koko, vieras_lyh = pura_joukkue(vieras_valinta)
    vieras_sp_nimi = st.selectbox("Aloitussyöttäjä", list(optiot_syottajat.keys()), key="v_sp")
    
    st.markdown("<br><b>Vierasjoukkueen Lyöjät (Kohtaavat kotisyöttäjän):</b>", unsafe_allow_html=True)
    vieras_lhb = st.slider(f"Vasenkätisiä lyöjiä (LHB) - {vieras_lyh}", 0, 9, 3, key="v_lhb")
    vieras_rhb = 9 - vieras_lhb
    st.caption(f"Oikeakätisiä lyöjiä (RHB): **{vieras_rhb}**")

if st.button("LASKE TODENNÄKÖISYYS (PRO MALLI)"):
    koti_sp_data = optiot_syottajat[koti_sp_nimi]
    vieras_sp_data = optiot_syottajat[vieras_sp_nimi]
    
    # Haetaan Bullpen-data (Fallback 3.20 jos dataa ei löydy)
    koti_bp_data = bp_dict.get(koti_lyh, {"vs_L": 3.20, "vs_R": 3.20})
    vieras_bp_data = bp_dict.get(vieras_lyh, {"vs_L": 3.20, "vs_R": 3.20})
    
    koti_lyojat = {"L": koti_lhb, "R": koti_rhb}
    vieras_lyojat = {"L": vieras_lhb, "R": vieras_rhb}
    
    tulos = laske_todennakoisyys(
        koti_koko, vieras_koko, df=lataa_data(),
        koti_sp=koti_sp_data, koti_bp=koti_bp_data, koti_lyojat=koti_lyojat,
        vieras_sp=vieras_sp_data, vieras_bp=vieras_bp_data, vieras_lyojat=vieras_lyojat
    )

    k_pct, v_pct = tulos["koti_voitto_tod"] * 100, tulos["vieras_voitto_tod"] * 100
    k_odds, v_odds = 1/tulos["koti_voitto_tod"], 1/tulos["vieras_voitto_tod"]

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([10, 1, 10])
    with col1:
        # Huom: koti_sp_dyn on kotijoukkueen syöttäjän arvo = vastustajan juoksujen estäminen
        st.markdown(f"""<div class="result-card"><div class="result-team">{koti_koko}</div>
        <div class="fip-badge">{koti_sp_nimi.split(' |')[0]}</div><br>
        <span style="color:#7a6e5f;font-size:0.85rem">
        SP xFIP (Mukautettu): {tulos['koti_sp_dyn']:.2f}<br>
        BP xFIP (Mukautettu): {tulos['koti_bp_dyn']:.2f}<br>
        <b>Ottelun syöttövoima: {tulos['koti_total_xfip']:.2f}</b></span>
        <div class="result-pct" style="color:{'#c8a84b' if k_pct>=v_pct else '#e8e0d0'}">{k_pct:.1f}%</div>
        <div class="result-odds">{k_odds:.2f}</div></div>""", unsafe_allow_html=True)
        
    with col3:
        st.markdown(f"""<div class="result-card"><div class="result-team">{vieras_koko}</div>
        <div class="fip-badge">{vieras_sp_nimi.split(' |')[0]}</div><br>
        <span style="color:#7a6e5f;font-size:0.85rem">
        SP xFIP (Mukautettu): {tulos['vieras_sp_dyn']:.2f}<br>
        BP xFIP (Mukautettu): {tulos['vieras_bp_dyn']:.2f}<br>
        <b>Ottelun syöttövoima: {tulos['vieras_total_xfip']:.2f}</b></span>
        <div class="result-pct" style="color:{'#c8a84b' if v_pct>k_pct else '#e8e0d0'}">{v_pct:.1f}%</div>
        <div class="result-odds">{v_odds:.2f}</div></div>""", unsafe_allow_html=True)

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown(f'<div class="ou-card"><div style="color:#5a7a50;letter-spacing:0.2em;">JUOKSUODOTTAMA (O/U)</div><div class="ou-runs">{tulos["total_odotus"]:.1f}</div><div>{koti_lyh}: {tulos["k_odotus"]:.1f} &nbsp;|&nbsp; {vieras_lyh}: {tulos["v_odotus"]:.1f}</div></div>', unsafe_allow_html=True)