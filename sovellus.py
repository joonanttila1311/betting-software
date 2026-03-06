"""
app.py  –  MLB Vedonlyönti-UI  v5.1 (Sanakirja-korjaus)
"""

import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path
from laskentamoottori import laske_todennakoisyys, lataa_data, DB_POLKU

st.set_page_config(page_title="MLB Pro Engine", page_icon="⚾", layout="centered")

# --- SANAKIRJA LYHENTEILLE ---
# Yhdistää Statcastin lyhenteet otteluhistorian koko nimiin
# --- SANAKIRJA LYHENTEILLE ---
# Yhdistää Statcastin lyhenteet otteluhistorian koko nimiin
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
    .main-title { font-family: 'Bebas Neue', sans-serif; font-size: 4rem; color: #f0e6c8; text-align: center; margin-bottom: 0; }
    .main-subtitle { font-family: 'Source Serif 4', serif; font-style: italic; color: #7a6e5f; text-align: center; margin-bottom: 2rem; }
    .divider { border-top: 1px solid #2e2a24; margin: 1.5rem 0; }
    .result-card { background: #141210; border: 1px solid #2e2a24; border-radius: 6px; padding: 1.5rem; text-align: center; }
    .result-team { font-family: 'Bebas Neue', sans-serif; font-size: 1.6rem; color: #e8e0d0; }
    .fip-badge { display: inline-block; background: #121e10; border: 1px solid #203a20; padding: 0.2rem 0.5rem; font-size: 0.8rem; color: #4bc84b; margin: 0.5rem 0; }
    .result-pct { font-family: 'Bebas Neue', sans-serif; font-size: 3.5rem; line-height: 1; }
    .result-odds { font-size: 1.5rem; color: #c8a84b; font-weight: bold; }
    .ou-card { background: #0e1208; border: 1px solid #2a3820; border-radius: 6px; padding: 1.5rem; text-align: center; margin-bottom: 1rem; }
    .ou-runs { font-family: 'Bebas Neue', sans-serif; font-size: 4rem; color: #7ec870; line-height: 1; }
    div.stButton > button { width: 100%; background-color: #c8a84b; color: #000; font-family: 'Bebas Neue', sans-serif; font-size: 1.5rem; }
    div.stButton > button:hover { background-color: #e0bf60; }
    </style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-title">MLB PRO ENGINE</p><p class="main-subtitle">Expected FIP & Bullpen Integration · v5.1</p>', unsafe_allow_html=True)

@st.cache_data
def lataa_tiimit():
    conn = sqlite3.connect(DB_POLKU)
    df = pd.read_sql_query("SELECT DISTINCT Team FROM bullpen_statcast ORDER BY Team", conn)
    conn.close()
    
    # Muotoillaan lista: "Minnesota Twins (MIN)"
    tiimit_lista = []
    for t in df["Team"].tolist():
        koko_nimi = MLB_JOUKKUEET.get(t, t)
        tiimit_lista.append(f"{koko_nimi} ({t})")
    
    # Järjestetään aakkosjärjestykseen koko nimen mukaan
    return sorted(tiimit_lista)

@st.cache_data
def lataa_syottajat():
    conn = sqlite3.connect(DB_POLKU)
    df = pd.read_sql_query("SELECT Name, Team, xFIP, IP_per_Start FROM syottajat_statcast ORDER BY Name", conn)
    conn.close()
    return df

@st.cache_data
def lataa_bullpenit():
    conn = sqlite3.connect(DB_POLKU)
    df = pd.read_sql_query("SELECT Team, Bullpen_xFIP FROM bullpen_statcast", conn)
    conn.close()
    return df.set_index("Team")["Bullpen_xFIP"].to_dict()

tiimit = lataa_tiimit()
df_syottajat = lataa_syottajat()
bp_dict = lataa_bullpenit()

if not tiimit: st.stop()

def luo_optiot():
    optiot = ["— Valitse syöttäjä —"]
    for _, r in df_syottajat.iterrows():
        optiot.append(f"{r['Name']} ({r['Team']}) | xFIP: {r['xFIP']:.2f} | IP/GS: {r['IP_per_Start']:.1f}")
    return optiot

optiot = luo_optiot()

def pura_valinta(valinta):
    if valinta.startswith("—"): return None, None
    nimi = valinta.split(" (")[0]
    xfip = float(valinta.split("xFIP: ")[1].split(" |")[0])
    ip_gs = float(valinta.split("IP/GS: ")[1])
    return xfip, ip_gs

def pura_joukkue(valinta):
    # Erottaa "Minnesota Twins (MIN)" -> "Minnesota Twins" ja "MIN"
    osat = valinta.split(" (")
    koko_nimi = osat[0]
    lyhenne = osat[1].replace(")", "") if len(osat) > 1 else koko_nimi
    return koko_nimi, lyhenne

c1, c2, c3 = st.columns([10, 1, 10])
with c1:
    koti_valinta = st.selectbox("🏠 KOTIJOUKKUE", tiimit, index=0)
    koti_sp = st.selectbox("⚾ ALOITUSSYÖTTÄJÄ (Koti)", optiot)
with c3:
    vieras_valinta = st.selectbox("✈️ VIERASJOUKKUE", tiimit, index=1 if len(tiimit)>1 else 0)
    vieras_sp = st.selectbox("⚾ ALOITUSSYÖTTÄJÄ (Vieras)", optiot)

if st.button("LASKE TODENNÄKÖISYYS (PRO MALLI)"):
    k_xfip, k_ip = pura_valinta(koti_sp)
    v_xfip, v_ip = pura_valinta(vieras_sp)
    
    # Puretaan joukkueen koko nimi ja lyhenne
    koti_koko, koti_lyh = pura_joukkue(koti_valinta)
    vieras_koko, vieras_lyh = pura_joukkue(vieras_valinta)
    
    # Bullpen xFIP haetaan LYHENTEELLÄ
    k_bp = bp_dict.get(koti_lyh, 3.20)
    v_bp = bp_dict.get(vieras_lyh, 3.20)
    
    # Laskentamoottorille syötetään KOKO NIMET (jotta historiadata löytyy)
    tulos = laske_todennakoisyys(
        koti_koko, vieras_koko, df=lataa_data(), 
        koti_aloittaja_xfip=k_xfip, koti_ip_start=k_ip, koti_bullpen=k_bp,
        vieras_aloittaja_xfip=v_xfip, vieras_ip_start=v_ip, vieras_bullpen=v_bp
    )

    k_pct, v_pct = tulos["koti_voitto_tod"] * 100, tulos["vieras_voitto_tod"] * 100
    k_odds, v_odds = 1/tulos["koti_voitto_tod"], 1/tulos["vieras_voitto_tod"]

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([10, 1, 10])
    with col1:
        bp_teksti = f"Bullpen xFIP: {k_bp:.2f}" if tulos["xfip_kaytossa"] else ""
        yhdistetty = f"<b>Koko ottelun xFIP: {tulos['koti_total_xfip']:.2f}</b>" if tulos["xfip_kaytossa"] else ""
        st.markdown(f"""<div class="result-card"><div class="result-team">{koti_koko}</div>
        <div class="fip-badge">{koti_sp.split(' |')[0]}</div><br><span style="color:#7a6e5f;font-size:0.8rem">{bp_teksti}<br>{yhdistetty}</span>
        <div class="result-pct" style="color:{'#c8a84b' if k_pct>=v_pct else '#e8e0d0'}">{k_pct:.1f}%</div>
        <div class="result-odds">{k_odds:.2f}</div></div>""", unsafe_allow_html=True)
        
    with col3:
        bp_teksti = f"Bullpen xFIP: {v_bp:.2f}" if tulos["xfip_kaytossa"] else ""
        yhdistetty = f"<b>Koko ottelun xFIP: {tulos['vieras_total_xfip']:.2f}</b>" if tulos["xfip_kaytossa"] else ""
        st.markdown(f"""<div class="result-card"><div class="result-team">{vieras_koko}</div>
        <div class="fip-badge">{vieras_sp.split(' |')[0]}</div><br><span style="color:#7a6e5f;font-size:0.8rem">{bp_teksti}<br>{yhdistetty}</span>
        <div class="result-pct" style="color:{'#c8a84b' if v_pct>k_pct else '#e8e0d0'}">{v_pct:.1f}%</div>
        <div class="result-odds">{v_odds:.2f}</div></div>""", unsafe_allow_html=True)

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown(f'<div class="ou-card"><div style="color:#5a7a50;letter-spacing:0.2em;">JUOKSUODOTTAMA (O/U)</div><div class="ou-runs">{tulos["total_odotus"]:.1f}</div><div>{koti_lyh}: {tulos["k_odotus"]:.1f} &nbsp;|&nbsp; {vieras_lyh}: {tulos["v_odotus"]:.1f}</div></div>', unsafe_allow_html=True)