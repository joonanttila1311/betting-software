"""
app.py  –  MLB Vedonlyönti-UI  v4.0 (Statcast Edition)
======================================================
Käyttöliittymä käyttää nyt ammattilaistason FIP-dataa ERA:n sijaan.
"""

import sqlite3
from pathlib import Path
import pandas as pd
import streamlit as st
from laskentamoottori import laske_todennakoisyys, lataa_data, DB_POLKU, TAULU

st.set_page_config(page_title="MLB Odds Engine", page_icon="⚾", layout="centered")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Source+Serif+4:ital,wght@0,300;0,600;1,300&display=swap');
    html, body, [data-testid="stAppViewContainer"] { background-color: #0d0d0d; color: #e8e0d0; }
    [data-testid="stHeader"] { background: transparent; }
    [data-testid="stSidebar"] { display: none; }
    h1, h2, h3 { font-family: 'Bebas Neue', sans-serif; letter-spacing: 0.06em; }
    .main-title { font-family: 'Bebas Neue', sans-serif; font-size: clamp(2.8rem, 6vw, 5rem); letter-spacing: 0.1em; color: #f0e6c8; text-align: center; margin-bottom: 0; }
    .main-subtitle { font-family: 'Source Serif 4', serif; font-style: italic; font-weight: 300; font-size: 1rem; color: #7a6e5f; text-align: center; letter-spacing: 0.15em; margin-top: 0.3rem; margin-bottom: 2.5rem; }
    .divider { border: none; border-top: 1px solid #2e2a24; margin: 1.8rem 0; }
    label[data-testid="stWidgetLabel"] p { font-family: 'Bebas Neue', sans-serif; font-size: 0.85rem; letter-spacing: 0.18em; color: #7a6e5f; }
    [data-testid="stSelectbox"] > div > div { background-color: #1a1814 !important; border: 1px solid #2e2a24 !important; color: #e8e0d0 !important; font-family: 'Source Serif 4', serif !important; }
    .fip-badge { display: inline-block; background: #121e10; border: 1px solid #203a20; border-radius: 3px; padding: 0.18rem 0.55rem; font-family: 'Source Serif 4', serif; font-size: 0.72rem; color: #4bc84b; letter-spacing: 0.06em; margin-bottom: 0.9rem; }
    div.stButton > button { width: 100%; background-color: #c8a84b; color: #0d0d0d; font-family: 'Bebas Neue', sans-serif; font-size: 1.35rem; letter-spacing: 0.2em; border: none; padding: 0.7rem 1rem; margin-top: 1.2rem; transition: background-color 0.2s ease, transform 0.1s ease; }
    div.stButton > button:hover { background-color: #e0bf60; transform: translateY(-1px); }
    .result-card { background: #141210; border: 1px solid #2e2a24; border-radius: 6px; padding: 1.4rem 1.6rem; text-align: center; }
    .result-team { font-family: 'Bebas Neue', sans-serif; font-size: 1.55rem; letter-spacing: 0.08em; color: #e8e0d0; margin-bottom: 0.15rem; }
    .result-role { font-family: 'Source Serif 4', serif; font-size: 0.7rem; font-style: italic; color: #5a5040; margin-bottom: 0.5rem; }
    .result-pitcher { font-family: 'Source Serif 4', serif; font-size: 0.78rem; color: #9a8c70; margin-bottom: 0.4rem; }
    .result-pct { font-family: 'Bebas Neue', sans-serif; font-size: 3.8rem; line-height: 1; margin-bottom: 0.1rem; }
    .result-pct-label { font-family: 'Source Serif 4', serif; font-size: 0.7rem; color: #7a6e5f; text-transform: uppercase; margin-bottom: 1.1rem; }
    .result-odds { font-family: 'Source Serif 4', serif; font-size: 1.55rem; font-weight: 600; color: #c8a84b; }
    .result-odds-label { font-family: 'Source Serif 4', serif; font-size: 0.68rem; color: #5a5040; text-transform: uppercase; }
    .vs-badge { font-family: 'Bebas Neue', sans-serif; font-size: 2.2rem; color: #2e2a24; text-align: center; padding-top: 1.2rem; }
    [data-testid="stProgress"] > div > div > div { background-color: #c8a84b !important; }
    [data-testid="stProgress"] > div > div { background-color: #1a1814 !important; }
    .ou-section-title { font-family: 'Bebas Neue', sans-serif; font-size: 0.78rem; letter-spacing: 0.28em; color: #5a7a50; text-align: center; margin-bottom: 1rem; }
    .ou-total-card { background: #0e1208; border: 1px solid #2a3820; border-radius: 6px; padding: 1.6rem 1.4rem 1.2rem; text-align: center; margin-bottom: 0.8rem; }
    .ou-total-number { font-family: 'Bebas Neue', sans-serif; font-size: 4.6rem; color: #7ec870; line-height: 1; }
    .ou-total-unit { font-family: 'Source Serif 4', serif; font-size: 0.75rem; color: #4a6040; text-transform: uppercase; margin-bottom: 1.1rem; }
    .ou-total-label { font-family: 'Bebas Neue', sans-serif; font-size: 1.05rem; color: #5a7a50; }
    .ou-team-card { background: #111309; border: 1px solid #232d1a; border-radius: 5px; padding: 1.1rem 1.2rem; text-align: center; }
    .ou-team-name { font-family: 'Bebas Neue', sans-serif; font-size: 1.1rem; color: #c8d4b8; }
    .ou-team-role { font-family: 'Source Serif 4', serif; font-size: 0.65rem; font-style: italic; color: #3a4a2e; margin-bottom: 0.7rem; }
    .ou-runs { font-family: 'Bebas Neue', sans-serif; font-size: 2.6rem; color: #7ec870; line-height: 1; }
    .ou-runs-label { font-family: 'Source Serif 4', serif; font-size: 0.65rem; color: #3a4a2e; text-transform: uppercase; }
    .ou-stat-row { font-family: 'Source Serif 4', serif; font-size: 0.7rem; color: #4a5c3e; margin-top: 0.6rem; font-style: italic; }
    .ou-stat-row span { color: #7a9a6e; font-style: normal; }
    .info-box, .era-info-box, .ou-note-box { font-family: 'Source Serif 4', serif; font-size: 0.85rem; border-radius: 4px; padding: 0.9rem 1.2rem; margin-top: 1rem; font-style: italic; }
    .info-box { background: #141210; border: 1px solid #2e2a24; border-left: 3px solid #c8a84b; color: #7a6e5f; }
    .info-box b { color: #c8a84b; font-style: normal; font-weight: 600; }
    .era-info-box { background: #0f1208; border: 1px solid #203a20; border-left: 3px solid #4bc84b; color: #5a7a50; }
    .era-info-box b { color: #4bc84b; font-style: normal; }
    .ou-note-box { background: #0b0e08; border: 1px solid #2a3820; border-left: 3px solid #4a7040; color: #4a5c3e; }
    .warn-box { background: #1a1208; border: 1px solid #c8a84b44; border-radius: 4px; padding: 0.8rem 1.2rem; text-align: center; font-family: 'Source Serif 4', serif; font-size: 0.88rem; color: #c8a84b99; margin-top: 1rem; }
    .no-pitcher { font-family: 'Source Serif 4', serif; font-size: 0.75rem; color: #3a3228; font-style: italic; margin-bottom: 0.9rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<p class="main-title">⚾ MLB Odds Engine</p>', unsafe_allow_html=True)
st.markdown('<p class="main-subtitle">Statcast Edition · FIP Integration · v4.0</p>', unsafe_allow_html=True)
st.markdown('<hr class="divider">', unsafe_allow_html=True)

@st.cache_data(show_spinner=False)
def hae_joukkueet() -> list[str]:
    if not Path(DB_POLKU).exists(): return []
    yhteys = sqlite3.connect(DB_POLKU)
    try:
        df = pd.read_sql_query(f"SELECT DISTINCT Kotijoukkue AS j FROM {TAULU} UNION SELECT DISTINCT Vierasjoukkue FROM {TAULU} ORDER BY j", yhteys)
    finally:
        yhteys.close()
    return df["j"].tolist()

@st.cache_data(show_spinner=False)
def hae_syottajat_statcast() -> pd.DataFrame:
    if not Path(DB_POLKU).exists(): return pd.DataFrame()
    yhteys = sqlite3.connect(DB_POLKU)
    try:
        df = pd.read_sql_query("SELECT Name, FIP, IP FROM syottajat_statcast ORDER BY Name", yhteys)
    except Exception:
        df = pd.DataFrame()
    finally:
        yhteys.close()
    return df

def syottajat_lista(df_syottajat: pd.DataFrame) -> list[str]:
    if df_syottajat.empty: return ["— ei statcast-dataa —"]
    optiot = [f"{r['Name']}  (FIP: {r['FIP']:.2f} | IP: {r['IP']:.1f})" for _, r in df_syottajat.iterrows()]
    return ["— valitse syöttäjä (kirjoita nimi) —"] + optiot

def fip_valinnasta(valinta: str) -> float | None:
    if not valinta or valinta.startswith("—"): return None
    try: return float(valinta.split("FIP:")[1].split("|")[0].strip())
    except (IndexError, ValueError): return None

def nimi_valinnasta(valinta: str) -> str | None:
    if not valinta or valinta.startswith("—"): return None
    return valinta.split("  (FIP:")[0].strip()

joukkueet = hae_joukkueet()
df_syottajat = hae_syottajat_statcast()

if not joukkueet: st.stop()

col_koti, col_vs, col_vieras = st.columns([10, 1, 10])
optiot = syottajat_lista(df_syottajat)

with col_koti:
    koti = st.selectbox("🏠  KOTIJOUKKUE", joukkueet, index=0)
    koti_valinta = st.selectbox("⚾  ALOITUSSYÖTTÄJÄ (KOTI)", optiot, key="koti_sp")

with col_vs:
    st.markdown('<div class="vs-badge">VS</div>', unsafe_allow_html=True)

with col_vieras:
    vieras_default = 1 if len(joukkueet) > 1 else 0
    vieras = st.selectbox("✈️  VIERASJOUKKUE", joukkueet, index=vieras_default)
    vieras_valinta = st.selectbox("⚾  ALOITUSSYÖTTÄJÄ (VIERAS)", optiot, key="vieras_sp")

koti_fip = fip_valinnasta(koti_valinta)
vieras_fip = fip_valinnasta(vieras_valinta)
koti_pitcher = nimi_valinnasta(koti_valinta)
vieras_pitcher = nimi_valinnasta(vieras_valinta)

laske = st.button("LASKE TODENNÄKÖISYYS")

if laske:
    if koti == vieras: st.stop()
    with st.spinner("Prosessoidaan Statcast-mallia..."):
        data = lataa_data()
        tulos = laske_todennakoisyys(koti, vieras, df=data, koti_fip=koti_fip, vieras_fip=vieras_fip)

    koti_pct = tulos["koti_voitto_tod"] * 100
    vieras_pct = tulos["vieras_voitto_tod"] * 100
    koti_kerroin = 1 / tulos["koti_voitto_tod"]
    vieras_kerroin = 1 / tulos["vieras_voitto_tod"]
    fip_mukana = tulos.get("fip_kaytossa", False)

    koti_color = "#c8a84b" if koti_pct >= vieras_pct else "#e8e0d0"
    vieras_color = "#c8a84b" if vieras_pct > koti_pct else "#e8e0d0"

    def pitcher_html(pitcher: str | None, fip: float | None) -> str:
        if pitcher and fip is not None:
            return f'<div class="result-pitcher">&#9918; {pitcher}</div><div class="fip-badge">FIP &nbsp; {fip:.2f}</div>'
        return '<div class="no-pitcher">Syöttäjää ei valittu</div>'

    st.markdown('<hr class="divider">', unsafe_allow_html=True)

    c1, c2, c3 = st.columns([10, 1, 10])
    with c1:
        st.markdown(f"""<div class="result-card"><div class="result-team">{koti}</div><div class="result-role">Kotijoukkue</div>{pitcher_html(koti_pitcher, koti_fip)}<div class="result-pct" style="color:{koti_color}">{koti_pct:.1f}<span style="font-size:1.6rem">%</span></div><div class="result-pct-label">Voittotn.</div><div class="result-odds">{koti_kerroin:.2f}</div><div class="result-odds-label">True Odds</div></div>""", unsafe_allow_html=True)
    with c2: st.markdown('<div class="vs-badge">–</div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div class="result-card"><div class="result-team">{vieras}</div><div class="result-role">Vierasjoukkue</div>{pitcher_html(vieras_pitcher, vieras_fip)}<div class="result-pct" style="color:{vieras_color}">{vieras_pct:.1f}<span style="font-size:1.6rem">%</span></div><div class="result-pct-label">Voittotn.</div><div class="result-odds">{vieras_kerroin:.2f}</div><div class="result-odds-label">True Odds</div></div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.progress(int(koti_pct))

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown('<div class="ou-section-title">&#127919; &nbsp; JUOKSUODOTTAMA &nbsp; / &nbsp; OVER · UNDER</div>', unsafe_allow_html=True)

    total_od = tulos["total_odotus"]
    st.markdown(f'<div class="ou-total-card"><div class="ou-total-number">{total_od:.1f}</div><div class="ou-total-unit">Juoksua yhteensä</div><div class="ou-total-label">O/U&#8209;LINJA</div></div>', unsafe_allow_html=True)

    d1, d_mid, d2 = st.columns([10, 1, 10])
    with d1:
        st.markdown(f"""<div class="ou-team-card"><div class="ou-team-name">{koti}</div><div class="ou-team-role">Kotijoukkue</div><div class="ou-runs">{tulos['koti_odotus']:.1f}</div><div class="ou-runs-label">Odotettua juoksua</div><div class="ou-stat-row">Pisteytyska.&nbsp;<span>{tulos['koti_pisteet_ka']:.1f}</span> &nbsp;&#183;&nbsp; Päästöka.&nbsp;<span>{tulos['koti_paastot_ka']:.1f}</span></div></div>""", unsafe_allow_html=True)
    with d_mid: st.markdown('<div style="text-align:center;padding-top:1.8rem;font-family:\'Bebas Neue\',sans-serif;font-size:1.2rem;color:#2a3820;">+</div>', unsafe_allow_html=True)
    with d2:
        st.markdown(f"""<div class="ou-team-card"><div class="ou-team-name">{vieras}</div><div class="ou-team-role">Vierasjoukkue</div><div class="ou-runs">{tulos['vieras_odotus']:.1f}</div><div class="ou-runs-label">Odotettua juoksua</div><div class="ou-stat-row">Pisteytyska.&nbsp;<span>{tulos['vieras_pisteet_ka']:.1f}</span> &nbsp;&#183;&nbsp; Päästöka.&nbsp;<span>{tulos['vieras_paastot_ka']:.1f}</span></div></div>""", unsafe_allow_html=True)

    ou_malli = "35% hyökkäys + 35% puolustus + 30% Statcast FIP" if fip_mukana else "50% hyökkäys + 50% puolustus (syöttäjää ei valittu)"
    st.markdown(f'<div class="ou-note-box">&#128202; O/U&#8209;malli: {ou_malli}.</div>', unsafe_allow_html=True)

    malli = "60 % VP + 20 % H2H + 20 % FIP&#8209;vertailu" if fip_mukana else "70 % VP + 30 % H2H"
    st.markdown(f'<div class="info-box">&#128202;&nbsp; Yleinen voitto&#8209;%: <b>{koti}</b> {tulos["koti_yleinen_vp"]*100:.1f} % &nbsp;|&nbsp; <b>{vieras}</b> {tulos["vieras_yleinen_vp"]*100:.1f} %<br><br><span style="font-size:0.75rem">Voittomalli: {malli}</span></div>', unsafe_allow_html=True)

    if fip_mukana:
        parempi = koti_pitcher if koti_fip < vieras_fip else vieras_pitcher
        st.markdown(f'<div class="era-info-box">&#9918; FIP&#8209;analyysi:&nbsp; <b>{koti_pitcher}</b> {koti_fip:.2f} &nbsp;vs&nbsp; <b>{vieras_pitcher}</b> {vieras_fip:.2f} &nbsp;&#183;&nbsp; Parempi (pienempi FIP): <b>{parempi}</b></div>', unsafe_allow_html=True)