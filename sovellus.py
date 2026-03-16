"""
app.py  –  MLB Vedonlyönti-UI  v9.2
====================================
v9.2: "Vapaat markkinat" -päivitys.
  - Syöttäjien ja lyöjien valikoista poistettu joukkuerajoitukset ja -lyhenteet.
  - Pelaajia voi siirtää vapaasti joukkueesta toiseen ilman että wOBA/xFIP-matematiikka kärsii.
  - Globaali pelaajarekisteri (kaikki_lyojat_id) varmistaa ID-numeroiden löytymisen.
"""

import csv
import sqlite3
from datetime import date
from pathlib import Path

import json
import pandas as pd
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
# DATAN LATAUS
# ────────────────────────────────────────────────────────────────────────────

@st.cache_data
def lataa_tiimit():
    conn = sqlite3.connect(DB_POLKU)
    df = pd.read_sql_query(
        "SELECT DISTINCT Team FROM bullpen_statcast ORDER BY Team", conn
    )
    conn.close()
    tiimit_lista = [
        f"{MLB_JOUKKUEET.get(t, t)} ({t})" for t in df["Team"].tolist()
    ]
    return sorted(tiimit_lista)


@st.cache_data
def lataa_syottajat():
    conn = sqlite3.connect(DB_POLKU)
    try:
        df = pd.read_sql_query(
            "SELECT Name, Team, xFIP_All, xFIP_vs_L, xFIP_vs_R, IP_per_Start, p_throws "
            "FROM syottajat_statcast ORDER BY Name",
            conn,
        )
    except Exception:
        df = pd.read_sql_query(
            "SELECT Name, Team, xFIP_All, xFIP_vs_L, xFIP_vs_R, IP_per_Start "
            "FROM syottajat_statcast ORDER BY Name",
            conn,
        )
        df["p_throws"] = "R"
    conn.close()

    optiot = {}
    for _, r in df.iterrows():
        katisyys = r.get("p_throws", "R")
        if pd.isna(katisyys):
            katisyys = "R"
        # POISTETTU JOUKKUEEN LYHENNE NIMEN PERÄSTÄ!
        avain = (
            f"{r['Name']} | {katisyys}HP | xFIP: {r['xFIP_All']:.2f}"
        )
        optiot[avain] = {
            "xFIP_All": r["xFIP_All"],
            "vs_L": r["xFIP_vs_L"],
            "vs_R": r["xFIP_vs_R"],
            "IP": r["IP_per_Start"],
            "Name": r["Name"],
            "Katisyys": katisyys,
        }
    return optiot


@st.cache_data
def lataa_bullpenit():
    conn = sqlite3.connect(DB_POLKU)
    df = pd.read_sql_query(
        "SELECT Team, Bullpen_xFIP_All, Bullpen_xFIP_vs_L, Bullpen_xFIP_vs_R "
        "FROM bullpen_statcast",
        conn,
    )
    conn.close()
    df = df.set_index("Team")
    return {
        team: {
            "All": row["Bullpen_xFIP_All"],
            "vs_L": row["Bullpen_xFIP_vs_L"],
            "vs_R": row["Bullpen_xFIP_vs_R"],
        }
        for team, row in df.iterrows()
    }


@st.cache_data
def lataa_rosterit():
    if not Path(JSON_POLKU).exists():
        return {}
    with open(JSON_POLKU, encoding="utf-8") as f:
        return json.load(f)


@st.cache_data
def lataa_lyojat():
    try:
        conn = sqlite3.connect(DB_POLKU)
        df = pd.read_sql_query(
            "SELECT Batter_ID, wOBA_All, wOBA_vs_L, wOBA_vs_R FROM lyojat_statcast",
            conn,
        )
        conn.close()
        df["Batter_ID"] = df["Batter_ID"].astype(int)
        return df.set_index("Batter_ID")
    except Exception:
        return pd.DataFrame()


# Alustus
tiimit           = lataa_tiimit()
optiot_syottajat = lataa_syottajat()
bp_dict          = lataa_bullpenit()
rosterit         = lataa_rosterit()
df_lyojat        = lataa_lyojat()

if not tiimit or not optiot_syottajat or not rosterit:
    st.error("Dataa puuttuu! Varmista, että tietokanta ja JSON-tiedosto ovat olemassa.")
    st.stop()

# ────────────────────────────────────────────────────────────────────────────
# GLOBAALI PELAAJAREKISTERI (Uusi v9.2)
# ────────────────────────────────────────────────────────────────────────────
# Rakennetaan yksi iso sanakirja (Nimi -> ID), johon on kerätty kaikki pelaajat
# kaikista joukkueista. Näin wOBA-matematiikka löytää oikean ID:n, vaikka
# pelaaja olisi juuri kaupattu toiseen tiimiin.

kaikki_lyojat_id = {}
for t_lyh, pelaajat in rosterit.items():
    for p in pelaajat:
        kaikki_lyojat_id[p["name"]] = p["id"]

# Aakkosellinen lista puhtaita nimiä alasvetovalikkoihin
kaikki_lyojat_nimet = sorted(list(kaikki_lyojat_id.keys()))


# ────────────────────────────────────────────────────────────────────────────
# APUFUNKTIOT
# ────────────────────────────────────────────────────────────────────────────

def pura_joukkue(valinta: str) -> tuple[str, str]:
    osat = valinta.split(" (")
    return osat[0], osat[1].replace(")", "") if len(osat) > 1 else osat[0]


def laske_joukkueen_woba(yh_nimet: list, pe_nimet: list, vastus_sp_kasisyys: str) -> float:
    """
    Laskee joukkueen yhdistetyn wOBA:n.
    HUOM: Etsii ID:t globaalista kaikki_lyojat_id -sanakirjasta!
    """
    split = (
        "wOBA_All" if vastus_sp_kasisyys == "All"
        else f"wOBA_vs_{vastus_sp_kasisyys}"
    )

    def hae_arvot(nimet):
        lst = []
        for puhtaanimi in nimet:
            pid = kaikki_lyojat_id.get(puhtaanimi)
            if pid and not df_lyojat.empty and pid in df_lyojat.index:
                v = df_lyojat.loc[pid].get(
                    split, df_lyojat.loc[pid].get("wOBA_All")
                )
                lst.append(float(v) if pd.notna(v) else LIIGA_WOBA_KA)
            else:
                lst.append(LIIGA_WOBA_KA)
        return lst

    yh_lst = hae_arvot(yh_nimet)
    pe_lst = hae_arvot(pe_nimet)
    yh_ka  = sum(yh_lst) / len(yh_lst) if yh_lst else LIIGA_WOBA_KA
    pe_ka  = sum(pe_lst) / len(pe_lst) if pe_lst else LIIGA_WOBA_KA
    return round((yh_ka * 0.90) + (pe_ka * 0.10), 3)


def hae_kaikki_syottajat() -> list[str]:
    kaikki = list(optiot_syottajat.keys())
    return kaikki if kaikki else ["(Ei syöttäjiä)"]


# ── Tallennusfunktiot ────────────────────────────────────────────────────────

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
        if col not in df.columns:
            df[col] = ""
    for col in CSV_SARAKKEET:
        if col not in df.columns:
            df[col] = ""
    return df[CSV_SARAKKEET]

def tallenna_seuranta_df(df: pd.DataFrame):
    df.to_csv(CSV_POLKU, index=False, encoding="utf-8")


# ────────────────────────────────────────────────────────────────────────────
# VÄLILEHDET
# ────────────────────────────────────────────────────────────────────────────

tab_analyysi, tab_seuranta = st.tabs(["⚾ Uusi Analyysi", "📂 Seuranta"])

# ============================================================================
# VÄLILEHTI 1 – UUSI ANALYYSI
# ============================================================================

with tab_analyysi:

    c1, c2, c3 = st.columns([10, 1, 10])

    with c1:
        st.markdown("### 🏠 KOTIJOUKKUE")
        koti_valinta  = st.selectbox("Joukkue", tiimit, index=0, key="k_team")
        koti_koko, koti_lyh = pura_joukkue(koti_valinta)
        koti_sp_nimi  = st.selectbox(
            "Aloitussyöttäjä", hae_kaikki_syottajat(), key="k_sp"
        )

        st.markdown("<br><b>Kotijoukkueen Lyöjät:</b>", unsafe_allow_html=True)
        # Haetaan valitun joukkueen oletuslyöjät esitäyttöä varten
        koti_oletus_nimet = [p['name'] for p in rosterit.get(koti_lyh, [])]
        
        koti_yh = st.multiselect(
            "Aloittava Yhdeksikkö (9)", kaikki_lyojat_nimet,
            default=koti_oletus_nimet[: min(9, len(koti_oletus_nimet))], key="k_yh",
        )
        
        koti_pe_opt = [n for n in kaikki_lyojat_nimet if n not in koti_yh]
        koti_pe_oletus = [n for n in koti_oletus_nimet if n not in koti_yh]
        
        koti_pe = st.multiselect(
            "Penkki (10% paino)", koti_pe_opt,
            default=koti_pe_oletus[: min(4, len(koti_pe_oletus))], key="k_pe",
        )

    with c3:
        st.markdown("### ✈️ VIERASJOUKKUE")
        vieras_valinta = st.selectbox(
            "Joukkue", tiimit,
            index=1 if len(tiimit) > 1 else 0, key="v_team",
        )
        vieras_koko, vieras_lyh = pura_joukkue(vieras_valinta)
        vieras_sp_nimi = st.selectbox(
            "Aloitussyöttäjä", hae_kaikki_syottajat(), key="v_sp"
        )

        st.markdown("<br><b>Vierasjoukkueen Lyöjät:</b>", unsafe_allow_html=True)
        vieras_oletus_nimet = [p['name'] for p in rosterit.get(vieras_lyh, [])]
        
        vieras_yh = st.multiselect(
            "Aloittava Yhdeksikkö (9)", kaikki_lyojat_nimet,
            default=vieras_oletus_nimet[: min(9, len(vieras_oletus_nimet))], key="v_yh",
        )
        
        vieras_pe_opt = [n for n in kaikki_lyojat_nimet if n not in vieras_yh]
        vieras_pe_oletus = [n for n in vieras_oletus_nimet if n not in vieras_yh]
        
        vieras_pe = st.multiselect(
            "Penkki (10% paino)", vieras_pe_opt,
            default=vieras_pe_oletus[: min(4, len(vieras_pe_oletus))], key="v_pe",
        )

    # ── SÄÄ- JA STADIONOSIO ─────────────────────────────────────
    st.markdown('<hr class="divider">', unsafe_allow_html=True)

    stadion_info = STADION_DATA.get(
        koti_lyh,
        {"Stadion": "Tuntematon", "PF": 1.00, "Dome": False},
    )
    on_dome = stadion_info.get("Dome", False)
    pf_val  = stadion_info.get("PF", 1.00)

    dome_badge = (
        '<span class="dome-badge">🔒 Dome (Katto)</span>'
        if on_dome
        else '<span class="outdoor-badge">🌿 Ulkoilma</span>'
    )

    st.markdown(
        f"<div class='weather-box'>"
        f"<div class='weather-title'>🏟️ STADION &amp; OLOSUHTEET</div>"
        f"<div class='stadion-info'>"
        f"  🏟️ Stadion: <b>{stadion_info['Stadion']}</b>"
        f"  {dome_badge}"
        f"  &nbsp;&nbsp;|&nbsp;&nbsp; Park Factor: <b>{pf_val:.2f}</b>"
        f"</div>",
        unsafe_allow_html=True,
    )

    saa_c1, saa_c2, saa_c3 = st.columns(3)

    with saa_c1:
        lampotila = st.number_input(
            "🌡️ Lämpötila (°C)",
            min_value=-10, max_value=45, value=20, step=1,
            key="lampotila",
            disabled=on_dome,
        )

    with saa_c2:
        tuuli_ms = st.number_input(
            "💨 Tuuli (m/s)",
            min_value=0.0, max_value=30.0, value=0.0, step=0.5,
            key="tuuli_ms",
            disabled=on_dome,
        )

    with saa_c3:
        tuuli_suunta = st.selectbox(
            "🧭 Tuulen suunta",
            TUULI_OPTIOT,
            index=0,
            key="tuuli_suunta",
            disabled=on_dome,
        )

    if on_dome:
        st.markdown(
            "<p class='warn'>🔒 Dome-stadion – sää ei vaikuta peliin, kentät poistettu käytöstä.</p>",
            unsafe_allow_html=True,
        )

    st.markdown("</div>", unsafe_allow_html=True)

    # ── Laske-nappi ──────────────────────────────────────────────────────────
    if st.button("⚡ LASKE TODENNÄKÖISYYS"):
        st.session_state["saved_inputs"] = {
            "koti_koko":       koti_koko,
            "koti_lyh":        koti_lyh,
            "koti_sp_nimi":    koti_sp_nimi,
            "koti_yh":         koti_yh,
            "koti_pe":         koti_pe,
            "vieras_koko":     vieras_koko,
            "vieras_lyh":      vieras_lyh,
            "vieras_sp_nimi":  vieras_sp_nimi,
            "vieras_yh":       vieras_yh,
            "vieras_pe":       vieras_pe,
            "lampotila_c":     20 if on_dome else int(lampotila),
            "tuuli_ms":        0.0 if on_dome else float(tuuli_ms),
            "tuuli_suunta":    "Sivutuuli / Tyyni" if on_dome else tuuli_suunta,
        }
        if "tallennettu_viesti" in st.session_state:
            del st.session_state["tallennettu_viesti"]

    # ── Tulosten piirto ────────
    if "saved_inputs" in st.session_state:
        inp = st.session_state["saved_inputs"]

        koti_sp_data   = optiot_syottajat[inp["koti_sp_nimi"]]
        vieras_sp_data = optiot_syottajat[inp["vieras_sp_nimi"]]

        koti_bp_data   = bp_dict.get(inp["koti_lyh"],   {"All": 3.80, "vs_L": 3.80, "vs_R": 3.80})
        vieras_bp_data = bp_dict.get(inp["vieras_lyh"], {"All": 3.80, "vs_L": 3.80, "vs_R": 3.80})

        koti_sp_arm   = koti_sp_data.get("Katisyys", "R")
        vieras_sp_arm = vieras_sp_data.get("Katisyys", "R")

        koti_woba_sp   = laske_joukkueen_woba(inp["koti_yh"], inp["koti_pe"], vieras_sp_arm)
        vieras_woba_sp = laske_joukkueen_woba(inp["vieras_yh"], inp["vieras_pe"], koti_sp_arm)

        koti_woba_bp   = laske_joukkueen_woba(inp["koti_yh"], inp["koti_pe"], "All")
        vieras_woba_bp = laske_joukkueen_woba(inp["vieras_yh"], inp["vieras_pe"], "All")

        tulos = laske_todennakoisyys(
            inp["koti_koko"], inp["vieras_koko"],
            koti_sp=koti_sp_data,
            koti_bp=koti_bp_data,
            koti_woba=koti_woba_sp,
            vieras_sp=vieras_sp_data,
            vieras_bp=vieras_bp_data,
            vieras_woba=vieras_woba_sp,
            koti_woba_bp=koti_woba_bp,
            vieras_woba_bp=vieras_woba_bp,
            lampotila_c=inp["lampotila_c"],
            tuuli_ms=inp["tuuli_ms"],
            tuuli_suunta=inp["tuuli_suunta"],
            koti_lyh=inp["koti_lyh"],
        )

        k_pct  = tulos["koti_voitto_tod"]   * 100
        v_pct  = tulos["vieras_voitto_tod"] * 100
        k_odds = 1 / tulos["koti_voitto_tod"]
        v_odds = 1 / tulos["vieras_voitto_tod"]

        st.session_state["viimeisin_tulos"] = {
            "Pvm":            str(date.today()),
            "Koti":           inp["koti_koko"],
            "Vieras":         inp["vieras_koko"],
            "Koti %":         f"{k_pct:.1f}",
            "Vieras %":       f"{v_pct:.1f}",
            "Koti kerroin":   f"{k_odds:.2f}",
            "Vieras kerroin": f"{v_odds:.2f}",
            "O/U odotus":     f"{tulos['total_odotus']:.1f}",
            "Koti odotus":    f"{tulos['k_odotus']:.1f}",
            "Vieras odotus":  f"{tulos['v_odotus']:.1f}",
            "Koti tulos":     "",
            "Vieras tulos":   "",
        }

        # ── Tulosruudut ──────────────────────────────────────────────────────
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

        # ── O/U -kortti ───────────────────────────────────────────────────────
        st.markdown('<hr class="divider">', unsafe_allow_html=True)
        st.markdown(
            f'<div class="ou-card">'
            f'<div style="color:#5a7a50;letter-spacing:0.2em;">JUOKSUODOTTAMA (O/U)</div>'
            f'<div class="ou-runs">{tulos["total_odotus"]:.1f}</div>'
            f'<div>{inp["koti_lyh"]}: {tulos["k_odotus"]:.1f} &nbsp;|&nbsp; '
            f'{inp["vieras_lyh"]}: {tulos["v_odotus"]:.1f}</div></div>',
            unsafe_allow_html=True,
        )

        # ── LISÄTIEDOT-KORTIT ─────────────────────────────────────────────────
        st.markdown('<hr class="divider">', unsafe_allow_html=True)
        st.markdown(
            "<p style='font-family:Bebas Neue,sans-serif;font-size:1.4rem;"
            "color:#7a6e5f;letter-spacing:0.15em;margin-bottom:0.5rem;'>"
            "LISÄTIEDOT</p>",
            unsafe_allow_html=True,
        )

        det_c1, det_c2, det_c3 = st.columns([10, 1, 10])

        def detail_card_html(
            joukkue: str,
            sp_xfip: float,
            bp_xfip: float,
            woba: float,
            sp_arm: str,
            stadion_nimi: str | None = None,
        ) -> str:
            stadion_rivi = ""
            if stadion_nimi is not None:
                stadion_rivi = (
                    f"<div class='detail-row'>"
                    f"<span class='detail-key'>Stadion</span>"
                    f"<span class='detail-val'>{stadion_nimi}</span>"
                    f"</div>"
                )
            return (
                f"<div class='detail-card'>"
                f"<div class='detail-title'>{joukkue}</div>"
                f"<div class='detail-row'>"
                f"  <span class='detail-key'>Aloittajan xFIP</span>"
                f"  <span class='detail-val green'>{sp_xfip:.2f}</span>"
                f"</div>"
                f"<div class='detail-row'>"
                f"  <span class='detail-key'>Bullpen xFIP</span>"
                f"  <span class='detail-val green'>{bp_xfip:.2f}</span>"
                f"</div>"
                f"<div class='detail-row'>"
                f"  <span class='detail-key'>Hyökkäys wOBA"
                f"    <span style='font-size:0.78em;color:#5a5450;'>"
                f"      &nbsp;(vs {sp_arm}HP)</span></span>"
                f"  <span class='detail-val gold'>{woba:.3f}</span>"
                f"</div>"
                f"{stadion_rivi}"
                f"</div>"
            )

        stadion_nimi_tulos = tulos.get(
            "stadion_nimi",
            STADION_DATA.get(inp["koti_lyh"], {}).get("Stadion", "Tuntematon"),
        )

        with det_c1:
            st.markdown(
                detail_card_html(
                    inp["koti_koko"],
                    tulos["koti_sp_dyn"],
                    tulos["koti_bp_dyn"],
                    koti_woba_sp,
                    vieras_sp_arm,
                    stadion_nimi=stadion_nimi_tulos,
                ),
                unsafe_allow_html=True,
            )

        with det_c3:
            st.markdown(
                detail_card_html(
                    inp["vieras_koko"],
                    tulos["vieras_sp_dyn"],
                    tulos["vieras_bp_dyn"],
                    vieras_woba_sp,
                    koti_sp_arm,
                    stadion_nimi=None,
                ),
                unsafe_allow_html=True,
            )

        # ── TALLENNA SEURANTAAN -nappi ────────────────────────────────────────
        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
        _, save_col, _ = st.columns([3, 4, 3])
        with save_col:
            st.markdown("<div class='save-btn'>", unsafe_allow_html=True)
            if st.button("💾 Tallenna seurantaan", key="tallenna_btn"):
                rivi = st.session_state.get("viimeisin_tulos")
                if rivi:
                    tallenna_peli(rivi)
                    st.session_state["tallennettu_viesti"] = (
                        f"✅ Tallennettu: {rivi['Koti']} vs {rivi['Vieras']} ({rivi['Pvm']})"
                    )
            st.markdown("</div>", unsafe_allow_html=True)

            if "tallennettu_viesti" in st.session_state:
                st.success(st.session_state["tallennettu_viesti"])

# ============================================================================
# VÄLILEHTI 2 – SEURANTA
# ============================================================================

with tab_seuranta:
    st.markdown(
        "<h3 style='font-family:Bebas Neue,sans-serif;letter-spacing:0.12em;"
        "color:#c8a84b;'>📂 TALLENNETUT PELIT</h3>",
        unsafe_allow_html=True,
    )

    varmista_csv()
    df_seuranta = lataa_seuranta_df()

    if df_seuranta.empty:
        st.info(
            "Ei tallennettuja pelejä. Analysoi ottelu Uusi Analyysi -välilehdellä "
            "ja paina '💾 Tallenna seurantaan'."
        )
    else:
        st.markdown(
            f"<p style='color:#7a6e5f;font-size:0.85rem;'>"
            f"{len(df_seuranta)} ottelua tallennettu. "
            "Syötä lopputulokset suoraan taulukkoon ja muutokset tallentuvat "
            "automaattisesti.</p>",
            unsafe_allow_html=True,
        )

        muokattu = st.data_editor(
            df_seuranta,
            width="stretch",
            num_rows="dynamic",
            column_config={
                "Pvm":            st.column_config.TextColumn("Pvm",         width="small"),
                "Koti":           st.column_config.TextColumn("Koti",        width="medium"),
                "Vieras":         st.column_config.TextColumn("Vieras",      width="medium"),
                "Koti %":         st.column_config.TextColumn("Koti %",      width="small"),
                "Vieras %":       st.column_config.TextColumn("Vieras %",    width="small"),
                "Koti kerroin":   st.column_config.TextColumn("Koti k.",     width="small"),
                "Vieras kerroin": st.column_config.TextColumn("Vieras k.",   width="small"),
                "O/U odotus":     st.column_config.TextColumn("O/U",         width="small"),
                "Koti odotus":    st.column_config.TextColumn("Koti O",      width="small"),
                "Vieras odotus":  st.column_config.TextColumn("Vieras O",    width="small"),
                "Koti tulos":     st.column_config.TextColumn("✏ Koti R",   width="small"),
                "Vieras tulos":   st.column_config.TextColumn("✏ Vieras R", width="small"),
            },
            key="seuranta_editor",
        )

        df_seuranta = df_seuranta.fillna("")
        muokattu    = muokattu.fillna("")

        if not muokattu.equals(df_seuranta):
            tallenna_seuranta_df(muokattu)
            st.success("✅ Muutokset tallennettu.")
            st.rerun()