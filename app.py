"""
app.py  –  MLB Vedonlyönti-UI
=============================
Streamlit-käyttöliittymä laskentamoottori.py:lle.

Käynnistys:
    streamlit run app.py
"""

import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

# Importataan oma laskentamoottori (oltava samassa hakemistossa)
from laskentamoottori import laske_todennakoisyys, lataa_data, DB_POLKU, TAULU

# ── Sivun perusasetukset ───────────────────────────────────────────────────
st.set_page_config(
    page_title="MLB Odds Engine",
    page_icon="⚾",
    layout="centered",
)

# ── Custom CSS – tumma, lehtori-/sanomalehti-tyylinen ulkoasu ─────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Source+Serif+4:ital,wght@0,300;0,600;1,300&display=swap');

    /* Taustaväri ja perusfontti */
    html, body, [data-testid="stAppViewContainer"] {
        background-color: #0d0d0d;
        color: #e8e0d0;
    }
    [data-testid="stHeader"] { background: transparent; }
    [data-testid="stSidebar"] { display: none; }

    /* Otsikot */
    h1, h2, h3 {
        font-family: 'Bebas Neue', sans-serif;
        letter-spacing: 0.06em;
    }

    /* Pääotsikko */
    .main-title {
        font-family: 'Bebas Neue', sans-serif;
        font-size: clamp(2.8rem, 6vw, 5rem);
        letter-spacing: 0.1em;
        color: #f0e6c8;
        text-align: center;
        line-height: 1;
        margin-bottom: 0;
    }
    .main-subtitle {
        font-family: 'Source Serif 4', serif;
        font-style: italic;
        font-weight: 300;
        font-size: 1rem;
        color: #7a6e5f;
        text-align: center;
        letter-spacing: 0.15em;
        margin-top: 0.3rem;
        margin-bottom: 2.5rem;
    }

    /* Erotusviiva */
    .divider {
        border: none;
        border-top: 1px solid #2e2a24;
        margin: 1.8rem 0;
    }

    /* Selectbox + label */
    label[data-testid="stWidgetLabel"] p {
        font-family: 'Bebas Neue', sans-serif;
        font-size: 0.85rem;
        letter-spacing: 0.18em;
        color: #7a6e5f;
        margin-bottom: 0.2rem;
    }
    [data-testid="stSelectbox"] > div > div {
        background-color: #1a1814 !important;
        border: 1px solid #2e2a24 !important;
        border-radius: 4px !important;
        color: #e8e0d0 !important;
        font-family: 'Source Serif 4', serif !important;
    }

    /* Painike */
    div.stButton > button {
        width: 100%;
        background-color: #c8a84b;
        color: #0d0d0d;
        font-family: 'Bebas Neue', sans-serif;
        font-size: 1.35rem;
        letter-spacing: 0.2em;
        border: none;
        border-radius: 4px;
        padding: 0.7rem 1rem;
        margin-top: 1.2rem;
        transition: background-color 0.2s ease, transform 0.1s ease;
        cursor: pointer;
    }
    div.stButton > button:hover {
        background-color: #e0bf60;
        transform: translateY(-1px);
    }
    div.stButton > button:active {
        transform: translateY(0);
    }

    /* Tuloskortti */
    .result-card {
        background: #141210;
        border: 1px solid #2e2a24;
        border-radius: 6px;
        padding: 1.4rem 1.6rem;
        text-align: center;
    }
    .result-team {
        font-family: 'Bebas Neue', sans-serif;
        font-size: 1.55rem;
        letter-spacing: 0.08em;
        color: #e8e0d0;
        margin-bottom: 0.15rem;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .result-role {
        font-family: 'Source Serif 4', serif;
        font-size: 0.7rem;
        font-style: italic;
        color: #5a5040;
        letter-spacing: 0.12em;
        margin-bottom: 1.1rem;
    }
    .result-pct {
        font-family: 'Bebas Neue', sans-serif;
        font-size: 3.8rem;
        letter-spacing: 0.04em;
        line-height: 1;
        margin-bottom: 0.1rem;
    }
    .result-pct-label {
        font-family: 'Source Serif 4', serif;
        font-size: 0.7rem;
        letter-spacing: 0.15em;
        color: #7a6e5f;
        text-transform: uppercase;
        margin-bottom: 1.1rem;
    }
    .result-odds {
        font-family: 'Source Serif 4', serif;
        font-size: 1.55rem;
        font-weight: 600;
        color: #c8a84b;
    }
    .result-odds-label {
        font-family: 'Source Serif 4', serif;
        font-size: 0.68rem;
        letter-spacing: 0.12em;
        color: #5a5040;
        text-transform: uppercase;
    }
    .vs-badge {
        font-family: 'Bebas Neue', sans-serif;
        font-size: 2.2rem;
        color: #2e2a24;
        text-align: center;
        line-height: 1;
        padding-top: 1.2rem;
    }

    /* Progresspalkki-tyyli override */
    [data-testid="stProgress"] > div > div > div {
        background-color: #c8a84b !important;
    }
    [data-testid="stProgress"] > div > div {
        background-color: #1a1814 !important;
        border-radius: 2px !important;
    }

    /* H2H-info */
    .info-box {
        background: #141210;
        border: 1px solid #2e2a24;
        border-left: 3px solid #c8a84b;
        border-radius: 4px;
        padding: 0.9rem 1.2rem;
        margin-top: 1.5rem;
        font-family: 'Source Serif 4', serif;
        font-size: 0.85rem;
        color: #7a6e5f;
        font-style: italic;
    }
    .info-box b {
        color: #c8a84b;
        font-style: normal;
        font-weight: 600;
    }

    /* Varoitus: sama joukkue */
    .warn-box {
        background: #1a1208;
        border: 1px solid #c8a84b44;
        border-radius: 4px;
        padding: 0.8rem 1.2rem;
        text-align: center;
        font-family: 'Source Serif 4', serif;
        font-size: 0.88rem;
        color: #c8a84b99;
        margin-top: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Otsikko ───────────────────────────────────────────────────────────────
st.markdown('<p class="main-title">⚾ MLB Odds Engine</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="main-subtitle">True Odds · Probability Model · MVP v1.0</p>',
    unsafe_allow_html=True,
)
st.markdown('<hr class="divider">', unsafe_allow_html=True)


# ── Joukkueiden haku tietokannasta ────────────────────────────────────────
@st.cache_data(show_spinner=False)
def hae_joukkueet() -> list[str]:
    """Hakee uniikit joukkuenimet tietokannasta (koti + vieras)."""
    if not Path(DB_POLKU).exists():
        return []
    yhteys = sqlite3.connect(DB_POLKU)
    try:
        df = pd.read_sql_query(
            f"SELECT DISTINCT Kotijoukkue AS j FROM {TAULU} "
            f"UNION SELECT DISTINCT Vierasjoukkue FROM {TAULU} "
            f"ORDER BY j",
            yhteys,
        )
    finally:
        yhteys.close()
    return df["j"].tolist()


@st.cache_data(show_spinner=False)
def hae_data() -> pd.DataFrame:
    return lataa_data()


joukkueet = hae_joukkueet()

if not joukkueet:
    st.error(
        "⚠️ Tietokantaa **mlb_historical.db** ei löydy tai se on tyhjä. "
        "Varmista, että tiedosto on samassa hakemistossa kuin app.py."
    )
    st.stop()


# ── Joukkueen valinta ─────────────────────────────────────────────────────
col_koti, col_vs, col_vieras = st.columns([10, 1, 10])

with col_koti:
    koti = st.selectbox("🏠  KOTIJOUKKUE", joukkueet, index=0)

with col_vs:
    st.markdown('<div class="vs-badge">VS</div>', unsafe_allow_html=True)

with col_vieras:
    # Asetetaan oletukseksi eri joukkue kuin koti
    vieras_default = 1 if len(joukkueet) > 1 else 0
    vieras = st.selectbox("✈️  VIERASJOUKKUE", joukkueet, index=vieras_default)

# ── Laske-painike ─────────────────────────────────────────────────────────
laske = st.button("LASKE TODENNÄKÖISYYS")

# ── Laskenta & tulosnäkymä ────────────────────────────────────────────────
if laske:
    if koti == vieras:
        st.markdown(
            '<div class="warn-box">⚠️  Valitse kaksi eri joukkuetta.</div>',
            unsafe_allow_html=True,
        )
    else:
        with st.spinner("Lasketaan..."):
            data = hae_data()
            tulos = laske_todennakoisyys(koti, vieras, df=data)

        koti_pct   = tulos["koti_voitto_tod"]   * 100
        vieras_pct = tulos["vieras_voitto_tod"] * 100
        koti_kerroin   = 1 / tulos["koti_voitto_tod"]
        vieras_kerroin = 1 / tulos["vieras_voitto_tod"]

        # Väri: suosikki saa kultaisen sävyn
        koti_color   = "#c8a84b" if koti_pct >= vieras_pct else "#e8e0d0"
        vieras_color = "#c8a84b" if vieras_pct > koti_pct  else "#e8e0d0"

        st.markdown('<hr class="divider">', unsafe_allow_html=True)

        # ── Tuloskortit ──
        c1, c2, c3 = st.columns([10, 1, 10])

        with c1:
            st.markdown(
                f"""
                <div class="result-card">
                    <div class="result-team">{koti}</div>
                    <div class="result-role">Kotijoukkue</div>
                    <div class="result-pct" style="color:{koti_color}">
                        {koti_pct:.1f}<span style="font-size:1.6rem">%</span>
                    </div>
                    <div class="result-pct-label">Voittotn.</div>
                    <div class="result-odds">{koti_kerroin:.2f}</div>
                    <div class="result-odds-label">True Odds</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with c2:
            st.markdown('<div class="vs-badge">–</div>', unsafe_allow_html=True)

        with c3:
            st.markdown(
                f"""
                <div class="result-card">
                    <div class="result-team">{vieras}</div>
                    <div class="result-role">Vierasjoukkue</div>
                    <div class="result-pct" style="color:{vieras_color}">
                        {vieras_pct:.1f}<span style="font-size:1.6rem">%</span>
                    </div>
                    <div class="result-pct-label">Voittotn.</div>
                    <div class="result-odds">{vieras_kerroin:.2f}</div>
                    <div class="result-odds-label">True Odds</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ── Progresspalkki ──
        st.markdown("<br>", unsafe_allow_html=True)
        st.caption(
            f"**{koti}**  {koti_pct:.1f} %  ←  Voittojakauma  →  "
            f"{vieras_pct:.1f} %  **{vieras}**"
        )
        st.progress(int(koti_pct))

        # ── Lisätiedot: h2h + yleinen vp ──
        h2h = tulos["h2h_ottelut"]
        h2h_teksti = (
            f"<b>{h2h} keskinäistä ottelua</b> löydetty datasta. "
            f"Kotijoukkueen H2H-voitto&#8209;% näissä: "
            f"<b>{tulos['h2h_koti_vp']*100:.1f} %</b>."
            if h2h > 0
            else "Keskinäisiä otteluita ei löydetty datasta — laskennassa käytetty neutraalia 50/50 H2H-arvoa."
        )
        st.markdown(
            f"""
            <div class="info-box">
                📊 &nbsp;Yleinen voitto&#8209;%:
                <b>{koti}</b> {tulos['koti_yleinen_vp']*100:.1f} % &nbsp;|&nbsp;
                <b>{vieras}</b> {tulos['vieras_yleinen_vp']*100:.1f} %
                &nbsp;&nbsp;·&nbsp;&nbsp;
                {h2h_teksti}
                <br><br>
                <span style="font-size:0.75rem">
                Malli: 70 % yleinen voitto&#8209;% + 30 % head&#8209;to&#8209;head
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )