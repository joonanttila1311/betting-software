"""
sovellus.py  –  MLB Vedonlyönti-UI  v2.0
=====================================
Streamlit-käyttöliittymä laskentamoottori.py:lle.
Tukee nyt aloitussyöttäjän valintaa ja ERA-pohjaista laskentaa.

Käynnistys:
    streamlit run sovellus.py
"""

import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

from laskentamoottori import laske_todennakoisyys, lataa_data, DB_POLKU, TAULU

# ── Sivun perusasetukset ──────────────────────────────────────────────────
st.set_page_config(
    page_title="MLB Odds Engine",
    page_icon="⚾",
    layout="centered",
)

# ── Custom CSS ────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Source+Serif+4:ital,wght@0,300;0,600;1,300&display=swap');

    html, body, [data-testid="stAppViewContainer"] {
        background-color: #0d0d0d;
        color: #e8e0d0;
    }
    [data-testid="stHeader"] { background: transparent; }
    [data-testid="stSidebar"] { display: none; }

    h1, h2, h3 {
        font-family: 'Bebas Neue', sans-serif;
        letter-spacing: 0.06em;
    }

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

    .divider {
        border: none;
        border-top: 1px solid #2e2a24;
        margin: 1.8rem 0;
    }

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

    .era-badge {
        display: inline-block;
        background: #1e1a10;
        border: 1px solid #3a3020;
        border-radius: 3px;
        padding: 0.18rem 0.55rem;
        font-family: 'Source Serif 4', serif;
        font-size: 0.72rem;
        color: #c8a84b;
        letter-spacing: 0.06em;
        margin-bottom: 0.9rem;
    }

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
    div.stButton > button:active { transform: translateY(0); }

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
        margin-bottom: 0.5rem;
    }
    .result-pitcher {
        font-family: 'Source Serif 4', serif;
        font-size: 0.78rem;
        color: #9a8c70;
        margin-bottom: 0.4rem;
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

    [data-testid="stProgress"] > div > div > div {
        background-color: #c8a84b !important;
    }
    [data-testid="stProgress"] > div > div {
        background-color: #1a1814 !important;
        border-radius: 2px !important;
    }

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
    .info-box b { color: #c8a84b; font-style: normal; font-weight: 600; }

    .era-info-box {
        background: #0f0e08;
        border: 1px solid #3a3020;
        border-left: 3px solid #8a7030;
        border-radius: 4px;
        padding: 0.9rem 1.2rem;
        margin-top: 0.8rem;
        font-family: 'Source Serif 4', serif;
        font-size: 0.83rem;
        color: #6a5e40;
        font-style: italic;
    }
    .era-info-box b { color: #c8a84b; font-style: normal; }

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

    .no-pitcher {
        font-family: 'Source Serif 4', serif;
        font-size: 0.75rem;
        color: #3a3228;
        font-style: italic;
        margin-bottom: 0.9rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Otsikko ───────────────────────────────────────────────────────────────
st.markdown('<p class="main-title">⚾ MLB Odds Engine</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="main-subtitle">True Odds · Probability Model · MVP v2.0</p>',
    unsafe_allow_html=True,
)
st.markdown('<hr class="divider">', unsafe_allow_html=True)


# ── Datatietokantahaut ────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def hae_joukkueet() -> list[str]:
    """Hakee uniikit joukkuenimet ottelutulokset-taulusta."""
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
def hae_syottajat() -> pd.DataFrame:
    """
    Lataa syöttäjätilastot taulusta 'syottajat_2025'.
    Palauttaa tyhjän DataFramen jos taulua ei löydy.
    """
    if not Path(DB_POLKU).exists():
        return pd.DataFrame()
    yhteys = sqlite3.connect(DB_POLKU)
    try:
        df = pd.read_sql_query(
            "SELECT Name, Team, ERA, WHIP, GS, IP "
            "FROM syottajat_2025 ORDER BY Name",
            yhteys,
        )
    except Exception:
        df = pd.DataFrame()
    finally:
        yhteys.close()
    return df


@st.cache_data(show_spinner=False)
def hae_data() -> pd.DataFrame:
    return lataa_data()


# ── Apufunktiot syöttäjävalikkoja varten ─────────────────────────────────

def syottajat_joukkueelle(df_syottajat: pd.DataFrame, joukkue: str) -> list[str]:
    """
    Rakentaa listan selectbox-optioista muodossa 'Nimi  (ERA: X.XX)'
    suodatettuna joukkueen mukaan. Lisää alkuun 'ei valita' -vaihtoehdon.
    """
    if df_syottajat.empty:
        return ["— ei syöttäjädataa —"]
    suodatettu = df_syottajat[df_syottajat["Team"] == joukkue]
    if suodatettu.empty:
        return ["— ei syöttäjää tälle joukkueelle —"]
    optiot = [
        f"{r['Name']}  (ERA: {r['ERA']:.2f})"
        for _, r in suodatettu.iterrows()
    ]
    return ["— ei valita —"] + optiot


def era_valinnasta(valinta: str) -> float | None:
    """Parsii ERA-arvon merkkijonosta 'Nimi  (ERA: X.XX)'."""
    if not valinta or valinta.startswith("—"):
        return None
    try:
        return float(valinta.split("ERA:")[1].strip().rstrip(")"))
    except (IndexError, ValueError):
        return None


def nimi_valinnasta(valinta: str) -> str | None:
    """Parsii syöttäjän nimen merkkijonosta."""
    if not valinta or valinta.startswith("—"):
        return None
    return valinta.split("  (ERA:")[0].strip()


# ── Datan lataus ──────────────────────────────────────────────────────────
joukkueet    = hae_joukkueet()
df_syottajat = hae_syottajat()

if not joukkueet:
    st.error(
        "⚠️ Tietokantaa **mlb_historical.db** ei löydy tai se on tyhjä. "
        "Varmista, että tiedosto on samassa hakemistossa kuin app.py."
    )
    st.stop()

if df_syottajat.empty:
    st.markdown(
        '<div class="warn-box" style="margin-bottom:1.2rem">'
        '⚾ Syöttäjädata puuttuu – ajo <code>fetch_pitchers.py</code> '
        'lisää ERA-tuen laskentaan.'
        '</div>',
        unsafe_allow_html=True,
    )


# ── Joukkueen ja syöttäjän valinta ───────────────────────────────────────
col_koti, col_vs, col_vieras = st.columns([10, 1, 10])

with col_koti:
    koti = st.selectbox("🏠  KOTIJOUKKUE", joukkueet, index=0)
    koti_optiot = syottajat_joukkueelle(df_syottajat, koti)
    koti_valinta = st.selectbox(
        "⚾  ALOITUSSYÖTTÄJÄ (KOTI)", koti_optiot, key="koti_syottaja"
    )

with col_vs:
    st.markdown('<div class="vs-badge">VS</div>', unsafe_allow_html=True)

with col_vieras:
    vieras_default = 1 if len(joukkueet) > 1 else 0
    vieras = st.selectbox("✈️  VIERASJOUKKUE", joukkueet, index=vieras_default)
    vieras_optiot = syottajat_joukkueelle(df_syottajat, vieras)
    vieras_valinta = st.selectbox(
        "⚾  ALOITUSSYÖTTÄJÄ (VIERAS)", vieras_optiot, key="vieras_syottaja"
    )

# Parsitaan ERA ja nimet valinnoista
koti_era       = era_valinnasta(koti_valinta)
vieras_era     = era_valinnasta(vieras_valinta)
koti_pitcher   = nimi_valinnasta(koti_valinta)
vieras_pitcher = nimi_valinnasta(vieras_valinta)

# ── Laske-painike ─────────────────────────────────────────────────────────
laske = st.button("LASKE TODENNÄKÖISYYS")

# ── Laskenta & tulokset ───────────────────────────────────────────────────
if laske:
    if koti == vieras:
        st.markdown(
            '<div class="warn-box">⚠️  Valitse kaksi eri joukkuetta.</div>',
            unsafe_allow_html=True,
        )
    else:
        with st.spinner("Lasketaan..."):
            data  = hae_data()
            tulos = laske_todennakoisyys(
                koti,
                vieras,
                df=data,
                koti_era=koti_era,
                vieras_era=vieras_era,
            )

        koti_pct       = tulos["koti_voitto_tod"]   * 100
        vieras_pct     = tulos["vieras_voitto_tod"] * 100
        koti_kerroin   = 1 / tulos["koti_voitto_tod"]
        vieras_kerroin = 1 / tulos["vieras_voitto_tod"]
        era_mukana     = tulos.get("era_kaytossa", False)

        koti_color   = "#c8a84b" if koti_pct >= vieras_pct else "#e8e0d0"
        vieras_color = "#c8a84b" if vieras_pct > koti_pct  else "#e8e0d0"

        def pitcher_html(pitcher: str | None, era: float | None) -> str:
            """Rakentaa syöttäjärivin HTML:n tuloskorttiin."""
            if pitcher and era is not None:
                return (
                    f'<div class="result-pitcher">&#9918; {pitcher}</div>'
                    f'<div class="era-badge">ERA &nbsp; {era:.2f}</div>'
                )
            return '<div class="no-pitcher">Syöttäjää ei valittu</div>'

        st.markdown('<hr class="divider">', unsafe_allow_html=True)

        # ── Tuloskortit ──
        c1, c2, c3 = st.columns([10, 1, 10])

        with c1:
            st.markdown(
                f"""
                <div class="result-card">
                    <div class="result-team">{koti}</div>
                    <div class="result-role">Kotijoukkue</div>
                    {pitcher_html(koti_pitcher, koti_era)}
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
                    {pitcher_html(vieras_pitcher, vieras_era)}
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

        # ── Tilasto-infolaatikko ──
        h2h = tulos["h2h_ottelut"]
        h2h_teksti = (
            f"<b>{h2h} keskinäistä ottelua</b> löydetty. "
            f"Kotivoitto&#8209;% H2H: <b>{tulos['h2h_koti_vp']*100:.1f} %</b>."
            if h2h > 0
            else "Keskinäisiä otteluita ei löydetty — käytetty neutraalia 50/50."
        )
        malli = (
            "60 % yleinen VP + 20 % H2H + 20 % ERA&#8209;vertailu"
            if era_mukana
            else "70 % yleinen VP + 30 % H2H"
        )

        st.markdown(
            f"""
            <div class="info-box">
                &#128202; &nbsp;Yleinen voitto&#8209;%:
                <b>{koti}</b> {tulos['koti_yleinen_vp']*100:.1f} % &nbsp;|&nbsp;
                <b>{vieras}</b> {tulos['vieras_yleinen_vp']*100:.1f} %
                &nbsp;&nbsp;&#183;&nbsp;&nbsp;{h2h_teksti}
                <br><br>
                <span style="font-size:0.75rem">Malli: {malli}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # ── ERA-vertailulohko ──
        if era_mukana:
            parempi = (
                koti_pitcher if (koti_era or 99) < (vieras_era or 99)
                else vieras_pitcher
            )
            st.markdown(
                f"""
                <div class="era-info-box">
                    &#9918; ERA-analyysi: &nbsp;
                    <b>{koti_pitcher}</b> {koti_era:.2f}
                    &nbsp; vs &nbsp;
                    <b>{vieras_pitcher}</b> {vieras_era:.2f}
                    &nbsp; &#183; &nbsp;
                    Parempi syöttäjä: <b>{parempi}</b>
                </div>
                """,
                unsafe_allow_html=True,
            )
        elif koti_pitcher or vieras_pitcher:
            st.markdown(
                '<div class="era-info-box">'
                '&#128161; Valitse <b>molempien</b> joukkueiden syöttäjä, '
                'jotta ERA otetaan mukaan laskentaan.'
                '</div>',
                unsafe_allow_html=True,
            )