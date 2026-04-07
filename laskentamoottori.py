"""
laskentamoottori.py – v5.0 (Dynamic wOBA & IP/GS Weighting)
==========================================================
Tämä on mallin aivot. Laskee voittotodennäköisyydet ja juoksuodotukset.
Sisältää 1% Momentum- ja H2H-painotuksen
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import date

DB_POLKU = "mlb_historical.db"
LIIGA_XFIP_KA = 3.80
LIIGA_WOBA_KA = 0.310

# ---------------------------------------------------------------------------
# STADIONIT JA SÄÄ (Park Factors 2026 & Weather Modifiers)
# PF > 1.00 suosii lyöjiä (Over), PF < 1.00 suosii syöttäjiä (Under).
# Dome = True tarkoittaa, että säällä ei ole vaikutusta.
# ---------------------------------------------------------------------------
STADION_DATA = {
    "COL": {"Stadion": "Coors Field", "PF": 1.14, "Dome": False},
    "CIN": {"Stadion": "Great American Ball Park", "PF": 1.06, "Dome": False},
    "BOS": {"Stadion": "Fenway Park", "PF": 1.05, "Dome": False},
    "CWS": {"Stadion": "Guaranteed Rate Field", "PF": 1.02, "Dome": False},
    "LAA": {"Stadion": "Angel Stadium", "PF": 1.02, "Dome": False},
    "NYY": {"Stadion": "Yankee Stadium", "PF": 1.01, "Dome": False},
    "CHC": {"Stadion": "Wrigley Field", "PF": 1.01, "Dome": False},
    "ATL": {"Stadion": "Truist Park", "PF": 1.01, "Dome": False},
    "PHI": {"Stadion": "Citizens Bank Park", "PF": 1.01, "Dome": False},
    "HOU": {"Stadion": "Minute Maid Park", "PF": 1.01, "Dome": True},
    "LAD": {"Stadion": "Dodger Stadium", "PF": 1.00, "Dome": False},
    "WSH": {"Stadion": "Nationals Park", "PF": 1.00, "Dome": False},
    "MIN": {"Stadion": "Target Field", "PF": 1.00, "Dome": False},
    "TEX": {"Stadion": "Globe Life Field", "PF": 1.00, "Dome": True},
    "TOR": {"Stadion": "Rogers Centre", "PF": 1.00, "Dome": True},
    "AZ":  {"Stadion": "Chase Field", "PF": 1.00, "Dome": True},
    "KC":  {"Stadion": "Kauffman Stadium", "PF": 0.99, "Dome": False},
    "CLE": {"Stadion": "Progressive Field", "PF": 0.99, "Dome": False},
    "MIL": {"Stadion": "American Family Field", "PF": 0.99, "Dome": True},
    "BAL": {"Stadion": "Oriole Park", "PF": 0.98, "Dome": False},
    "TB":  {"Stadion": "Tropicana Field", "PF": 0.98, "Dome": True},
    "PIT": {"Stadion": "PNC Park", "PF": 0.98, "Dome": False},
    "DET": {"Stadion": "Comerica Park", "PF": 0.97, "Dome": False},
    "NYM": {"Stadion": "Citi Field", "PF": 0.97, "Dome": False},
    "MIA": {"Stadion": "loanDepot park", "PF": 0.97, "Dome": True},
    "SD":  {"Stadion": "Petco Park", "PF": 0.96, "Dome": False},
    "STL": {"Stadion": "Busch Stadium", "PF": 0.96, "Dome": False},
    "SF":  {"Stadion": "Oracle Park", "PF": 0.95, "Dome": False},
    "OAK": {"Stadion": "Oakland Coliseum", "PF": 0.95, "Dome": False},
    "ATH": {"Stadion": "Sutter Health Park", "PF": 0.95, "Dome": False}, # Athletics 2026
    "SEA": {"Stadion": "T-Mobile Park", "PF": 0.93, "Dome": False},
}

def laske_saa_kerroin(lampotila_c: int, tuuli_ms: int, tuuli_suunta: str, is_dome: bool) -> float:
    """
    Laskee Vegas-mallin mukaisen sääkertoimen.
    Jos stadionilla on katto (Dome), palautetaan aina neutraali 1.00.
    """
    if is_dome:
        return 1.00

    kerroin = 1.00

    # 1. Lämpötilan vaikutus (Perus 20 °C)
    if lampotila_c >= 35:
        kerroin += 0.075
    elif lampotila_c >= 30:
        kerroin += 0.050
    elif lampotila_c >= 25:
        kerroin += 0.025
    elif lampotila_c < 10:
        kerroin -= 0.050
    elif lampotila_c < 15:
        kerroin -= 0.025

    # 2. Tuulen vaikutus
    if tuuli_ms >= 3 and tuuli_suunta != "Sivutuuli / Tyyni":
        voimakkuus = 0.10 if tuuli_ms >= 6 else 0.05
        
        if tuuli_suunta == "Ulos katsomoon":
            kerroin += voimakkuus
        elif tuuli_suunta == "Sisään pesälle":
            kerroin -= voimakkuus

    return kerroin


def hae_momentum(koti_nimi, vieras_nimi):
    """
    Lukee ottelutulokset-taulusta joukkueiden historian.
    Laskee H2H-edun ja Kuntopuntari-edun (viimeiset 10 peliä).
    Palauttaa momentum-edun (Edge), joka on maksimissaan n. 0.015 (n. 1.5%).
    """
    vuosi = date.today().year
    taulu = f"ottelutulokset_{vuosi}"
    
    try:
        conn = sqlite3.connect(DB_POLKU)
        df = pd.read_sql_query(f"SELECT * FROM {taulu}", conn)
        conn.close()
    except:
        # Jos taulua ei ole (esim. kauden eka päivä), ei anneta momentum-etua
        return 0.0

    if df.empty:
        return 0.0

    # 1. KESKINÄISET OTTELUT (H2H)
    # Etsitään pelit, joissa nämä kaksi joukkuetta ovat kohdanneet
    h2h_pelit = df[
        ((df['Kotijoukkue'] == koti_nimi) & (df['Vierasjoukkue'] == vieras_nimi)) |
        ((df['Kotijoukkue'] == vieras_nimi) & (df['Vierasjoukkue'] == koti_nimi))
    ]
    
    koti_h2h_voitot = 0
    vieras_h2h_voitot = 0
    
    for _, peli in h2h_pelit.iterrows():
        if peli['Koti_Juoksut'] > peli['Vieras_Juoksut']:
            if peli['Kotijoukkue'] == koti_nimi: koti_h2h_voitot += 1
            else: vieras_h2h_voitot += 1
        else:
            if peli['Vierasjoukkue'] == koti_nimi: koti_h2h_voitot += 1
            else: vieras_h2h_voitot += 1
            
    h2h_yht = koti_h2h_voitot + vieras_h2h_voitot
    
    # 2. KUNTOPUNTARI (Viimeiset 10 peliä per joukkue)
    def laske_kunto(joukkue):
        pelit = df[(df['Kotijoukkue'] == joukkue) | (df['Vierasjoukkue'] == joukkue)].tail(10)
        voitot = 0
        for _, p in pelit.iterrows():
            if p['Kotijoukkue'] == joukkue and p['Koti_Juoksut'] > p['Vieras_Juoksut']: voitot += 1
            elif p['Vierasjoukkue'] == joukkue and p['Vieras_Juoksut'] > p['Koti_Juoksut']: voitot += 1
        return voitot / max(len(pelit), 1)

    koti_kunto = laske_kunto(koti_nimi)
    vieras_kunto = laske_kunto(vieras_nimi)
    
    # 3. YHDISTETÄÄN EDUKSI (Matemaattinen skaalaus)
    # Jos koti on voittanut kaikki H2H-pelit, h2h_etu on +0.005
    h2h_etu = 0.0
    if h2h_yht > 0:
        h2h_etu = ((koti_h2h_voitot / h2h_yht) - 0.5) * 0.01  # Max +/- 0.005 Edge
        
    # Jos koti on voittanut 10/10 ja vieras 0/10, kunto_etu on +0.005
    kunto_etu = (koti_kunto - vieras_kunto) * 0.005

    # Palautetaan yhteinen momentum-etu kotijoukkueen näkökulmasta
    return h2h_etu + kunto_etu

def laske_todennakoisyys(koti_nimi, vieras_nimi, koti_sp, koti_bp, koti_woba, vieras_sp, vieras_bp, vieras_woba, koti_woba_bp=None, vieras_woba_bp=None, lampotila_c: int = 20, tuuli_ms: int = 0, tuuli_suunta: str = "Sivutuuli / Tyyni", koti_lyh: str = "NYY", koti_iso: float = 0.150, vieras_iso: float = 0.150) -> dict:
    """
    Laskee ottelun lopputuloksen dynaamisesti hyödyntäen:
    - Time Decay xFIP (SP & BP)
    - Bullpen Leverage (1.20x)
    - wOBA Platoon Splits
    - K-BB% (Syöttäjän dominanssi)
    - ISO (Lyöjien tyrmäysvoima)
    """
    # 1. Haetaan stadionin ja sään tiedot
    stadion = STADION_DATA.get(koti_lyh, {"Stadion": "Tuntematon", "PF": 1.00, "Dome": False})
    saa_kerroin = laske_saa_kerroin(lampotila_c, tuuli_ms, tuuli_suunta, stadion["Dome"])
    ymparisto_kerroin = stadion["PF"] * saa_kerroin

    if koti_woba_bp is None: koti_woba_bp = koti_woba
    if vieras_woba_bp is None: vieras_woba_bp = vieras_woba

    # =============================================================
    # HAETAAN SYÖTTÖVUOROT JA LASKETAAN BULLPEN LEVERAGE
    # =============================================================
    BULLPEN_LEVERAGE = 1.20

    koti_sp_ip_raaka = min(koti_sp.get("IP", 5.5), 8.1) 
    koti_bp_ip_raaka = 9.0 - koti_sp_ip_raaka
    koti_bp_ip_painotettu = koti_bp_ip_raaka * BULLPEN_LEVERAGE
    koti_yhteensa_ip = koti_sp_ip_raaka + koti_bp_ip_painotettu
    koti_sp_paino = koti_sp_ip_raaka / koti_yhteensa_ip
    koti_bp_paino = koti_bp_ip_painotettu / koti_yhteensa_ip

    vieras_sp_ip_raaka = min(vieras_sp.get("IP", 5.5), 8.1)
    vieras_bp_ip_raaka = 9.0 - vieras_sp_ip_raaka
    vieras_bp_ip_painotettu = vieras_bp_ip_raaka * BULLPEN_LEVERAGE
    vieras_yhteensa_ip = vieras_sp_ip_raaka + vieras_bp_ip_painotettu
    vieras_sp_paino = vieras_sp_ip_raaka / vieras_yhteensa_ip
    vieras_bp_paino = vieras_bp_ip_painotettu / vieras_yhteensa_ip

    # HAETAAN xFIP ARVOT
    koti_sp_xfip = koti_sp.get("xFIP_All", LIIGA_XFIP_KA)
    koti_bp_xfip = koti_bp.get("All", LIIGA_XFIP_KA)
    vieras_sp_xfip = vieras_sp.get("xFIP_All", LIIGA_XFIP_KA)
    vieras_bp_xfip = vieras_bp.get("All", LIIGA_XFIP_KA)

    # =============================================================
    # UUSI: HAETAAN ISO & K-BB% KERTOIMET (Liigan KA on 0.150)
    # =============================================================
    LIIGA_TILASTO_KA = 0.150

    koti_iso_kerroin = 1.0 + (koti_iso - LIIGA_TILASTO_KA)
    vieras_iso_kerroin = 1.0 + (vieras_iso - LIIGA_TILASTO_KA)

    koti_sp_kbb = 1.0 - (koti_sp.get("K_BB_pct", LIIGA_TILASTO_KA) - LIIGA_TILASTO_KA)
    koti_bp_kbb = 1.0 - (koti_bp.get("K_BB_pct", LIIGA_TILASTO_KA) - LIIGA_TILASTO_KA)
    vieras_sp_kbb = 1.0 - (vieras_sp.get("K_BB_pct", LIIGA_TILASTO_KA) - LIIGA_TILASTO_KA)
    vieras_bp_kbb = 1.0 - (vieras_bp.get("K_BB_pct", LIIGA_TILASTO_KA) - LIIGA_TILASTO_KA)

    # -------------------------------------------------------------
    # 1. KOTIJOUKKUEEN HYÖKKÄYSETU
    # -------------------------------------------------------------
    koti_etu_sp = (koti_woba / LIIGA_WOBA_KA) - (vieras_sp_xfip / LIIGA_XFIP_KA) + (koti_iso - LIIGA_TILASTO_KA) - (vieras_sp.get("K_BB_pct", LIIGA_TILASTO_KA) - LIIGA_TILASTO_KA)
    koti_etu_bp = (koti_woba_bp / LIIGA_WOBA_KA) - (vieras_bp_xfip / LIIGA_XFIP_KA) + (koti_iso - LIIGA_TILASTO_KA) - (vieras_bp.get("K_BB_pct", LIIGA_TILASTO_KA) - LIIGA_TILASTO_KA)
    koti_etu_perus = (koti_etu_sp * vieras_sp_paino) + (koti_etu_bp * vieras_bp_paino)

    # -------------------------------------------------------------
    # 2. VIERASJOUKKUEEN HYÖKKÄYSETU
    # -------------------------------------------------------------
    vieras_etu_sp = (vieras_woba / LIIGA_WOBA_KA) - (koti_sp_xfip / LIIGA_XFIP_KA) + (vieras_iso - LIIGA_TILASTO_KA) - (koti_sp.get("K_BB_pct", LIIGA_TILASTO_KA) - LIIGA_TILASTO_KA)
    vieras_etu_bp = (vieras_woba_bp / LIIGA_WOBA_KA) - (koti_bp_xfip / LIIGA_XFIP_KA) + (vieras_iso - LIIGA_TILASTO_KA) - (koti_bp.get("K_BB_pct", LIIGA_TILASTO_KA) - LIIGA_TILASTO_KA)
    vieras_etu_perus = (vieras_etu_sp * koti_sp_paino) + (vieras_etu_bp * koti_bp_paino)

    # -------------------------------------------------------------
    # 3. KOTIKENTTÄ, MOMENTUM JA LOPPUTULOS
    # -------------------------------------------------------------
    koti_etu = koti_etu_perus + 0.035
    momentum = hae_momentum(koti_nimi, vieras_nimi)
    koti_etu += momentum

    ero = koti_etu - vieras_etu_perus
    koti_tod = 1 / (1 + np.exp(-5.0 * ero))
    vieras_tod = 1 - koti_tod

    # -------------------------------------------------------------
    # 4. JUOKSUODOTUS RAAKANA (wOBA * xFIP * KBB * ISO)
    # -------------------------------------------------------------
    perus_odotus = 8.6 
    
    k_odotus_sp = (perus_odotus / 2) * (koti_woba / LIIGA_WOBA_KA) * (vieras_sp_xfip / LIIGA_XFIP_KA) * vieras_sp_kbb * koti_iso_kerroin
    k_odotus_bp = (perus_odotus / 2) * (koti_woba_bp / LIIGA_WOBA_KA) * (vieras_bp_xfip / LIIGA_XFIP_KA) * vieras_bp_kbb * koti_iso_kerroin
    k_odotus_raaka = (k_odotus_sp * vieras_sp_paino) + (k_odotus_bp * vieras_bp_paino)

    v_odotus_sp = (perus_odotus / 2) * (vieras_woba / LIIGA_WOBA_KA) * (koti_sp_xfip / LIIGA_XFIP_KA) * koti_sp_kbb * vieras_iso_kerroin
    v_odotus_bp = (perus_odotus / 2) * (vieras_woba_bp / LIIGA_WOBA_KA) * (koti_bp_xfip / LIIGA_XFIP_KA) * koti_bp_kbb * vieras_iso_kerroin
    v_odotus_raaka = (v_odotus_sp * koti_sp_paino) + (v_odotus_bp * koti_bp_paino)
    
    k_odotus_raaka += 0.20
    momentum_edge_raaka = hae_momentum(koti_nimi, vieras_nimi)
    k_odotus_raaka += (momentum_edge_raaka * 10.0)

    k_odotus = k_odotus_raaka * ymparisto_kerroin
    v_odotus = v_odotus_raaka * ymparisto_kerroin
    total_odotus = k_odotus + v_odotus

    # -------------------------------------------------------------
    # 5. LOPPUTULOS (Pythagorean Expectation)
    # -------------------------------------------------------------
    koti_tod = (k_odotus ** 1.83) / ((k_odotus ** 1.83) + (v_odotus ** 1.83))
    vieras_tod = 1.0 - koti_tod

    koti_woba_total = (koti_woba * vieras_sp_paino) + (koti_woba_bp * vieras_bp_paino)
    vieras_woba_total = (vieras_woba * koti_sp_paino) + (vieras_woba_bp * koti_bp_paino)

    return {
        "koti_voitto_tod": koti_tod,
        "vieras_voitto_tod": vieras_tod,
        "total_odotus": total_odotus,
        "k_odotus": k_odotus,
        "v_odotus": v_odotus,
        "koti_sp_dyn": koti_sp_xfip,
        "koti_bp_dyn": koti_bp_xfip,
        "koti_total_xfip": (koti_sp_xfip * koti_sp_paino) + (koti_bp_xfip * koti_bp_paino),
        "vieras_sp_dyn": vieras_sp_xfip,
        "vieras_bp_dyn": vieras_bp_xfip,
        "vieras_total_xfip": (vieras_sp_xfip * vieras_sp_paino) + (vieras_bp_xfip * vieras_bp_paino),
        "momentum_edge": momentum_edge_raaka,
        "koti_woba_total": koti_woba_total,
        "vieras_woba_total": vieras_woba_total,
        "stadion_nimi": stadion["Stadion"],
        "stadion_pf": stadion["PF"],
        "onko_dome": stadion["Dome"],
        "saa_kerroin": saa_kerroin,
        "ymparisto_kerroin": ymparisto_kerroin
    }
