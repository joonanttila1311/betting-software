"""
laskentamoottori.py – v5.1 (Weighted Factor Engine)
==========================================================
Tämä on mallin aivot. Laskee voittotodennäköisyydet ja juoksuodotukset.
Päivitetty: Pitching (60% K-BB%, 40% xFIP) | Hitting (80% wOBA, 20% ISO)
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import date

DB_POLKU = "mlb_historical.db"
LIIGA_XFIP_KA = 3.65  # Vastaa uutta FIP-vakiota
LIIGA_WOBA_KA = 0.310
LIIGA_TILASTO_KA = 0.150

# STADIONIT JA SÄÄ - SÄILYTETTY TÄYSIN ENNALLAAN
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
    "ATH": {"Stadion": "Sutter Health Park", "PF": 0.95, "Dome": False},
    "SEA": {"Stadion": "T-Mobile Park", "PF": 0.93, "Dome": False},
}

# REST FACTOR COEFFICIENTS
REST_KERTOIMET = {
    (0, 2):   {"xfip": 1.08, "kbb": 0.95},   # Väsynyt (0-2 pv lepoa)
    (3, 3):   {"xfip": 1.02, "kbb": 0.98},   # Normaali 4-man rotaatio
    (4, 5):   {"xfip": 1.00, "kbb": 1.00},   # Optimaalinen 5-man rotaatio
    (6, 8):   {"xfip": 0.98, "kbb": 1.02},   # Ekstra lepo, terävä
    (9, 14):  {"xfip": 1.03, "kbb": 0.97},   # Liikaa lepoa, rytmi katkennut
    (15, 999):{"xfip": 1.06, "kbb": 0.93},   # Palannut DL:ltä tai pitkiltä vapaalta
}

def hae_viimeisin_peli(pitcher_name: str, db_polku: str = DB_POLKU) -> date | None:
    """Hakee syöttäjän viimeisen pelin päivämäärän statcast-taulusta."""
    try:
        conn = sqlite3.connect(db_polku)
        kysely = """
            SELECT MAX(game_date) as last_game
            FROM statcast_2025
            WHERE player_name = ? AND events IS NOT NULL
        """
        df = pd.read_sql_query(kysely, conn, params=(pitcher_name,))
        conn.close()
        
        if df is not None and not df.empty and pd.notna(df.iloc[0, 0]):
            last_game_str = str(df.iloc[0, 0])[:10]
            return pd.to_datetime(last_game_str).date()
    except Exception as e:
        pass
    return None

def laske_lepo_paivat(pitcher_name: str, laskenta_paiva: date) -> int:
    """Laskee montako päivää lepoa syöttäjällä on."""
    viimeisin_peli = hae_viimeisin_peli(pitcher_name)
    if viimeisin_peli is None:
        return 4  # Oletus: 4 päivää lepoa jos ei dataa
    lepo = (laskenta_paiva - viimeisin_peli).days
    return max(0, lepo)

def apply_rest_factor(xfip: float, k_bb_pct: float, rest_days: int) -> tuple[float, float]:
    """Soveltaa lepo-kertoimet xFIP:iin ja K-BB% arvoihin."""
    kerroin = next(
        (v for (min_d, max_d), v in REST_KERTOIMET.items() if min_d <= rest_days <= max_d),
        REST_KERTOIMET[(4, 5)]  # Oletus: optimaalinen 5-man rotaatio
    )
    adjusted_xfip = xfip * kerroin["xfip"]
    adjusted_kbb = k_bb_pct * kerroin["kbb"]
    return adjusted_xfip, adjusted_kbb

def hae_rest_factor_info(pitcher_name: str, laskenta_paiva: date = None) -> dict:
    """Hakee yksityiskohtaiset lepo-kertoin tiedot syöttäjälle."""
    if laskenta_paiva is None:
        laskenta_paiva = date.today()
    
    viimeisin_peli = hae_viimeisin_peli(pitcher_name)
    rest_days = laske_lepo_paivat(pitcher_name, laskenta_paiva)
    
    # Määritä rest factor kategoria
    rest_category = next(
        ((min_d, max_d) for (min_d, max_d) in REST_KERTOIMET.keys() if min_d <= rest_days <= max_d),
        (4, 5)
    )
    kerroin = REST_KERTOIMET[rest_category]
    
    # Kuvaa kategoriaa
    kategoriat = {
        (0, 2): "🔴 VÄSYNYT",
        (3, 3): "🟡 NORMAALI (4-man)",
        (4, 5): "🟢 OPTIMAALINEN (5-man)",
        (6, 8): "💚 TERÄVÄ (ekstra lepo)",
        (9, 14): "🟠 LIIKAA LEPOA",
        (15, 999): "⚠️ PALANNUT DL:LTÄ",
    }
    kategoria_teksti = kategoriat.get(rest_category, "Tuntematon")
    
    return {
        "last_game_date": viimeisin_peli,
        "rest_days": rest_days,
        "rest_category": kategoria_teksti,
        "xfip_multiplier": kerroin["xfip"],
        "kbb_multiplier": kerroin["kbb"],
        "rest_range": f"{rest_category[0]}-{rest_category[1]}" if rest_category[1] != 999 else f"{rest_category[0]}+"
    }

def laske_saa_kerroin(lampotila_c: int, tuuli_ms: int, tuuli_suunta: str, is_dome: bool) -> float:
    if is_dome: return 1.00
    kerroin = 1.00
    if lampotila_c >= 35: kerroin += 0.075
    elif lampotila_c >= 30: kerroin += 0.050
    elif lampotila_c >= 25: kerroin += 0.025
    elif lampotila_c < 10: kerroin -= 0.050
    elif lampotila_c < 15: kerroin -= 0.025
    if tuuli_ms >= 3 and tuuli_suunta != "Sivutuuli / Tyyni":
        voimakkuus = 0.10 if tuuli_ms >= 6 else 0.05
        if tuuli_suunta == "Ulos katsomoon": kerroin += voimakkuus
        elif tuuli_suunta == "Sisään pesälle": kerroin -= voimakkuus
    return kerroin

def hae_momentum(koti_nimi, vieras_nimi):
    vuosi = date.today().year
    taulu = f"ottelutulokset_{vuosi}"
    conn = None
    try:
        conn = sqlite3.connect(DB_POLKU)
        df = pd.read_sql_query(f"SELECT * FROM {taulu}", conn)
    except:
        return 0.0
    finally:
        if conn is not None:
            conn.close()

    if df.empty: return 0.0

    h2h_pelit = df[((df['Kotijoukkue'] == koti_nimi) & (df['Vierasjoukkue'] == vieras_nimi)) | ((df['Kotijoukkue'] == vieras_nimi) & (df['Vierasjoukkue'] == koti_nimi))]
    koti_h2h_voitot = sum((h2h_pelit['Kotijoukkue'] == koti_nimi) & (h2h_pelit['Koti_Juoksut'] > h2h_pelit['Vieras_Juoksut'])) + \
                      sum((h2h_pelit['Vierasjoukkue'] == koti_nimi) & (h2h_pelit['Vieras_Juoksut'] > h2h_pelit['Koti_Juoksut']))
    vieras_h2h_voitot = len(h2h_pelit) - koti_h2h_voitot
    h2h_yht = len(h2h_pelit)

    def laske_kunto(joukkue):
        pelit = df[(df['Kotijoukkue'] == joukkue) | (df['Vierasjoukkue'] == joukkue)].tail(10)
        voitot = sum((pelit['Kotijoukkue'] == joukkue) & (pelit['Koti_Juoksut'] > pelit['Vieras_Juoksut'])) + \
                 sum((pelit['Vierasjoukkue'] == joukkue) & (pelit['Vieras_Juoksut'] > pelit['Koti_Juoksut']))
        return voitot / max(len(pelit), 1)

    koti_kunto = laske_kunto(koti_nimi)
    vieras_kunto = laske_kunto(vieras_nimi)
    
    h2h_etu = ((koti_h2h_voitot / h2h_yht) - 0.5) * 0.01 if h2h_yht > 0 else 0.0
    kunto_etu = (koti_kunto - vieras_kunto) * 0.005
    return h2h_etu + kunto_etu

def hae_puolustus_kerroin(joukkue, conn):
    """Hakee joukkueen automaattisen DER-puolustuskertoimen tietokannasta."""
    try:
        cur = conn.cursor()
        # Haetaan kerroin 'puolustus_statcast' -taulusta tiimin nimellä (esim. 'NYY')
        cur.execute("SELECT Puolustus_Kerroin FROM puolustus_statcast WHERE Team=?", (joukkue,))
        rivi = cur.fetchone()
        if rivi:
            return rivi[0]
    except Exception:
        pass
    return 1.0 # Palauttaa neutraalin 1.0, jos tiimiä ei jostain syystä löydy

def laske_todennakoisyys(koti_nimi, vieras_nimi, koti_sp, koti_bp, koti_woba, vieras_sp, vieras_bp, vieras_woba, koti_woba_bp=None, vieras_woba_bp=None, lampotila_c: int = 20, tuuli_ms: int = 0, tuuli_suunta: str = "Sivutuuli / Tyyni", koti_lyh: str = "NYY", vieras_lyh: str = "BOS", koti_iso: float = 0.150, vieras_iso: float = 0.150, koti_sp_rest: int = 4, vieras_sp_rest: int = 4) -> dict:
    
    stadion = STADION_DATA.get(koti_lyh, {"Stadion": "Tuntematon", "PF": 1.00, "Dome": False})
    saa_kerroin = laske_saa_kerroin(lampotila_c, tuuli_ms, tuuli_suunta, stadion["Dome"])
    ymparisto_kerroin = stadion["PF"] * saa_kerroin

    if koti_woba_bp is None: koti_woba_bp = koti_woba
    if vieras_woba_bp is None: vieras_woba_bp = vieras_woba

    BULLPEN_LEVERAGE = 1.20
    koti_sp_ip_raaka = min(koti_sp.get("IP", 5.5), 8.1)
    koti_bp_ip_painotettu = (9.0 - koti_sp_ip_raaka) * BULLPEN_LEVERAGE
    koti_yhteensa_ip = koti_sp_ip_raaka + koti_bp_ip_painotettu
    koti_sp_paino = koti_sp_ip_raaka / koti_yhteensa_ip
    koti_bp_paino = koti_bp_ip_painotettu / koti_yhteensa_ip

    vieras_sp_ip_raaka = min(vieras_sp.get("IP", 5.5), 8.1)
    vieras_bp_ip_painotettu = (9.0 - vieras_sp_ip_raaka) * BULLPEN_LEVERAGE
    vieras_yhteensa_ip = vieras_sp_ip_raaka + vieras_bp_ip_painotettu
    vieras_sp_paino = vieras_sp_ip_raaka / vieras_yhteensa_ip
    vieras_bp_paino = vieras_bp_ip_painotettu / vieras_yhteensa_ip

    koti_sp_xfip = koti_sp.get("xFIP_All", LIIGA_XFIP_KA)
    koti_bp_xfip = koti_bp.get("All", LIIGA_XFIP_KA)
    vieras_sp_xfip = vieras_sp.get("xFIP_All", LIIGA_XFIP_KA)
    vieras_bp_xfip = vieras_bp.get("All", LIIGA_XFIP_KA)

    # UUSI: SOVELLETAAN LEPO-KERTOIMET
    koti_sp_kbb_raw = koti_sp.get("K_BB_pct", LIIGA_TILASTO_KA)
    vieras_sp_kbb_raw = vieras_sp.get("K_BB_pct", LIIGA_TILASTO_KA)
    
    koti_sp_xfip, koti_sp_kbb_raw = apply_rest_factor(koti_sp_xfip, koti_sp_kbb_raw, koti_sp_rest)
    vieras_sp_xfip, vieras_sp_kbb_raw = apply_rest_factor(vieras_sp_xfip, vieras_sp_kbb_raw, vieras_sp_rest)

    koti_iso_kerroin = 1.0 + (koti_iso - LIIGA_TILASTO_KA)
    vieras_iso_kerroin = 1.0 + (vieras_iso - LIIGA_TILASTO_KA)
    koti_sp_kbb   = 1.0 - (koti_sp_kbb_raw - LIIGA_TILASTO_KA)
    koti_bp_kbb   = 1.0 - (koti_bp.get("Bullpen_K_BB_pct", LIIGA_TILASTO_KA) - LIIGA_TILASTO_KA)
    vieras_sp_kbb = 1.0 - (vieras_sp_kbb_raw - LIIGA_TILASTO_KA)
    vieras_bp_kbb = 1.0 - (vieras_bp.get("Bullpen_K_BB_pct", LIIGA_TILASTO_KA) - LIIGA_TILASTO_KA)

    # Varmistetaan että kertoimet pysyvät järkevissä rajoissa
    koti_sp_kbb   = max(0.85, min(1.15, koti_sp_kbb))
    koti_bp_kbb   = max(0.85, min(1.15, koti_bp_kbb))
    vieras_sp_kbb = max(0.85, min(1.15, vieras_sp_kbb))
    vieras_bp_kbb = max(0.85, min(1.15, vieras_bp_kbb))

    # =============================================================
    # UUSI: YHDISTETYT KERTOIMET (80/20 Hitting, 60/40 Pitching)
    # =============================================================
    
    # Koti Hyökkäys vs Vieras SP/BP
    koti_hyokkays_kerroin_sp = (0.80 * (koti_woba / LIIGA_WOBA_KA)) + (0.20 * koti_iso_kerroin)
    koti_hyokkays_kerroin_bp = (0.80 * (koti_woba_bp / LIIGA_WOBA_KA)) + (0.20 * koti_iso_kerroin)
    vieras_puolustus_kerroin_sp = (0.40 * (vieras_sp_xfip / LIIGA_XFIP_KA)) + (0.60 * vieras_sp_kbb)
    vieras_puolustus_kerroin_bp = (0.40 * (vieras_bp_xfip / LIIGA_XFIP_KA)) + (0.60 * vieras_bp_kbb)

    # Vieras Hyökkäys vs Koti SP/BP
    vieras_hyokkays_kerroin_sp = (0.80 * (vieras_woba / LIIGA_WOBA_KA)) + (0.20 * vieras_iso_kerroin)
    vieras_hyokkays_kerroin_bp = (0.80 * (vieras_woba_bp / LIIGA_WOBA_KA)) + (0.20 * vieras_iso_kerroin)
    koti_puolustus_kerroin_sp   = (0.40 * (koti_sp_xfip / LIIGA_XFIP_KA)) + (0.60 * koti_sp_kbb)
    koti_puolustus_kerroin_bp   = (0.40 * (koti_bp_xfip / LIIGA_XFIP_KA)) + (0.60 * koti_bp_kbb)

    vieras_puolustus_kerroin_sp = max(0.70, min(1.30, vieras_puolustus_kerroin_sp))
    vieras_puolustus_kerroin_bp = max(0.70, min(1.30, vieras_puolustus_kerroin_bp))
    koti_puolustus_kerroin_sp   = max(0.70, min(1.30, koti_puolustus_kerroin_sp))
    koti_puolustus_kerroin_bp   = max(0.70, min(1.30, koti_puolustus_kerroin_bp))

    # Juoksuodotus (Multiplicative Model)
    perus_odotus = 8.6 
    
    k_odotus_sp = (perus_odotus / 2) * koti_hyokkays_kerroin_sp * vieras_puolustus_kerroin_sp
    k_odotus_bp = (perus_odotus / 2) * koti_hyokkays_kerroin_bp * vieras_puolustus_kerroin_bp
    k_odotus_raaka = (k_odotus_sp * vieras_sp_paino) + (k_odotus_bp * vieras_bp_paino)

    v_odotus_sp = (perus_odotus / 2) * vieras_hyokkays_kerroin_sp * koti_puolustus_kerroin_sp
    v_odotus_bp = (perus_odotus / 2) * vieras_hyokkays_kerroin_bp * koti_puolustus_kerroin_bp
    v_odotus_raaka = (v_odotus_sp * koti_sp_paino) + (v_odotus_bp * koti_bp_paino)

    # === UUSI AUTOMATISOITU PUOLUSTUS (DER) ===
    conn = None
    try:
        conn = sqlite3.connect(DB_POLKU)
        koti_def_auto = hae_puolustus_kerroin(koti_lyh, conn)
        vieras_def_auto = hae_puolustus_kerroin(vieras_lyh, conn)
    finally:
        if conn is not None:
            conn.close()

    # Kotijoukkueen juoksumäärää vaikeuttaa/helpottaa vieraan puolustus, ja päinvastoin
    k_odotus_raaka *= vieras_def_auto
    v_odotus_raaka *= koti_def_auto
    # ==========================================

    momentum_arvo = hae_momentum(koti_nimi, vieras_nimi)
    k_odotus_raaka = (k_odotus_raaka * 1.035) + (momentum_arvo * 2.0)

    k_odotus = k_odotus_raaka * ymparisto_kerroin
    v_odotus = v_odotus_raaka * ymparisto_kerroin
    total_odotus = k_odotus + v_odotus

    koti_tod = (k_odotus ** 1.9) / ((k_odotus ** 1.9) + (v_odotus ** 1.9))
    vieras_tod = 1.0 - koti_tod

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
        "momentum_edge": momentum_arvo,
        "koti_woba_total": (koti_woba * vieras_sp_paino) + (koti_woba_bp * vieras_bp_paino),
        "vieras_woba_total": (vieras_woba * koti_sp_paino) + (vieras_woba_bp * koti_bp_paino),
        "stadion_nimi": stadion["Stadion"],
        "stadion_pf": stadion["PF"],
        "onko_dome": stadion["Dome"],
        "saa_kerroin": saa_kerroin,
        "ymparisto_kerroin": ymparisto_kerroin,
        "koti_sp_kbb_adj":   round(koti_sp_kbb_raw, 4),
        "vieras_sp_kbb_adj": round(vieras_sp_kbb_raw, 4),
        "koti_def": koti_def_auto,      # <--- LISÄÄ TÄMÄ
        "vieras_def": vieras_def_auto   # <--- LISÄÄ TÄMÄ
    }