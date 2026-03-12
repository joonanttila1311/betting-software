"""
laskentamoottori.py
===================
MLB-vedonlyönnin todennäköisyyslaskuri (Time Decay + Platoon Splits)
"""

import sqlite3
import pandas as pd
from pathlib import Path

DB_POLKU = "mlb_historical.db"
TAULU    = "ottelutulokset_2025"

# ---------------------------------------------------------
# Todennäköisyyspainotukset (summa = 1.0)
# VEGAS PRO -ASETUS (Väliaikainen, kunnes lyöjät lisätään)
# ---------------------------------------------------------
PAINO_YLEINEN_XFIP = 0.01  # 1 % Joukkueen vanha taso (Intangibles)
PAINO_H2H_XFIP     = 0.01  # 1 % Keskinäiset kohtaamiset
PAINO_XFIP         = 0.98  # 98 % Syöttäjät (Dynaaminen Platoon Split xFIP)

XFIP_REFERENSSI = 4.00

# ---------------------------------------------------------
# Juoksuodottaman painotukset (summa = 1.0)
# ---------------------------------------------------------
PAINO_JO_HYOKKAYS  = 0.01
PAINO_JO_PUOLUSTUS = 0.01
PAINO_JO_XFIP      = 0.98

MLB_JUOKSU_KESKIARVO = 4.50

def lataa_data(db_polku: str = DB_POLKU) -> pd.DataFrame:
    if not Path(db_polku).exists(): return pd.DataFrame()
    yhteys = sqlite3.connect(db_polku)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {TAULU}", yhteys)
    except sqlite3.Error:
        df = pd.DataFrame()
    yhteys.close()
    return df

def laske_yleinen_voittoprosentti(df: pd.DataFrame, joukkue: str) -> float:
    if df.empty: return 0.5
    koti_pelit = df[df["Kotijoukkue"] == joukkue]
    vieras_pelit = df[df["Vierasjoukkue"] == joukkue]
    voitot = (koti_pelit["Koti_Juoksut"] > koti_pelit["Vieras_Juoksut"]).sum() + \
             (vieras_pelit["Vieras_Juoksut"] > vieras_pelit["Koti_Juoksut"]).sum()
    yhteensa = len(koti_pelit) + len(vieras_pelit)
    return voitot / yhteensa if yhteensa > 0 else 0.5

def laske_h2h_voittoprosentti(df: pd.DataFrame, koti: str, vieras: str) -> tuple[float, int]:
    if df.empty: return 0.5, 0
    h2h = df[((df["Kotijoukkue"] == koti) & (df["Vierasjoukkue"] == vieras)) |
             ((df["Kotijoukkue"] == vieras) & (df["Vierasjoukkue"] == koti))]
    if len(h2h) == 0: return 0.5, 0
    k_voitot = ((h2h["Kotijoukkue"] == koti) & (h2h["Koti_Juoksut"] > h2h["Vieras_Juoksut"])).sum() + \
               ((h2h["Vierasjoukkue"] == koti) & (h2h["Vieras_Juoksut"] > h2h["Koti_Juoksut"])).sum()
    return k_voitot / len(h2h), len(h2h)

def hae_turvallinen_keskiarvo(sarja: pd.Series, oletus: float = MLB_JUOKSU_KESKIARVO) -> float:
    if sarja.empty: return oletus
    ka = sarja.mean()
    return oletus if pd.isna(ka) else float(ka)

def laske_yhdistetty_xfip(aloittaja_xfip: float, ip_start: float, bullpen_xfip: float) -> float:
    aloittajan_osuus = min(1.0, ip_start / 9.0)
    bullpenin_osuus = 1.0 - aloittajan_osuus
    return (aloittaja_xfip * aloittajan_osuus) + (bullpen_xfip * bullpenin_osuus)

def laske_juoksuodottama(koti: str, vieras: str, df: pd.DataFrame, koti_xfip: float, vieras_xfip: float) -> dict:
    if df.empty:
        k_pisteet = v_pisteet = k_paastot = v_paastot = MLB_JUOKSU_KESKIARVO
    else:
        k_pisteet = hae_turvallinen_keskiarvo(pd.concat([df[df["Kotijoukkue"] == koti]["Koti_Juoksut"], df[df["Vierasjoukkue"] == koti]["Vieras_Juoksut"]]))
        v_pisteet = hae_turvallinen_keskiarvo(pd.concat([df[df["Kotijoukkue"] == vieras]["Koti_Juoksut"], df[df["Vierasjoukkue"] == vieras]["Vieras_Juoksut"]]))
        k_paastot = hae_turvallinen_keskiarvo(pd.concat([df[df["Kotijoukkue"] == vieras]["Vieras_Juoksut"], df[df["Vierasjoukkue"] == vieras]["Koti_Juoksut"]]))
        v_paastot = hae_turvallinen_keskiarvo(pd.concat([df[df["Kotijoukkue"] == koti]["Vieras_Juoksut"], df[df["Vierasjoukkue"] == koti]["Koti_Juoksut"]]))

    if koti_xfip and vieras_xfip:
        k_odotus = (PAINO_JO_HYOKKAYS * k_pisteet + PAINO_JO_PUOLUSTUS * k_paastot + PAINO_JO_XFIP * vieras_xfip)
        v_odotus = (PAINO_JO_HYOKKAYS * v_pisteet + PAINO_JO_PUOLUSTUS * v_paastot + PAINO_JO_XFIP * koti_xfip)
    else:
        k_odotus = 0.5 * k_pisteet + 0.5 * k_paastot
        v_odotus = 0.5 * v_pisteet + 0.5 * v_paastot

    return {"koti_odotus": round(k_odotus, 2), "vieras_odotus": round(v_odotus, 2), "total": round(k_odotus + v_odotus, 2)}

def laske_dynaaminen_xfip(stats: dict, opp_l: int, opp_r: int) -> float:
    """Laskee syöttäjälle/bullpenille tarkan xFIP:n vastustajan kätisyyksien perusteella."""
    return ((opp_l * stats["vs_L"]) + (opp_r * stats["vs_R"])) / 9.0

def laske_todennakoisyys(
    koti: str, vieras: str, df: pd.DataFrame = None, db_polku: str = DB_POLKU,
    koti_sp: dict = None, koti_bp: dict = None, koti_lyojat: dict = None,
    vieras_sp: dict = None, vieras_bp: dict = None, vieras_lyojat: dict = None
) -> dict:
    if df is None: df = lataa_data(db_polku)

    k_yleinen = laske_yleinen_voittoprosentti(df, koti)
    v_yleinen = laske_yleinen_voittoprosentti(df, vieras)
    k_norm = k_yleinen / (k_yleinen + v_yleinen) if (k_yleinen + v_yleinen) > 0 else 0.5
    v_norm = 1.0 - k_norm

    h2h_k, h2h_n = laske_h2h_voittoprosentti(df, koti, vieras)
    h2h_v = 1.0 - h2h_k

    # 1. Kotijoukkue SYÖTTÄÄ (Vieraan lyöjiä vastaan)
    k_sp_dyn = laske_dynaaminen_xfip(koti_sp, vieras_lyojat["L"], vieras_lyojat["R"])
    k_bp_dyn = laske_dynaaminen_xfip(koti_bp, vieras_lyojat["L"], vieras_lyojat["R"])
    koti_total_xfip = laske_yhdistetty_xfip(k_sp_dyn, koti_sp["IP"], k_bp_dyn)

    # 2. Vierasjoukkue SYÖTTÄÄ (Kodin lyöjiä vastaan)
    v_sp_dyn = laske_dynaaminen_xfip(vieras_sp, koti_lyojat["L"], koti_lyojat["R"])
    v_bp_dyn = laske_dynaaminen_xfip(vieras_bp, koti_lyojat["L"], koti_lyojat["R"])
    vieras_total_xfip = laske_yhdistetty_xfip(v_sp_dyn, vieras_sp["IP"], v_bp_dyn)

    # 3. Voimapisteet
    koti_xfip_score = XFIP_REFERENSSI / koti_total_xfip
    vieras_xfip_score = XFIP_REFERENSSI / vieras_total_xfip
    summa_xfip = koti_xfip_score + vieras_xfip_score
    
    k_xfip_norm = koti_xfip_score / summa_xfip
    v_xfip_norm = vieras_xfip_score / summa_xfip

    k_final = (PAINO_YLEINEN_XFIP * k_norm) + (PAINO_H2H_XFIP * h2h_k) + (PAINO_XFIP * k_xfip_norm)
    v_final = (PAINO_YLEINEN_XFIP * v_norm) + (PAINO_H2H_XFIP * h2h_v) + (PAINO_XFIP * v_xfip_norm)

    kokonais = k_final + v_final
    juoksut = laske_juoksuodottama(koti, vieras, df, koti_total_xfip, vieras_total_xfip)

    return {
        "koti_voitto_tod": round(k_final / kokonais, 4), "vieras_voitto_tod": round(v_final / kokonais, 4),
        "koti_total_xfip": koti_total_xfip, "vieras_total_xfip": vieras_total_xfip,
        "koti_sp_dyn": k_sp_dyn, "koti_bp_dyn": k_bp_dyn,
        "vieras_sp_dyn": v_sp_dyn, "vieras_bp_dyn": v_bp_dyn,
        "k_odotus": juoksut["koti_odotus"], "v_odotus": juoksut["vieras_odotus"], "total_odotus": juoksut["total"]
    }