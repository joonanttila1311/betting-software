"""
laskentamoottori.py
===================
MLB-vedonlyönnin todennäköisyyslaskuri (Statcast FIP -päivitys)

Lukee ottelutulokset SQLite-tietokannasta ja laskee
todennäköisyysarvion kahdelle joukkueelle sekä juoksuodottaman.
Käyttää syöttäjien arviointiin edistynyttä FIP-tilastoa ERA:n sijaan.
"""

import sqlite3
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# VAKIOT
# ---------------------------------------------------------------------------
DB_POLKU = "mlb_historical.db"
TAULU    = "ottelutulokset_2025"

# Todennäköisyyspainotukset ilman FIP (summa = 1.0)
PAINO_YLEINEN = 0.70
PAINO_H2H     = 0.30

# Todennäköisyyspainotukset FIP:n kanssa (summa = 1.0)
PAINO_YLEINEN_FIP = 0.60
PAINO_H2H_FIP     = 0.20
PAINO_FIP         = 0.20

# FIP-referenssiarvo normalisointia varten (~4.20 on MLB keskiarvo)
FIP_REFERENSSI = 4.20

# Juoksuodottaman painotukset FIP:n kanssa (summa = 1.0)
PAINO_JO_HYOKKAYS  = 0.35
PAINO_JO_PUOLUSTUS = 0.35
PAINO_JO_FIP       = 0.30

MLB_JUOKSU_KESKIARVO = 4.50

def lataa_data(db_polku: str = DB_POLKU) -> pd.DataFrame:
    if not Path(db_polku).exists():
        raise FileNotFoundError("Tietokantaa ei löydy.")
    yhteys = sqlite3.connect(db_polku)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {TAULU}", yhteys)
    finally:
        yhteys.close()
    return df

def laske_yleinen_voittoprosentti(df: pd.DataFrame, joukkue: str) -> float:
    koti_pelit    = df[df["Kotijoukkue"]  == joukkue]
    koti_voitot   = (koti_pelit["Koti_Juoksut"] > koti_pelit["Vieras_Juoksut"]).sum()
    vieras_pelit  = df[df["Vierasjoukkue"] == joukkue]
    vieras_voitot = (vieras_pelit["Vieras_Juoksut"] > vieras_pelit["Koti_Juoksut"]).sum()
    yhteensa = len(koti_pelit) + len(vieras_pelit)
    if yhteensa == 0: return 0.5
    return (koti_voitot + vieras_voitot) / yhteensa

def laske_h2h_voittoprosentti(df: pd.DataFrame, koti: str, vieras: str) -> tuple[float, int]:
    h2h = df[
        ((df["Kotijoukkue"]  == koti)   & (df["Vierasjoukkue"] == vieras)) |
        ((df["Kotijoukkue"]  == vieras) & (df["Vierasjoukkue"] == koti))
    ].copy()
    if len(h2h) == 0: return 0.5, 0
    koti_voitot = 0
    for _, rivi in h2h.iterrows():
        if rivi["Kotijoukkue"] == koti and rivi["Koti_Juoksut"] > rivi["Vieras_Juoksut"]:
            koti_voitot += 1
        elif rivi["Vierasjoukkue"] == koti and rivi["Vieras_Juoksut"] > rivi["Koti_Juoksut"]:
            koti_voitot += 1
    return koti_voitot / len(h2h), len(h2h)

def laske_juoksuodottama(
    koti: str, vieras: str, df: pd.DataFrame,
    koti_fip: float | None = None, vieras_fip: float | None = None,
) -> dict:
    
    def pisteet_ka(joukkue: str) -> float:
        koti_p   = df[df["Kotijoukkue"]  == joukkue]["Koti_Juoksut"]
        vieras_p = df[df["Vierasjoukkue"] == joukkue]["Vieras_Juoksut"]
        kaikki   = pd.concat([koti_p, vieras_p])
        return float(kaikki.mean()) if len(kaikki) > 0 else MLB_JUOKSU_KESKIARVO

    def paastot_ka(joukkue: str) -> float:
        koti_p   = df[df["Kotijoukkue"]  == joukkue]["Vieras_Juoksut"]
        vieras_p = df[df["Vierasjoukkue"] == joukkue]["Koti_Juoksut"]
        kaikki   = pd.concat([koti_p, vieras_p])
        return float(kaikki.mean()) if len(kaikki) > 0 else MLB_JUOKSU_KESKIARVO

    koti_pisteet   = pisteet_ka(koti)
    vieras_pisteet = pisteet_ka(vieras)
    koti_paastot   = paastot_ka(vieras) 
    vieras_paastot = paastot_ka(koti)   

    fip_kaytossa = (koti_fip is not None) and (vieras_fip is not None)

    if fip_kaytossa:
        # FIP skaalautuu suoraan juoksuiksi per peli. Rajataan järkevästi 1.0 - 8.0.
        koti_fip_juoksut   = max(1.0, min(8.0, vieras_fip))  
        vieras_fip_juoksut = max(1.0, min(8.0, koti_fip))    

        koti_odotus = (PAINO_JO_HYOKKAYS * koti_pisteet + PAINO_JO_PUOLUSTUS * koti_paastot + PAINO_JO_FIP * koti_fip_juoksut)
        vieras_odotus = (PAINO_JO_HYOKKAYS * vieras_pisteet + PAINO_JO_PUOLUSTUS * vieras_paastot + PAINO_JO_FIP * vieras_fip_juoksut)
    else:
        koti_odotus   = 0.5 * koti_pisteet   + 0.5 * koti_paastot
        vieras_odotus = 0.5 * vieras_pisteet + 0.5 * vieras_paastot

    return {
        "koti_odotus":       round(koti_odotus,   2),
        "vieras_odotus":     round(vieras_odotus, 2),
        "total_odotus":      round(koti_odotus + vieras_odotus, 2),
        "koti_pisteet_ka":   round(koti_pisteet,   2),
        "vieras_pisteet_ka": round(vieras_pisteet, 2),
        "koti_paastot_ka":   round(koti_paastot,   2),
        "vieras_paastot_ka": round(vieras_paastot, 2),
    }

def laske_todennakoisyys(
    koti: str, vieras: str, df: pd.DataFrame | None = None,
    db_polku: str = DB_POLKU, koti_fip: float | None = None, vieras_fip: float | None = None,
) -> dict:
    if df is None: df = lataa_data(db_polku)

    koti_yleinen   = laske_yleinen_voittoprosentti(df, koti)
    vieras_yleinen = laske_yleinen_voittoprosentti(df, vieras)

    summa_yleinen = koti_yleinen + vieras_yleinen
    if summa_yleinen == 0:
        koti_norm = vieras_norm = 0.5
    else:
        koti_norm   = koti_yleinen   / summa_yleinen
        vieras_norm = vieras_yleinen / summa_yleinen

    h2h_koti_vp, h2h_maara = laske_h2h_voittoprosentti(df, koti, vieras)
    h2h_vieras_vp = 1.0 - h2h_koti_vp

    fip_kaytossa = (koti_fip is not None) and (vieras_fip is not None)

    if fip_kaytossa:
        koti_fip_score   = max(0.2, min(0.8, FIP_REFERENSSI / (koti_fip   + 0.01)))
        vieras_fip_score = max(0.2, min(0.8, FIP_REFERENSSI / (vieras_fip + 0.01)))
        fip_summa        = koti_fip_score + vieras_fip_score
        koti_fip_norm    = koti_fip_score   / fip_summa
        vieras_fip_norm  = vieras_fip_score / fip_summa

        koti_yhdistetty = (PAINO_YLEINEN_FIP * koti_norm + PAINO_H2H_FIP * h2h_koti_vp + PAINO_FIP * koti_fip_norm)
        vieras_yhdistetty = (PAINO_YLEINEN_FIP * vieras_norm + PAINO_H2H_FIP * h2h_vieras_vp + PAINO_FIP * vieras_fip_norm)
    else:
        koti_yhdistetty   = PAINO_YLEINEN * koti_norm   + PAINO_H2H * h2h_koti_vp
        vieras_yhdistetty = PAINO_YLEINEN * vieras_norm + PAINO_H2H * h2h_vieras_vp

    kokonais     = koti_yhdistetty + vieras_yhdistetty
    koti_final   = koti_yhdistetty   / kokonais
    vieras_final = vieras_yhdistetty / kokonais

    juoksut = laske_juoksuodottama(koti, vieras, df, koti_fip, vieras_fip)

    return {
        "kotijoukkue":       koti,
        "vierasjoukkue":     vieras,
        "koti_voitto_tod":   round(koti_final,   4),
        "vieras_voitto_tod": round(vieras_final, 4),
        "koti_yleinen_vp":   round(koti_yleinen, 4),
        "vieras_yleinen_vp": round(vieras_yleinen, 4),
        "h2h_koti_vp":       round(h2h_koti_vp,  4),
        "h2h_ottelut":       h2h_maara,
        "fip_kaytossa":      fip_kaytossa,
        "koti_odotus":       juoksut["koti_odotus"],
        "vieras_odotus":     juoksut["vieras_odotus"],
        "total_odotus":      juoksut["total_odotus"],
        "koti_pisteet_ka":   juoksut["koti_pisteet_ka"],
        "vieras_pisteet_ka": juoksut["vieras_pisteet_ka"],
        "koti_paastot_ka":   juoksut["koti_paastot_ka"],
        "vieras_paastot_ka": juoksut["vieras_paastot_ka"],
    }