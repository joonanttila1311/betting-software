"""
laskentamoottori.py
===================
MLB-vedonlyönnin todennäköisyyslaskuri (Vapautettu xFIP & Bullpen -malli)
"""

import sqlite3
import pandas as pd
from pathlib import Path

DB_POLKU = "mlb_historical.db"
TAULU    = "ottelutulokset_2025"

# Todennäköisyyspainotukset (summa = 1.0)
PAINO_YLEINEN_XFIP = 0.01
PAINO_H2H_XFIP     = 0.01
PAINO_XFIP         = 0.98  # Kasvatettu syöttämisen painoa, koska data on nyt tarkkaa!

XFIP_REFERENSSI = 4.00

# Juoksuodottaman painotukset
PAINO_JO_HYOKKAYS  = 0.01
PAINO_JO_PUOLUSTUS = 0.01
PAINO_JO_XFIP      = 0.98  # Syöttäminen määrittää nyt vahvemmin juoksut

MLB_JUOKSU_KESKIARVO = 4.50

def lataa_data(db_polku: str = DB_POLKU) -> pd.DataFrame:
    if not Path(db_polku).exists(): return pd.DataFrame()
    yhteys = sqlite3.connect(db_polku)
    df = pd.read_sql_query(f"SELECT * FROM {TAULU}", yhteys)
    yhteys.close()
    return df

def laske_yleinen_voittoprosentti(df: pd.DataFrame, joukkue: str) -> float:
    koti_pelit = df[df["Kotijoukkue"] == joukkue]
    vieras_pelit = df[df["Vierasjoukkue"] == joukkue]
    voitot = (koti_pelit["Koti_Juoksut"] > koti_pelit["Vieras_Juoksut"]).sum() + \
             (vieras_pelit["Vieras_Juoksut"] > vieras_pelit["Koti_Juoksut"]).sum()
    yhteensa = len(koti_pelit) + len(vieras_pelit)
    return voitot / yhteensa if yhteensa > 0 else 0.5

def laske_h2h_voittoprosentti(df: pd.DataFrame, koti: str, vieras: str) -> tuple[float, int]:
    h2h = df[((df["Kotijoukkue"] == koti) & (df["Vierasjoukkue"] == vieras)) |
             ((df["Kotijoukkue"] == vieras) & (df["Vierasjoukkue"] == koti))]
    if len(h2h) == 0: return 0.5, 0
    k_voitot = ((h2h["Kotijoukkue"] == koti) & (h2h["Koti_Juoksut"] > h2h["Vieras_Juoksut"])).sum() + \
               ((h2h["Vierasjoukkue"] == koti) & (h2h["Vieras_Juoksut"] > h2h["Koti_Juoksut"])).sum()
    return k_voitot / len(h2h), len(h2h)

def laske_yhdistetty_xfip(aloittaja_xfip: float, ip_start: float, bullpen_xfip: float) -> float:
    """Laskee joukkueen todellisen xFIP:n otteluun (Aloittaja + Bullpen)."""
    # Varmistetaan, ettei aloitussyöttäjä voi syöttää yli 9 vuoroparia
    aloittajan_osuus = min(1.0, ip_start / 9.0)
    bullpenin_osuus = 1.0 - aloittajan_osuus
    return (aloittaja_xfip * aloittajan_osuus) + (bullpen_xfip * bullpenin_osuus)

def laske_juoksuodottama(koti: str, vieras: str, df: pd.DataFrame, koti_xfip: float, vieras_xfip: float) -> dict:
    k_pisteet = float(pd.concat([df[df["Kotijoukkue"] == koti]["Koti_Juoksut"], df[df["Vierasjoukkue"] == koti]["Vieras_Juoksut"]]).mean() or MLB_JUOKSU_KESKIARVO)
    v_pisteet = float(pd.concat([df[df["Kotijoukkue"] == vieras]["Koti_Juoksut"], df[df["Vierasjoukkue"] == vieras]["Vieras_Juoksut"]]).mean() or MLB_JUOKSU_KESKIARVO)
    k_paastot = float(pd.concat([df[df["Kotijoukkue"] == vieras]["Vieras_Juoksut"], df[df["Vierasjoukkue"] == vieras]["Koti_Juoksut"]]).mean() or MLB_JUOKSU_KESKIARVO)
    v_paastot = float(pd.concat([df[df["Kotijoukkue"] == koti]["Vieras_Juoksut"], df[df["Vierasjoukkue"] == koti]["Koti_Juoksut"]]).mean() or MLB_JUOKSU_KESKIARVO)

    if koti_xfip and vieras_xfip:
        # Vierasjoukkueen syöttäminen estää kotijoukkueen juoksuja ja päinvastoin
        k_odotus = (PAINO_JO_HYOKKAYS * k_pisteet + PAINO_JO_PUOLUSTUS * k_paastot + PAINO_JO_XFIP * vieras_xfip)
        v_odotus = (PAINO_JO_HYOKKAYS * v_pisteet + PAINO_JO_PUOLUSTUS * v_paastot + PAINO_JO_XFIP * koti_xfip)
    else:
        k_odotus = 0.5 * k_pisteet + 0.5 * k_paastot
        v_odotus = 0.5 * v_pisteet + 0.5 * v_paastot

    return {"koti_odotus": round(k_odotus, 2), "vieras_odotus": round(v_odotus, 2), "total": round(k_odotus + v_odotus, 2)}

def laske_todennakoisyys(
    koti: str, vieras: str, df: pd.DataFrame = None, db_polku: str = DB_POLKU,
    koti_aloittaja_xfip: float = None, koti_ip_start: float = None, koti_bullpen: float = None,
    vieras_aloittaja_xfip: float = None, vieras_ip_start: float = None, vieras_bullpen: float = None
) -> dict:
    if df is None: df = lataa_data(db_polku)

    k_yleinen = laske_yleinen_voittoprosentti(df, koti)
    v_yleinen = laske_yleinen_voittoprosentti(df, vieras)
    k_norm = k_yleinen / (k_yleinen + v_yleinen) if (k_yleinen + v_yleinen) > 0 else 0.5
    v_norm = 1.0 - k_norm

    h2h_k, h2h_n = laske_h2h_voittoprosentti(df, koti, vieras)
    h2h_v = 1.0 - h2h_k

    xfip_kaytossa = all(v is not None for v in [koti_aloittaja_xfip, vieras_aloittaja_xfip, koti_bullpen, vieras_bullpen])

    if xfip_kaytossa:
        # 1. Lasketaan joukkueiden yhdistetty xFIP (Aloittaja + Bullpen)
        koti_total_xfip = laske_yhdistetty_xfip(koti_aloittaja_xfip, koti_ip_start, koti_bullpen)
        vieras_total_xfip = laske_yhdistetty_xfip(vieras_aloittaja_xfip, vieras_ip_start, vieras_bullpen)

        # 2. VAPAUTETTU MATEMATIIKKA (Ei max/min rajoja!)
        koti_xfip_score = XFIP_REFERENSSI / koti_total_xfip
        vieras_xfip_score = XFIP_REFERENSSI / vieras_total_xfip
        summa_xfip = koti_xfip_score + vieras_xfip_score
        
        k_xfip_norm = koti_xfip_score / summa_xfip
        v_xfip_norm = vieras_xfip_score / summa_xfip

        k_final = (PAINO_YLEINEN_XFIP * k_norm) + (PAINO_H2H_XFIP * h2h_k) + (PAINO_XFIP * k_xfip_norm)
        v_final = (PAINO_YLEINEN_XFIP * v_norm) + (PAINO_H2H_XFIP * h2h_v) + (PAINO_XFIP * v_xfip_norm)
    else:
        koti_total_xfip = vieras_total_xfip = None
        k_final = (0.70 * k_norm) + (0.30 * h2h_k)
        v_final = (0.70 * v_norm) + (0.30 * h2h_v)

    kokonais = k_final + v_final
    juoksut = laske_juoksuodottama(koti, vieras, df, koti_total_xfip, vieras_total_xfip)

    return {
        "koti_voitto_tod": round(k_final / kokonais, 4), "vieras_voitto_tod": round(v_final / kokonais, 4),
        "k_yleinen": k_yleinen, "v_yleinen": v_yleinen, "h2h_k": h2h_k, "h2h_n": h2h_n,
        "xfip_kaytossa": xfip_kaytossa, "koti_total_xfip": koti_total_xfip, "vieras_total_xfip": vieras_total_xfip,
        "k_odotus": juoksut["koti_odotus"], "v_odotus": juoksut["vieras_odotus"], "total_odotus": juoksut["total"]
    }