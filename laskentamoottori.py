"""
laskentamoottori.py
===================
MLB-vedonlyönnin todennäköisyyslaskuri (MVP)

Lukee ottelutulokset SQLite-tietokannasta ja laskee
yksinkertaisen todennäköisyysarvion kahdelle joukkueelle.

Painotuslogiikka:
  - 70% yleinen voittoprosentti (koko kausi)
  - 30% keskinäiset ottelut (head-to-head)
"""

import sqlite3
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# VAKIOT
# ---------------------------------------------------------------------------
DB_POLKU = "mlb_historical.db"          # Tietokannan sijainti
TAULU    = "ottelutulokset_2025"         # Käytettävä taulu

# Painotukset todennäköisyyslaskennassa (summa = 1.0)
PAINO_YLEINEN   = 0.70   # yleinen voittoprosentti koko kaudelta
PAINO_H2H       = 0.30   # keskinäiset ottelut (head-to-head)


# ---------------------------------------------------------------------------
# DATAN LATAUS
# ---------------------------------------------------------------------------

def lataa_data(db_polku: str = DB_POLKU) -> pd.DataFrame:
    """
    Lukee kaikki rivit taulusta 'ottelutulokset_2025' Pandas-dataframeen.

    Palauttaa:
        pd.DataFrame – sarakkeet: Paivamaara, Kotijoukkue, Koti_Juoksut,
                                   Vierasjoukkue, Vieras_Juoksut
    """
    if not Path(db_polku).exists():
        raise FileNotFoundError(
            f"Tietokantaa '{db_polku}' ei löydy. "
            "Varmista, että mlb_historical.db on samassa hakemistossa."
        )

    yhteys = sqlite3.connect(db_polku)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {TAULU}", yhteys)
    finally:
        yhteys.close()

    # Tarkistetaan, että vaaditut sarakkeet löytyvät
    vaaditut = {"Paivamaara", "Kotijoukkue", "Koti_Juoksut",
                "Vierasjoukkue", "Vieras_Juoksut"}
    puuttuvat = vaaditut - set(df.columns)
    if puuttuvat:
        raise ValueError(f"Taulusta puuttuu sarakkeet: {puuttuvat}")

    return df


# ---------------------------------------------------------------------------
# APUFUNKTIOT
# ---------------------------------------------------------------------------

def laske_yleinen_voittoprosentti(df: pd.DataFrame, joukkue: str) -> float:
    """
    Laskee joukkueen yleisen voittoprosentin koko kaudelta
    (kotipelit + vieraspelit yhteenlaskettuna).

    Palauttaa:
        float – voittoprosentti välillä [0.0, 1.0],
                tai 0.5 jos joukkueella ei ole yhtään peliä datassa.
    """
    # Kotipelit
    koti_pelit  = df[df["Kotijoukkue"]   == joukkue]
    koti_voitot = (koti_pelit["Koti_Juoksut"] > koti_pelit["Vieras_Juoksut"]).sum()

    # Vieraspelit
    vieras_pelit  = df[df["Vierasjoukkue"]  == joukkue]
    vieras_voitot = (vieras_pelit["Vieras_Juoksut"] > vieras_pelit["Koti_Juoksut"]).sum()

    yhteensa_pelit  = len(koti_pelit) + len(vieras_pelit)
    yhteensa_voitot = koti_voitot + vieras_voitot

    if yhteensa_pelit == 0:
        # Ei dataa → palautetaan neutraali arvo
        return 0.5

    return yhteensa_voitot / yhteensa_pelit


def laske_h2h_voittoprosentti(
    df: pd.DataFrame, koti: str, vieras: str
) -> tuple[float, int]:
    """
    Laskee kotijoukkueen voittoprosentin *keskinäisissä* otteluissa
    näitä kahta joukkuetta vastaan.

    Palauttaa:
        (voittoprosentti: float, ottelumaara: int)
        Voittoprosentti on kotijoukkueen osuus [0.0, 1.0].
        Jos keskinäisiä pelejä ei ole, palautetaan (0.5, 0).
    """
    # Suodatetaan pelit, joissa nämä kaksi joukkuetta kohtaavat
    h2h = df[
        ((df["Kotijoukkue"]   == koti)   & (df["Vierasjoukkue"] == vieras)) |
        ((df["Kotijoukkue"]   == vieras) & (df["Vierasjoukkue"] == koti))
    ].copy()

    if len(h2h) == 0:
        return 0.5, 0

    # Lasketaan kotijoukkueen voitot keskinäisissä peleissä
    # (riippumatta siitä, pelattiinko koti- vai vieraana)
    koti_voitot = 0
    for _, rivi in h2h.iterrows():
        if rivi["Kotijoukkue"] == koti:
            # Koti pelasi kotona
            if rivi["Koti_Juoksut"] > rivi["Vieras_Juoksut"]:
                koti_voitot += 1
        else:
            # Koti pelasi vieraana
            if rivi["Vieras_Juoksut"] > rivi["Koti_Juoksut"]:
                koti_voitot += 1

    return koti_voitot / len(h2h), len(h2h)


# ---------------------------------------------------------------------------
# PÄÄFUNKTIO: todennäköisyyslaskuri
# ---------------------------------------------------------------------------

def laske_todennakoisyys(
    koti: str,
    vieras: str,
    df: pd.DataFrame | None = None,
    db_polku: str = DB_POLKU,
) -> dict:
    """
    Laskee yksinkertaisen todennäköisyysarvion ottelulle koti vs. vieras.

    Parametrit:
        koti     – kotijoukkueen nimi (täsmälleen kuten tietokannassa)
        vieras   – vierasjoukkueen nimi
        df       – valmiiksi ladattu DataFrame (valinnainen; jos None, ladataan itse)
        db_polku – tietokannan polku (käytetään vain jos df=None)

    Palauttaa dict:
        {
            "kotijoukkue":        str,
            "vierasjoukkue":      str,
            "koti_voitto_tod":    float,   # 0–1
            "vieras_voitto_tod":  float,   # 0–1
            "koti_yleinen_vp":    float,
            "vieras_yleinen_vp":  float,
            "h2h_koti_vp":        float,
            "h2h_ottelut":        int,
        }
    """
    if df is None:
        df = lataa_data(db_polku)

    # --- 1. Yleinen voittoprosentti ---
    koti_yleinen   = laske_yleinen_voittoprosentti(df, koti)
    vieras_yleinen = laske_yleinen_voittoprosentti(df, vieras)

    # Normalisoidaan niin, että koti + vieras = 1.0
    summa_yleinen = koti_yleinen + vieras_yleinen
    if summa_yleinen == 0:
        koti_norm   = 0.5
        vieras_norm = 0.5
    else:
        koti_norm   = koti_yleinen   / summa_yleinen
        vieras_norm = vieras_yleinen / summa_yleinen

    # --- 2. Head-to-head ---
    h2h_koti_vp, h2h_maara = laske_h2h_voittoprosentti(df, koti, vieras)
    h2h_vieras_vp = 1.0 - h2h_koti_vp

    # --- 3. Painotettu yhdistelmä ---
    koti_yhdistetty   = PAINO_YLEINEN * koti_norm   + PAINO_H2H * h2h_koti_vp
    vieras_yhdistetty = PAINO_YLEINEN * vieras_norm + PAINO_H2H * h2h_vieras_vp

    # Normalisoidaan viimeiset todennäköisyydet (pitäisi jo summautua ~1:een,
    # mutta varmuuden vuoksi)
    kokonais = koti_yhdistetty + vieras_yhdistetty
    koti_final   = koti_yhdistetty   / kokonais
    vieras_final = vieras_yhdistetty / kokonais

    return {
        "kotijoukkue":       koti,
        "vierasjoukkue":     vieras,
        "koti_voitto_tod":   round(koti_final,   4),
        "vieras_voitto_tod": round(vieras_final, 4),
        "koti_yleinen_vp":   round(koti_yleinen, 4),
        "vieras_yleinen_vp": round(vieras_yleinen, 4),
        "h2h_koti_vp":       round(h2h_koti_vp,  4),
        "h2h_ottelut":       h2h_maara,
    }


# ---------------------------------------------------------------------------
# TULOSTUSAPURI
# ---------------------------------------------------------------------------

def tulosta_ennuste(tulos: dict) -> None:
    """Tulostaa laskentatuloksen selkeästi terminaaliin."""
    viiva = "─" * 52
    print(f"\n{viiva}")
    print(f"  ⚾  OTTELUENNUSTE (MVP-laskuri)")
    print(viiva)
    print(f"  Kotijoukkue  : {tulos['kotijoukkue']}")
    print(f"  Vierasjoukkue: {tulos['vierasjoukkue']}")
    print(viiva)

    # Yleinen voittoprosentti
    print(f"  Yleinen voitto-% (koko kausi):")
    print(f"    {tulos['kotijoukkue']:<28} {tulos['koti_yleinen_vp']*100:5.1f} %")
    print(f"    {tulos['vierasjoukkue']:<28} {tulos['vieras_yleinen_vp']*100:5.1f} %")

    # H2H
    if tulos["h2h_ottelut"] > 0:
        print(f"\n  Head-to-head ({tulos['h2h_ottelut']} ottelua):")
        print(f"    {tulos['kotijoukkue']:<28} {tulos['h2h_koti_vp']*100:5.1f} %")
        print(f"    {tulos['vierasjoukkue']:<28} {(1-tulos['h2h_koti_vp'])*100:5.1f} %")
    else:
        print(f"\n  Head-to-head: ei keskinäisiä otteluita datassa")
        print(f"    (käytetään neutraalia 50/50)")

    # Lopullinen ennuste
    print(f"\n  ▶  LOPULLINEN ENNUSTE  "
          f"(paino: {int(PAINO_YLEINEN*100)}% yleinen / {int(PAINO_H2H*100)}% H2H):")
    print(f"    {tulos['kotijoukkue']:<28} {tulos['koti_voitto_tod']*100:5.1f} %")
    print(f"    {tulos['vierasjoukkue']:<28} {tulos['vieras_voitto_tod']*100:5.1f} %")
    print(f"{viiva}\n")


# ---------------------------------------------------------------------------
# TESTIAJO  (suoritetaan kun skripti ajetaan suoraan: python laskentamoottori.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    # -----------------------------------------------------------------------
    # DEMO-DATA: luodaan väliaikainen SQLite-tietokanta testitarkoituksiin,
    # jotta skripti toimii heti ilman oikeaa mlb_historical.db-tiedostoa.
    # Poista tämä lohko kun oikea tietokanta on käytettävissä.
    # -----------------------------------------------------------------------
    import os
    import numpy as np

    DEMO_DB = "mlb_historical.db"
    if not Path(DEMO_DB).exists():
        print("ℹ️  Demo: luodaan väliaikainen testikanta...")
        rng = np.random.default_rng(42)

        joukkueet = [
            "New York Yankees", "Boston Red Sox",
            "Los Angeles Dodgers", "Chicago Cubs",
            "Houston Astros",
        ]

        rivit = []
        # Kotipelit jokaiselle joukkueparille
        for i, kj in enumerate(joukkueet):
            for j, vj in enumerate(joukkueet):
                if kj == vj:
                    continue
                for _ in range(rng.integers(3, 8)):
                    k_juoks = int(rng.integers(0, 12))
                    v_juoks = int(rng.integers(0, 12))
                    if k_juoks == v_juoks:
                        v_juoks += 1   # ei tasapelejä baseballissa
                    rivit.append({
                        "Paivamaara":    f"2025-{rng.integers(4,10):02d}-{rng.integers(1,28):02d}",
                        "Kotijoukkue":   kj,
                        "Koti_Juoksut":  k_juoks,
                        "Vierasjoukkue": vj,
                        "Vieras_Juoksut": v_juoks,
                    })

        demo_df = pd.DataFrame(rivit)
        yhteys  = sqlite3.connect(DEMO_DB)
        demo_df.to_sql(TAULU, yhteys, if_exists="replace", index=False)
        yhteys.close()
        print(f"   → Luotu {len(rivit)} testiottelua tietokantaan '{DEMO_DB}'.\n")

    # -----------------------------------------------------------------------
    # Ladataan data kerran ja käytetään sitä molemmissa testiajoissa
    # -----------------------------------------------------------------------
    print("Ladataan data tietokannasta...")
    data = lataa_data()
    print(f"Datassa {len(data)} ottelua, {data['Kotijoukkue'].nunique()} joukkuetta.\n")

    # Testiajo 1
    tulos1 = laske_todennakoisyys("New York Yankees", "Boston Red Sox", df=data)
    tulosta_ennuste(tulos1)

    # Testiajo 2
    tulos2 = laske_todennakoisyys("Los Angeles Dodgers", "Chicago Cubs", df=data)
    tulosta_ennuste(tulos2)

    # Testiajo 3: joukkue, jota ei ole datassa → demonstroi fallback-logiikan
    tulos3 = laske_todennakoisyys("Houston Astros", "New York Yankees", df=data)
    tulosta_ennuste(tulos3)