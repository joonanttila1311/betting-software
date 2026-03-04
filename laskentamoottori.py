"""
laskentamoottori.py
===================
MLB-vedonlyönnin todennäköisyyslaskuri (MVP)

Lukee ottelutulokset SQLite-tietokannasta ja laskee
yksinkertaisen todennäköisyysarvion kahdelle joukkueelle
sekä juoksuodottaman (Expected Runs / Over-Under).

Todennäköisyyslaskennan painotukset:
  Ilman ERA: 70% yleinen voittoprosentti + 30% H2H
  ERA:n kanssa: 60% yleinen VP + 20% H2H + 20% ERA-vertailu

Juoksuodottaman painotukset:
  Ilman ERA: 50% joukkueen hyökkäyskeskiarvo + 50% vastustajan puolustuskeskiarvo
  ERA:n kanssa: 35% hyökkäys + 35% puolustus + 30% syöttäjän ERA-skaalattu arvo
"""

import sqlite3
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# VAKIOT
# ---------------------------------------------------------------------------
DB_POLKU = "mlb_historical.db"
TAULU    = "ottelutulokset_2025"

# Todennäköisyyspainotukset ilman ERA (summa = 1.0)
PAINO_YLEINEN = 0.70
PAINO_H2H     = 0.30

# Todennäköisyyspainotukset ERA:n kanssa (summa = 1.0)
PAINO_YLEINEN_ERA = 0.60
PAINO_H2H_ERA     = 0.20
PAINO_ERA         = 0.20

# ERA-referenssiarvo normalisointia varten (MLB historiallinen keskiarvo ~4.20)
ERA_REFERENSSI = 4.20

# Juoksuodottaman painotukset ERA:n kanssa (summa = 1.0)
PAINO_JO_HYOKKAYS  = 0.35   # joukkueen oma pisteytyskeskiarvo
PAINO_JO_PUOLUSTUS = 0.35   # vastustajan päästämien juoksujen keskiarvo
PAINO_JO_ERA       = 0.30   # syöttäjän ERA muunnettuna juoksuarvioksi

# MLB:n historiallinen juoksukeskiarvo per ottelu per joukkue (~4.5)
MLB_JUOKSU_KESKIARVO = 4.50


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

    vaaditut = {"Paivamaara", "Kotijoukkue", "Koti_Juoksut",
                "Vierasjoukkue", "Vieras_Juoksut"}
    puuttuvat = vaaditut - set(df.columns)
    if puuttuvat:
        raise ValueError(f"Taulusta puuttuu sarakkeet: {puuttuvat}")

    return df


# ---------------------------------------------------------------------------
# APUFUNKTIOT – voittotodennäköisyys
# ---------------------------------------------------------------------------

def laske_yleinen_voittoprosentti(df: pd.DataFrame, joukkue: str) -> float:
    """
    Laskee joukkueen yleisen voittoprosentin koko kaudelta.
    Palauttaa 0.5 jos dataa ei löydy.
    """
    koti_pelit    = df[df["Kotijoukkue"]  == joukkue]
    koti_voitot   = (koti_pelit["Koti_Juoksut"] > koti_pelit["Vieras_Juoksut"]).sum()
    vieras_pelit  = df[df["Vierasjoukkue"] == joukkue]
    vieras_voitot = (vieras_pelit["Vieras_Juoksut"] > vieras_pelit["Koti_Juoksut"]).sum()

    yhteensa = len(koti_pelit) + len(vieras_pelit)
    if yhteensa == 0:
        return 0.5
    return (koti_voitot + vieras_voitot) / yhteensa


def laske_h2h_voittoprosentti(
    df: pd.DataFrame, koti: str, vieras: str
) -> tuple[float, int]:
    """
    Laskee kotijoukkueen voittoprosentin keskinäisissä otteluissa.
    Palauttaa (0.5, 0) jos pelejä ei ole.
    """
    h2h = df[
        ((df["Kotijoukkue"]  == koti)   & (df["Vierasjoukkue"] == vieras)) |
        ((df["Kotijoukkue"]  == vieras) & (df["Vierasjoukkue"] == koti))
    ].copy()

    if len(h2h) == 0:
        return 0.5, 0

    koti_voitot = 0
    for _, rivi in h2h.iterrows():
        if rivi["Kotijoukkue"] == koti:
            if rivi["Koti_Juoksut"] > rivi["Vieras_Juoksut"]:
                koti_voitot += 1
        else:
            if rivi["Vieras_Juoksut"] > rivi["Koti_Juoksut"]:
                koti_voitot += 1

    return koti_voitot / len(h2h), len(h2h)


# ---------------------------------------------------------------------------
# APUFUNKTIO – juoksuodottama
# ---------------------------------------------------------------------------

def laske_juoksuodottama(
    koti: str,
    vieras: str,
    df: pd.DataFrame,
    koti_era: float | None = None,
    vieras_era: float | None = None,
) -> dict:
    """
    Laskee kummankin joukkueen odotetun juoksumäärän tähän otteluun.

    Logiikka (kullekin joukkueelle erikseen):
      1. Joukkueen historiallinen pisteytyskeskiarvo (tehtyjen juoksujen ka.)
      2. Vastustajan historiallinen puolustuskeskiarvo (päästettyjen juoksujen ka.)
      3. Jos syöttäjän ERA annettu, muunnetaan se 9 vuoroparin juoksumääräksi
         (ERA = juoksut / 9 inningiä) ja painotetaan mukaan.

    Fallback: jos dataa ei ole, käytetään MLB-keskiarvoa (4.50 juoksua/ottelu).

    Palauttaa:
        dict:
            koti_odotus   – kotijoukkueen odotetut juoksut (float)
            vieras_odotus – vierasjoukkueen odotetut juoksut (float)
            total_odotus  – odotettu kokonaisjuoksumäärä (summa)
            koti_pisteet_ka   – historiallinen pisteytyskeskiarvo
            vieras_pisteet_ka – historiallinen pisteytyskeskiarvo
            koti_paastot_ka   – historiallinen päästöt-ka (vastustajan näkökulmasta)
            vieras_paastot_ka – historiallinen päästöt-ka
    """

    # ── 1. Historiallinen pisteytyskeskiarvo (joukkueen tekemät juoksut) ──
    def pisteet_ka(joukkue: str) -> float:
        """Joukkueen keskimääräiset tehdyt juoksut per ottelu koko kaudelta."""
        koti_p   = df[df["Kotijoukkue"]  == joukkue]["Koti_Juoksut"]
        vieras_p = df[df["Vierasjoukkue"] == joukkue]["Vieras_Juoksut"]
        kaikki   = pd.concat([koti_p, vieras_p])
        return float(kaikki.mean()) if len(kaikki) > 0 else MLB_JUOKSU_KESKIARVO

    # ── 2. Historiallinen puolustuskeskiarvo (vastustajan päästämät) ──
    def paastot_ka(joukkue: str) -> float:
        """Joukkueen vastustajille keskimäärin päästämät juoksut per ottelu."""
        koti_p   = df[df["Kotijoukkue"]  == joukkue]["Vieras_Juoksut"]
        vieras_p = df[df["Vierasjoukkue"] == joukkue]["Koti_Juoksut"]
        kaikki   = pd.concat([koti_p, vieras_p])
        return float(kaikki.mean()) if len(kaikki) > 0 else MLB_JUOKSU_KESKIARVO

    koti_pisteet   = pisteet_ka(koti)
    vieras_pisteet = pisteet_ka(vieras)
    # Puolustus: kotijoukkue päästää joukkueelle "vieras" ja päinvastoin
    koti_paastot   = paastot_ka(vieras)   # vieras päästää kotia vastaan
    vieras_paastot = paastot_ka(koti)     # koti päästää vierasta vastaan

    era_kaytossa = (koti_era is not None) and (vieras_era is not None)

    if era_kaytossa:
        # ERA → juoksua per ottelu (ERA on juoksut per 9 inningiä, ottelu ~9 in.)
        # Klipatataan järkevälle välille [1.0, 8.0]
        koti_era_juoksut   = max(1.0, min(8.0, vieras_era))  # vierassyöttäjä päästää kotia
        vieras_era_juoksut = max(1.0, min(8.0, koti_era))    # kotisyöttäjä päästää vierasta

        # Painotettu yhdistelmä ERA:n kanssa
        koti_odotus = (
            PAINO_JO_HYOKKAYS  * koti_pisteet
            + PAINO_JO_PUOLUSTUS * koti_paastot
            + PAINO_JO_ERA       * koti_era_juoksut
        )
        vieras_odotus = (
            PAINO_JO_HYOKKAYS  * vieras_pisteet
            + PAINO_JO_PUOLUSTUS * vieras_paastot
            + PAINO_JO_ERA       * vieras_era_juoksut
        )
    else:
        # Ilman ERA: tasapainoinen hyökkäys/puolustus-yhdistelmä
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


# ---------------------------------------------------------------------------
# PÄÄFUNKTIO: todennäköisyyslaskuri + juoksuodottama
# ---------------------------------------------------------------------------

def laske_todennakoisyys(
    koti: str,
    vieras: str,
    df: pd.DataFrame | None = None,
    db_polku: str = DB_POLKU,
    koti_era: float | None = None,
    vieras_era: float | None = None,
) -> dict:
    """
    Laskee voittotodennäköisyyden, True Odds -kertoimet
    ja juoksuodottaman ottelulle koti vs. vieras.

    Parametrit:
        koti       – kotijoukkueen nimi
        vieras     – vierasjoukkueen nimi
        df         – valmiiksi ladattu DataFrame (valinnainen)
        db_polku   – tietokannan polku (käytetään vain jos df=None)
        koti_era   – kotijoukkueen aloitussyöttäjän ERA (valinnainen)
        vieras_era – vierasjoukkueen aloitussyöttäjän ERA (valinnainen)

    Palauttaa dict (kaikki avaimet):
        kotijoukkue, vierasjoukkue,
        koti_voitto_tod, vieras_voitto_tod,
        koti_yleinen_vp, vieras_yleinen_vp,
        h2h_koti_vp, h2h_ottelut, era_kaytossa,
        koti_odotus, vieras_odotus, total_odotus,
        koti_pisteet_ka, vieras_pisteet_ka,
        koti_paastot_ka, vieras_paastot_ka
    """
    if df is None:
        df = lataa_data(db_polku)

    # ── 1. Yleinen voittoprosentti ──
    koti_yleinen   = laske_yleinen_voittoprosentti(df, koti)
    vieras_yleinen = laske_yleinen_voittoprosentti(df, vieras)

    summa_yleinen = koti_yleinen + vieras_yleinen
    if summa_yleinen == 0:
        koti_norm = vieras_norm = 0.5
    else:
        koti_norm   = koti_yleinen   / summa_yleinen
        vieras_norm = vieras_yleinen / summa_yleinen

    # ── 2. Head-to-head ──
    h2h_koti_vp, h2h_maara = laske_h2h_voittoprosentti(df, koti, vieras)
    h2h_vieras_vp = 1.0 - h2h_koti_vp

    # ── 3. ERA-komponentti ──
    era_kaytossa = (koti_era is not None) and (vieras_era is not None)

    if era_kaytossa:
        koti_era_score   = max(0.2, min(0.8, ERA_REFERENSSI / (koti_era   + 0.01)))
        vieras_era_score = max(0.2, min(0.8, ERA_REFERENSSI / (vieras_era + 0.01)))
        era_summa        = koti_era_score + vieras_era_score
        koti_era_norm    = koti_era_score   / era_summa
        vieras_era_norm  = vieras_era_score / era_summa

        koti_yhdistetty = (
            PAINO_YLEINEN_ERA * koti_norm
            + PAINO_H2H_ERA   * h2h_koti_vp
            + PAINO_ERA       * koti_era_norm
        )
        vieras_yhdistetty = (
            PAINO_YLEINEN_ERA * vieras_norm
            + PAINO_H2H_ERA   * h2h_vieras_vp
            + PAINO_ERA       * vieras_era_norm
        )
    else:
        koti_yhdistetty   = PAINO_YLEINEN * koti_norm   + PAINO_H2H * h2h_koti_vp
        vieras_yhdistetty = PAINO_YLEINEN * vieras_norm + PAINO_H2H * h2h_vieras_vp

    kokonais     = koti_yhdistetty + vieras_yhdistetty
    koti_final   = koti_yhdistetty   / kokonais
    vieras_final = vieras_yhdistetty / kokonais

    # ── 4. Juoksuodottama ──
    juoksut = laske_juoksuodottama(koti, vieras, df, koti_era, vieras_era)

    return {
        # Joukkueet
        "kotijoukkue":       koti,
        "vierasjoukkue":     vieras,
        # Voittotodennäköisyys
        "koti_voitto_tod":   round(koti_final,   4),
        "vieras_voitto_tod": round(vieras_final, 4),
        # Tilastokomponentit
        "koti_yleinen_vp":   round(koti_yleinen, 4),
        "vieras_yleinen_vp": round(vieras_yleinen, 4),
        "h2h_koti_vp":       round(h2h_koti_vp,  4),
        "h2h_ottelut":       h2h_maara,
        "era_kaytossa":      era_kaytossa,
        # Juoksuodottama
        "koti_odotus":       juoksut["koti_odotus"],
        "vieras_odotus":     juoksut["vieras_odotus"],
        "total_odotus":      juoksut["total_odotus"],
        "koti_pisteet_ka":   juoksut["koti_pisteet_ka"],
        "vieras_pisteet_ka": juoksut["vieras_pisteet_ka"],
        "koti_paastot_ka":   juoksut["koti_paastot_ka"],
        "vieras_paastot_ka": juoksut["vieras_paastot_ka"],
    }


# ---------------------------------------------------------------------------
# TULOSTUSAPURI
# ---------------------------------------------------------------------------

def tulosta_ennuste(tulos: dict) -> None:
    """Tulostaa laskentatuloksen selkeästi terminaaliin."""
    viiva = "─" * 56
    print(f"\n{viiva}")
    print(f"  ⚾  OTTELUENNUSTE")
    print(viiva)
    print(f"  {tulos['kotijoukkue']}  vs  {tulos['vierasjoukkue']}")
    print(viiva)
    print(f"  Voittotodennäköisyys:")
    print(f"    {tulos['kotijoukkue']:<28} {tulos['koti_voitto_tod']*100:5.1f} %")
    print(f"    {tulos['vierasjoukkue']:<28} {tulos['vieras_voitto_tod']*100:5.1f} %")
    print(f"\n  Juoksuodottama (Over/Under):")
    print(f"    {tulos['kotijoukkue']:<28} {tulos['koti_odotus']:4.1f} juoksua")
    print(f"    {tulos['vierasjoukkue']:<28} {tulos['vieras_odotus']:4.1f} juoksua")
    print(f"    {'Yhteensä (O/U-linja):':<28} {tulos['total_odotus']:4.1f} juoksua")
    print(f"  ERA käytössä: {tulos['era_kaytossa']}")
    print(f"{viiva}\n")


# ---------------------------------------------------------------------------
# TESTIAJO
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import numpy as np

    DEMO_DB = "mlb_historical.db"
    if not Path(DEMO_DB).exists():
        print("ℹ️  Demo: luodaan väliaikainen testikanta...")
        rng = np.random.default_rng(42)
        joukkueet = [
            "New York Yankees", "Boston Red Sox",
            "Los Angeles Dodgers", "Chicago Cubs", "Houston Astros",
        ]
        rivit = []
        for kj in joukkueet:
            for vj in joukkueet:
                if kj == vj:
                    continue
                for _ in range(rng.integers(3, 8)):
                    k = int(rng.integers(0, 12))
                    v = int(rng.integers(0, 12))
                    if k == v:
                        v += 1
                    rivit.append({
                        "Paivamaara":     f"2025-{rng.integers(4,10):02d}-{rng.integers(1,28):02d}",
                        "Kotijoukkue":    kj,
                        "Koti_Juoksut":   k,
                        "Vierasjoukkue":  vj,
                        "Vieras_Juoksut": v,
                    })
        demo_df = pd.DataFrame(rivit)
        yhteys  = sqlite3.connect(DEMO_DB)
        demo_df.to_sql(TAULU, yhteys, if_exists="replace", index=False)
        yhteys.close()
        print(f"   → Luotu {len(rivit)} testiottelua.\n")

    data = lataa_data()
    print(f"Datassa {len(data)} ottelua.\n")

    t1 = laske_todennakoisyys("New York Yankees", "Boston Red Sox", df=data)
    tulosta_ennuste(t1)

    t2 = laske_todennakoisyys(
        "New York Yankees", "Boston Red Sox",
        df=data, koti_era=2.85, vieras_era=4.60
    )
    tulosta_ennuste(t2)