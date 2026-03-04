"""
fetch_statcast.py
=================
Hakee MLB Statcast-datan (jokainen syöttö) kaudelta 2025
pybaseball-kirjastolla pätkittäin ja tallentaa SQLite-kantaan.

Strategia:
  - Koko kausi pilkotaan enintään 10 päivän paloihin
  - Jokainen pala haetaan erikseen time.sleep-tauolla
  - Epäonnistunut pala kirjataan ja ohitetaan (ei kaada koko ajoa)
  - Kaikki onnistuneet palat yhdistetään ja tallennetaan kantaan

Käyttö:
    pip install pybaseball pandas
    python fetch_statcast.py

Varoitus: Koko kauden haku kestää tyypillisesti 30–90 minuuttia.
"""

import sqlite3
import time
from datetime import date, timedelta

import pandas as pd
import pybaseball

# ---------------------------------------------------------------------------
# VAKIOT – muuta näitä tarpeen mukaan
# ---------------------------------------------------------------------------
DB_POLKU        = "mlb_historical.db"     # Kohdetietokanta
TAULU           = "statcast_2025"          # Kohdetaulu

KAUSI_ALKU      = date(2025, 2, 15)        # Harjoituskauden alku
KAUSI_LOPPU     = date(2025, 11, 5)        # Pudotuspelien loppu (arvio)

PALAN_KOKO_PV   = 10                       # Päiviä per haku (max ~10–14 suositeltava)
TAUKO_SEKUNTIA  = 5                        # Tauko hakujen välillä (älä laita alle 3)

# ---------------------------------------------------------------------------
# APUFUNKTIO: aikavälin pilkkominen paloihin
# ---------------------------------------------------------------------------

def luo_aikavalit(alku: date, loppu: date, palan_koko: int) -> list[tuple[date, date]]:
    """
    Pilkkoo aikavälin [alku, loppu] enintään `palan_koko` päivän paloihin.

    Palauttaa:
        list[tuple[date, date]] – lista (pala_alku, pala_loppu) -pareista
    """
    palat = []
    nykyinen = alku
    while nykyinen <= loppu:
        pala_loppu = min(nykyinen + timedelta(days=palan_koko - 1), loppu)
        palat.append((nykyinen, pala_loppu))
        nykyinen = pala_loppu + timedelta(days=1)
    return palat


# ---------------------------------------------------------------------------
# APUFUNKTIO: yksittäisen palan haku
# ---------------------------------------------------------------------------

def hae_pala(alku: date, loppu: date) -> pd.DataFrame | None:
    """
    Hakee Statcast-datan yhdeltä aikaväliltä pybaseballin avulla.

    Palauttaa:
        pd.DataFrame jos haku onnistui, None jos epäonnistui.
    """
    alku_str  = alku.strftime("%Y-%m-%d")
    loppu_str = loppu.strftime("%Y-%m-%d")

    try:
        df = pybaseball.statcast(
            start_dt=alku_str,
            end_dt=loppu_str,
            parallel=False,      # Ei rinnakkaishakuja – stabiilimpi
        )

        if df is None or df.empty:
            print(f"     ⚠️  Ei dataa välillä {alku_str} – {loppu_str} (off-season tai tyhjä)")
            return None

        print(f"     ✅ {alku_str} – {loppu_str}: {len(df):>7,} syöttöä haettu")
        return df

    except Exception as e:
        print(f"     ❌ VIRHE välillä {alku_str} – {loppu_str}: {type(e).__name__}: {e}")
        print(f"        → Ohitetaan tämä pala, jatketaan seuraavaan.")
        return None


# ---------------------------------------------------------------------------
# APUFUNKTIO: tallennus tietokantaan
# ---------------------------------------------------------------------------

def tallenna_kantaan(df: pd.DataFrame, db_polku: str = DB_POLKU) -> None:
    """
    Tallentaa yhdistetyn DataFramen SQLite-tietokantaan.
    Korvaa taulun jos se on jo olemassa.
    """
    try:
        yhteys = sqlite3.connect(db_polku)
        # Tallennetaan paloissa muistinkäytön minimoimiseksi (chunksize)
        df.to_sql(
            TAULU,
            yhteys,
            if_exists="replace",
            index=False,
            chunksize=10_000,
        )
        yhteys.close()
        print(f"\n✅ Tallennettu {len(df):,} riviä tauluun '{TAULU}' ({db_polku})")

    except sqlite3.Error as e:
        raise RuntimeError(f"❌ SQLite-virhe tallennuksessa: {e}") from e


# ---------------------------------------------------------------------------
# PÄÄOHJELMA
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    # ── Pybaseballin välimuistin kytkeminen pois – hakee aina tuoreen datan
    pybaseball.cache.enable()   # Kommentoi pois jos haluat välimuistia

    viiva = "═" * 62
    print(f"\n{viiva}")
    print(f"  ⚾  STATCAST 2025 – MASSIIVINEN DATAHAKU")
    print(viiva)
    print(f"  Aikaväli : {KAUSI_ALKU} – {KAUSI_LOPPU}")
    print(f"  Palan koko: {PALAN_KOKO_PV} päivää")
    print(f"  Tauko    : {TAUKO_SEKUNTIA} s hakujen välillä")
    print(f"  Kohde    : {DB_POLKU} → taulu '{TAULU}'")
    print(f"{viiva}\n")

    # ── Luo aikavälipalat ──
    palat = luo_aikavalit(KAUSI_ALKU, KAUSI_LOPPU, PALAN_KOKO_PV)
    print(f"📦 Paloja yhteensä: {len(palat)}\n")

    # ── Hakusilmukka ──
    onnistuneet: list[pd.DataFrame] = []
    epaonnistuneet: list[tuple[date, date]] = []

    for i, (alku, loppu) in enumerate(palat, start=1):
        print(f"[{i:>3}/{len(palat)}] Haetaan {alku} – {loppu} ...", end="  ")

        pala_df = hae_pala(alku, loppu)

        if pala_df is not None:
            onnistuneet.append(pala_df)
        else:
            epaonnistuneet.append((alku, loppu))

        # Tauko – paitsi viimeisen palan jälkeen
        if i < len(palat):
            time.sleep(TAUKO_SEKUNTIA)

    # ── Yhteenveto hausta ──
    print(f"\n{viiva}")
    print(f"  HAKU VALMIS")
    print(f"  Onnistuneet palat : {len(onnistuneet)} / {len(palat)}")
    print(f"  Epäonnistuneet    : {len(epaonnistuneet)}")
    if epaonnistuneet:
        for (a, b) in epaonnistuneet:
            print(f"    ✗ {a} – {b}")
    print(viiva)

    # ── Yhdistäminen ja tallennus ──
    if not onnistuneet:
        print("\n❌ Yhtään datapakettia ei saatu – tarkista verkko ja pybaseball-asennus.")
    else:
        print(f"\n🔗 Yhdistetään {len(onnistuneet)} datapakettia ...")
        yhdistetty = pd.concat(onnistuneet, ignore_index=True)

        # Poistetaan mahdolliset duplikaatit (päivämäärien reunat saattavat overlap)
        ennen = len(yhdistetty)
        if "pitch_number" in yhdistetty.columns and "game_pk" in yhdistetty.columns:
            yhdistetty = yhdistetty.drop_duplicates(
                subset=["game_pk", "at_bat_number", "pitch_number"]
            )
        jalkeen = len(yhdistetty)
        if ennen != jalkeen:
            print(f"   → Poistettu {ennen - jalkeen:,} duplikaattia")

        print(f"   → Yhteensä {len(yhdistetty):,} uniikkia syöttöä")

        # Tilastoitu sarakeinfo
        print(f"   → Sarakkeita: {len(yhdistetty.columns)}")
        print(f"\n💾 Tallennetaan tietokantaan ...")
        tallenna_kantaan(yhdistetty)

        # ── Loppuyhteenveto ──
        print(f"\n{viiva}")
        print(f"  📊 LOPPUTULOS")
        print(viiva)
        print(f"  Rivejä tallennettu : {len(yhdistetty):>10,}")
        print(f"  Sarakkeita         : {len(yhdistetty.columns):>10,}")
        if "game_date" in yhdistetty.columns:
            print(f"  Päivämääräväli     : {yhdistetty['game_date'].min()} – {yhdistetty['game_date'].max()}")
        if "player_name" in yhdistetty.columns:
            print(f"  Uniikkeja pelaajia : {yhdistetty['player_name'].nunique():>10,}")
        if "home_team" in yhdistetty.columns:
            print(f"  Uniikkeja joukkueita: {yhdistetty['home_team'].nunique():>9,}")
        print(viiva)
        print("\n🎉 Statcast-data valmis käytettäväksi!\n")