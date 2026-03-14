"""
fetch_statcast.py
=================
Hakee MLB Statcast-datan fiksusti (Incremental Update).
Jos tietokannassa on jo dataa, hakee vain puuttuvat päivät (esim. eilisillan pelit)
ja lisää ne kantaan. Jos dataa ei ole, hakee kaiken määritellystä alusta asti.

Käyttö:
    pip install pybaseball pandas
    python fetch_statcast.py
"""

import sqlite3
import time
from datetime import date, timedelta, datetime

import pandas as pd
import pybaseball

# ---------------------------------------------------------------------------
# VAKIOT – muuta näitä tarpeen mukaan
# ---------------------------------------------------------------------------
DB_POLKU        = "mlb_historical.db"      # Kohdetietokanta
TAULU           = "statcast_2025"          # HUOM: Pidettiin vanha nimi yhteensopivuuden vuoksi!
OLETUS_ALKU     = date(2025, 2, 15)        # Mistä aloitetaan jos kanta on aivan tyhjä

PALAN_KOKO_PV   = 10                       # Päiviä per haku
TAUKO_SEKUNTIA  = 5                        # Tauko hakujen välillä

# ---------------------------------------------------------------------------
# APUFUNKTIO: Tietokannan tilan tarkistus
# ---------------------------------------------------------------------------

def hae_viimeisin_paivamaara(db_polku: str, taulu: str) -> date | None:
    """
    Tarkistaa tietokannasta, mihin asti dataa on jo haettu.
    Etsii sarakkeesta 'game_date' tuoreimman päivämäärän.
    """
    try:
        yhteys = sqlite3.connect(db_polku)
        kysely = f"SELECT MAX(game_date) FROM {taulu}"
        tulos = pd.read_sql_query(kysely, yhteys).iloc[0, 0]
        yhteys.close()
        
        if pd.notna(tulos):
            # Muutetaan merkkijono (esim '2025-08-15') oikeaksi date-olioksi
            return datetime.strptime(str(tulos)[:10], "%Y-%m-%d").date()
    except Exception:
        # Taulua ei ole olemassa tai sarake puuttuu -> palautetaan None
        pass
    return None

# ---------------------------------------------------------------------------
# APUFUNKTIO: Aikavälin pilkkominen
# ---------------------------------------------------------------------------

def luo_aikavalit(alku: date, loppu: date, palan_koko: int) -> list[tuple[date, date]]:
    palat = []
    nykyinen = alku
    while nykyinen <= loppu:
        pala_loppu = min(nykyinen + timedelta(days=palan_koko - 1), loppu)
        palat.append((nykyinen, pala_loppu))
        nykyinen = pala_loppu + timedelta(days=1)
    return palat

# ---------------------------------------------------------------------------
# APUFUNKTIO: Yksittäisen palan haku
# ---------------------------------------------------------------------------

def hae_pala(alku: date, loppu: date) -> pd.DataFrame | None:
    alku_str  = alku.strftime("%Y-%m-%d")
    loppu_str = loppu.strftime("%Y-%m-%d")

    try:
        df = pybaseball.statcast(
            start_dt=alku_str,
            end_dt=loppu_str,
            parallel=False,
        )

        if df is None or df.empty:
            print(f"     ⚠️  Ei dataa välillä {alku_str} – {loppu_str}")
            return None

        print(f"     ✅ {alku_str} – {loppu_str}: {len(df):>7,} syöttöä haettu")
        return df

    except Exception as e:
        print(f"     ❌ VIRHE välillä {alku_str} – {loppu_str}: {type(e).__name__}: {e}")
        return None

# ---------------------------------------------------------------------------
# APUFUNKTIO: Tallennus
# ---------------------------------------------------------------------------

def tallenna_kantaan(df: pd.DataFrame, db_polku: str, taulu: str, tallennus_tapa: str) -> None:
    """
    Tallennus tietokantaan.
    tallennus_tapa = 'replace' (luo uuden) tai 'append' (lisää olemassa olevaan)
    """
    try:
        yhteys = sqlite3.connect(db_polku)
        df.to_sql(
            taulu,
            yhteys,
            if_exists=tallennus_tapa,
            index=False,
            chunksize=10_000,
        )
        yhteys.close()
        print(f"\n✅ Tallennettu {len(df):,} riviä tauluun '{taulu}' (Mode: {tallennus_tapa})")

    except sqlite3.Error as e:
        raise RuntimeError(f"❌ SQLite-virhe tallennuksessa: {e}") from e

# ---------------------------------------------------------------------------
# PÄÄOHJELMA
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    pybaseball.cache.enable()

    viiva = "═" * 62
    print(f"\n{viiva}")
    print(f"  ⚾  STATCAST DATAHAKU (Älykäs päivitys)")
    print(viiva)

    tanaan = date.today()
    viimeisin_kannassa = hae_viimeisin_paivamaara(DB_POLKU, TAULU)

    # Päätellään, mistä asti haetaan ja miten tallennetaan
    if viimeisin_kannassa:
        # Haetaan seuraavasta päivästä alkaen
        haku_alku = viimeisin_kannassa + timedelta(days=1)
        tallennus_tapa = "append"
        print(f"  📌 Tietokannassa on dataa päivään {viimeisin_kannassa} asti.")
        
        if haku_alku > tanaan:
            print("  ✅ Data on jo täysin ajan tasalla! Uutta haettavaa ei ole.")
            print(f"{viiva}\n")
            exit()
            
        print(f"  🚀 Haetaan puuttuvat päivät: {haku_alku} – {tanaan}")
    else:
        haku_alku = OLETUS_ALKU
        tallennus_tapa = "replace"
        print(f"  ⚠️  Ei aiempaa dataa. Haetaan kaikki alusta asti ({haku_alku} – {tanaan}).")

    haku_loppu = tanaan
    print(f"  Kohde : {DB_POLKU} → taulu '{TAULU}'")
    print(f"{viiva}\n")

    # ── Luo aikavälipalat ──
    palat = luo_aikavalit(haku_alku, haku_loppu, PALAN_KOKO_PV)
    print(f"📦 Hakuja tehtävänä yhteensä: {len(palat)}\n")

    onnistuneet: list[pd.DataFrame] = []
    epaonnistuneet: list[tuple[date, date]] = []

    for i, (alku, loppu) in enumerate(palat, start=1):
        print(f"[{i:>3}/{len(palat)}] Haetaan {alku} – {loppu} ...", end="  ")
        pala_df = hae_pala(alku, loppu)

        if pala_df is not None:
            onnistuneet.append(pala_df)
        else:
            epaonnistuneet.append((alku, loppu))

        if i < len(palat):
            time.sleep(TAUKO_SEKUNTIA)

    # ── Yhteenveto ──
    print(f"\n{viiva}")
    print(f"  HAKU VALMIS")
    print(f"  Onnistuneet haut  : {len(onnistuneet)} / {len(palat)}")
    print(f"  Epäonnistuneet    : {len(epaonnistuneet)}")
    print(viiva)

    # ── Yhdistäminen ja tallennus ──
    # TÄMÄ ON NYT OIKEASSA PAIKASSA (ei enää sisennettynä väärin!)
    if not onnistuneet:
        print("\n❌ Yhtään uutta datapakettia ei saatu.")
    else:
        print(f"\n🔗 Yhdistetään {len(onnistuneet)} datapakettia ...")
        yhdistetty = pd.concat(onnistuneet, ignore_index=True)

        # Poistetaan tuoreimman haun sisäiset duplikaatit
        ennen = len(yhdistetty)
        if "pitch_number" in yhdistetty.columns and "game_pk" in yhdistetty.columns:
            yhdistetty = yhdistetty.drop_duplicates(
                subset=["game_pk", "at_bat_number", "pitch_number"]
            )
        jalkeen = len(yhdistetty)
        if ennen != jalkeen:
            print(f"   → Poistettu {ennen - jalkeen:,} duplikaattia haun sisältä.")

        # KUTSUTAAN TALLENNUSTA OIKEIN:
        tallenna_kantaan(yhdistetty, DB_POLKU, TAULU, tallennus_tapa)

        print("\n🎉 Päivitys onnistui!")
