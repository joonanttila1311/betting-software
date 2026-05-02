"""
hae_lyojien_nimet.py
====================
Hakee lyöjien oikeat nimet pybaseballin playerid_reverse_lookup-funktiolla
ja päivittää ne 'lyojat_statcast' -tauluun.

Käyttö:
    python hae_lyojien_nimet.py

Vaatii:
    pip install pybaseball pandas
"""

import sqlite3
from pathlib import Path

import pandas as pd
import pybaseball

# ---------------------------------------------------------------------------
# VAKIOT
# ---------------------------------------------------------------------------
DB_POLKU      = "mlb_historical.db"
TAULU         = "lyojat_statcast"
MIN_PA_LISTAUS = 50    # Minimi-PA top-10-listauksia varten


# ---------------------------------------------------------------------------
# 1. LUE LYÖJÄDATA KANNASTA
# ---------------------------------------------------------------------------

def lue_lyojat(db_polku: str = DB_POLKU) -> pd.DataFrame:
    """Lukee koko lyojat_statcast-taulun DataFrameen."""
    if not Path(db_polku).exists():
        raise FileNotFoundError(
            f"Tietokantaa '{db_polku}' ei löydy. "
            "Aja ensin laske_lyojat.py."
        )
    yhteys = sqlite3.connect(db_polku)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {TAULU}", yhteys)
    except Exception as e:
        raise RuntimeError(
            f"Taulua '{TAULU}' ei löydy – aja ensin laske_lyojat.py. ({e})"
        ) from e
    finally:
        yhteys.close()

    print(f"📂 Luettu '{TAULU}': {len(df):,} lyöjää, "
          f"sarakkeet: {list(df.columns)}")
    return df


# ---------------------------------------------------------------------------
# 2. HAE NIMET PYBASEBALLISTA
# ---------------------------------------------------------------------------

def hae_nimet(batter_ids: list[int]) -> pd.DataFrame:
    """
    Hakee pelaajanimet pybaseballin playerid_reverse_lookup-funktiolla
    MLBAM-ID-numeroiden perusteella.

    Palauttaa DataFramen sarakkeilla: Batter_ID, Player_Name
    Pelaajat joille nimeä ei löydy saavat arvon 'Tuntematon (ID)'.
    """
    print(f"\n🔍 Haetaan nimiä {len(batter_ids):,} pelaajalle pybaseballista ...")

    try:
        lookup_df = pybaseball.playerid_reverse_lookup(
            batter_ids,
            key_type="mlbam",
        )
    except Exception as e:
        print(f"   ⚠️  pybaseball-haku epäonnistui: {e}")
        print("   → Palautetaan tyhjä nimitaulu (Player_Name = ID-numero)")
        return pd.DataFrame({"Batter_ID": batter_ids, "Player_Name": [
            f"ID_{bid}" for bid in batter_ids
        ]})

    if lookup_df is None or lookup_df.empty:
        print("   ⚠️  Lookup palautti tyhjän tuloksen.")
        return pd.DataFrame({"Batter_ID": batter_ids, "Player_Name": [
            f"ID_{bid}" for bid in batter_ids
        ]})

    print(f"   → Löydetty {len(lookup_df):,} osumaa. "
          f"Sarakkeet: {list(lookup_df.columns)}")

    # Rakennetaan 'Sukunimi, Etunimi' -muotoinen nimi isoin alkukirjaimin
    def muotoile_nimi(rivi) -> str:
        sukunimi  = str(rivi.get("name_last",  "")).strip().title()
        etunimi   = str(rivi.get("name_first", "")).strip().title()
        if sukunimi and etunimi:
            return f"{sukunimi}, {etunimi}"
        elif sukunimi:
            return sukunimi
        return f"ID_{int(rivi.get('key_mlbam', 0))}"

    lookup_df["Player_Name"] = lookup_df.apply(muotoile_nimi, axis=1)

    # Nimetään MLBAM-avain yhdenmukaiseksi
    if "key_mlbam" in lookup_df.columns:
        lookup_df = lookup_df.rename(columns={"key_mlbam": "Batter_ID"})

    return lookup_df[["Batter_ID", "Player_Name"]].copy()


# ---------------------------------------------------------------------------
# 3. YHDISTÄ NIMET DATAAN
# ---------------------------------------------------------------------------

def yhdista_nimet(
    df_lyojat: pd.DataFrame,
    df_nimet: pd.DataFrame,
) -> pd.DataFrame:
    """
    Yhdistää nimitaulun lyöjädataan Batter_ID:n perusteella (left join).
    Sijoittaa Player_Name-sarakkeen heti Batter_ID:n viereen.
    Pelaajille joille ei löydy nimeä asetetaan 'Tuntematon (ID)'.
    """
    # Varmistetaan yhteensopiva tyyppi join-sarakkeissa
    df_lyojat = df_lyojat.copy()
    df_nimet  = df_nimet.copy()
    df_lyojat["Batter_ID"] = df_lyojat["Batter_ID"].astype(int)
    df_nimet["Batter_ID"]  = df_nimet["Batter_ID"].astype(int)

    # SIIVOUS 1: Poistetaan vanhat duplikaatit lyöjädatasta (Bugi 14:n jälkivaikutus)
    rivit_ennen_dedup = len(df_lyojat)
    df_lyojat = df_lyojat.drop_duplicates(subset=["Batter_ID"], keep="first")
    poistetut_lyoja_duplikaatit = rivit_ennen_dedup - len(df_lyojat)
    if poistetut_lyoja_duplikaatit > 0:
        print(f"   🧹 Siivottu lyöjädatasta {poistetut_lyoja_duplikaatit} duplikaattirivi(ä)")

    # SIIVOUS 2: Poistetaan duplikaatit nimitaulusta (estää uusien duplikaattien synnyn)
    rivit_ennen_nimi_dedup = len(df_nimet)
    df_nimet = df_nimet.drop_duplicates(subset=["Batter_ID"], keep="first")
    poistetut_nimi_duplikaatit = rivit_ennen_nimi_dedup - len(df_nimet)
    if poistetut_nimi_duplikaatit > 0:
        print(f"   🧹 Siivottu nimitaulusta {poistetut_nimi_duplikaatit} duplikaattirivi(ä)")

    # SIIVOUS 3: Poistetaan vanha Player_Name-sarake jos se on olemassa (estää KeyError uudelleenajossa)
    if "Player_Name" in df_lyojat.columns:
        df_lyojat = df_lyojat.drop(columns=["Player_Name"])
        print(f"   🧹 Poistettu vanha Player_Name-sarake ennen mergeä")

    yhdistetty = df_lyojat.merge(df_nimet, on="Batter_ID", how="left")

    # Fallback: täytetään puuttuvat nimet ID-pohjaisella merkinnällä
    puuttuvat = yhdistetty["Player_Name"].isna().sum()
    if puuttuvat > 0:
        yhdistetty["Player_Name"] = yhdistetty.apply(
            lambda r: f"Tuntematon ({int(r['Batter_ID'])})"
            if pd.isna(r["Player_Name"]) else r["Player_Name"],
            axis=1,
        )
        print(f"   ⚠️  {puuttuvat} pelaajalle ei löytynyt nimeä → 'Tuntematon (ID)'")

    # Järjestetään sarakkeet: Player_Name heti Batter_ID:n jälkeen
    muut = [s for s in yhdistetty.columns if s not in ("Batter_ID", "Player_Name")]
    yhdistetty = yhdistetty[["Batter_ID", "Player_Name"] + muut]

    loydetty = (yhdistetty["Player_Name"].str.startswith("Tuntematon") == False).sum()
    print(f"   ✅ Nimet yhdistetty: {loydetty:,} / {len(yhdistetty):,} pelaajalla nimi")
    return yhdistetty


# ---------------------------------------------------------------------------
# 4. TALLENNA PÄIVITETTY TAULU
# ---------------------------------------------------------------------------

def tallenna(df: pd.DataFrame, db_polku: str = DB_POLKU) -> None:
    """Korvaa lyojat_statcast-taulun päivitetyllä DataFramella."""
    try:
        yhteys = sqlite3.connect(db_polku)
        df.to_sql(TAULU, yhteys, if_exists="replace", index=False)
        yhteys.close()
        print(f"\n✅ Tallennettu {len(df):,} riviä tauluun '{TAULU}' ({db_polku})")
    except sqlite3.Error as e:
        raise RuntimeError(f"❌ SQLite-virhe tallennuksessa: {e}") from e


# ---------------------------------------------------------------------------
# 5. TULOSTUS
# ---------------------------------------------------------------------------

def tulosta_top10(df: pd.DataFrame) -> None:
    """Tulostaa TOP-10 lyöjät Player_Name-sarakkeen kanssa."""
    viiva = "─" * 74
    print(f"\n{viiva}")
    print(f"  🏆 TOP-10 LYÖJÄT – aikapainotettu wOBA  (PA ≥ {MIN_PA_LISTAUS})")
    print(viiva)

    top = df[df["PA_raw"] >= MIN_PA_LISTAUS].nlargest(10, "wOBA_All")

    if top.empty:
        print(f"  Ei lyöjiä joilla PA ≥ {MIN_PA_LISTAUS}")
        return

    print(
        f"  {'#':<4} {'Pelaaja':<28} {'wOBA_All':>9} "
        f"{'vs_L':>8} {'vs_R':>8} {'PA':>6}"
    )
    print(f"  {'─'*4} {'─'*28} {'─'*9} {'─'*8} {'─'*8} {'─'*6}")

    for rank, (_, r) in enumerate(top.iterrows(), start=1):
        nimi   = str(r.get("Player_Name", f"ID_{int(r['Batter_ID'])}"))
        l_flag = " " if r["wOBA_vs_L"] != r["wOBA_All"] else "~"
        r_flag = " " if r["wOBA_vs_R"] != r["wOBA_All"] else "~"
        print(
            f"  {rank:<4} {nimi:<28} {r['wOBA_All']:>9.3f} "
            f"  {r['wOBA_vs_L']:>5.3f}{l_flag}  {r['wOBA_vs_R']:>5.3f}{r_flag} "
            f"{int(r['PA_raw']):>6}"
        )

    print(f"\n  ~ = split-arvo on fallback (käytetty wOBA_All)")
    print(viiva)


# ---------------------------------------------------------------------------
# PÄÄOHJELMA
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    viiva = "═" * 62
    print(f"\n{viiva}")
    print(f"  ⚾  HAE LYÖJIEN NIMET  –  lyojat_statcast")
    print(viiva)

    # Askel 1: Lue nykyinen taulu
    df_lyojat = lue_lyojat()

    # Askel 2: Hae uniikit ID:t ja nimet pybaseballista
    batter_ids = df_lyojat["Batter_ID"].astype(int).unique().tolist()
    df_nimet   = hae_nimet(batter_ids)

    # Askel 3: Yhdistä
    print("\n🔗 Yhdistetään nimet lyöjädataan ...")
    df_paivitetty = yhdista_nimet(df_lyojat, df_nimet)

    # Askel 4: Tallenna
    tallenna(df_paivitetty)

    # Askel 5: Tulosta TOP-10 nimillä
    tulosta_top10(df_paivitetty)

    print(f"\n  Tietokanta: {DB_POLKU}  |  Taulu: {TAULU}")
    print(f"{viiva}\n")