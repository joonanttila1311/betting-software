"""
laske_fip.py
============
Laskee syöttäjien FIP-tilaston (Fielding Independent Pitching)
Statcast 2025 -raakadatasta ja tallentaa SQLite-kantaan.

FIP-kaava:
    FIP = ((13 × HR) + (3 × (BB + HBP)) - (2 × K)) / IP + 3.20

Käyttö:
    python laske_fip.py
"""

import sqlite3
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# VAKIOT
# ---------------------------------------------------------------------------
DB_POLKU        = "mlb_historical.db"
LAHDE_TAULU     = "statcast_2025"
KOHDE_TAULU     = "syottajat_statcast"
FIP_VAKIO       = 3.20        # MLB:n historialliseen ERA-tasoon kalibroitu vakio
MIN_IP          = 10.0        # Minimi-IP suodatukseen
MIN_IP_LISTAUS  = 20.0        # Minimi-IP top-5-listaukseen

# ---------------------------------------------------------------------------
# OUT-PAINO SANAKIRJA
# ---------------------------------------------------------------------------
# Kuinka monta syöjää (out) kukin event tuottaa syöttäjälle.
OUT_PAINOT: dict[str, int] = {
    # 3 syöjää
    "triple_play":                           3,
    # 2 syöjää
    "double_play":                           2,
    "grounded_into_double_play":             2,
    "strikeout_double_play":                 2,
    "sac_fly_double_play":                   2,
    # 1 syöjä
    "field_out":                             1,
    "strikeout":                             1,
    "force_out":                             1,
    "sac_fly":                               1,
    "sac_bunt":                              1,
    "fielders_choice_out":                   1,
    "caught_stealing_2b":                    1,
    "caught_stealing_3b":                    1,
    "caught_stealing_home":                  1,
    "pickoff_1b":                            1,
    "pickoff_2b":                            1,
    "pickoff_3b":                            1,
    "pickoff_caught_stealing_2b":            1,
    "pickoff_caught_stealing_3b":            1,
    "pickoff_caught_stealing_home":          1,
}


# ---------------------------------------------------------------------------
# 1. DATAN LUKU
# ---------------------------------------------------------------------------

def lue_data(db_polku: str = DB_POLKU) -> pd.DataFrame:
    """
    Lukee taulusta 'statcast_2025' vain tarvittavat sarakkeet
    suoraan SQL-tasolla (muistitehokas).

    Suodatukset SQL:ssä:
      - events IS NOT NULL  → poistetaan tyhjät rivit
    """
    if not Path(db_polku).exists():
        raise FileNotFoundError(
            f"Tietokantaa '{db_polku}' ei löydy. "
            "Aja ensin fetch_statcast.py."
        )

    kysely = """
        SELECT
            player_name,
            events,
            game_type
        FROM statcast_2025
        WHERE events IS NOT NULL
          AND events != ''
          AND player_name IS NOT NULL
    """

    print(f"📂 Luetaan '{LAHDE_TAULU}' tietokannasta '{db_polku}' ...")
    yhteys = sqlite3.connect(db_polku)
    try:
        df = pd.read_sql_query(kysely, yhteys)
    finally:
        yhteys.close()

    print(f"   → {len(df):,} riviä luettu (events IS NOT NULL)")
    return df


# ---------------------------------------------------------------------------
# 2. PELIKATEGORIAN SUODATUS
# ---------------------------------------------------------------------------

def suodata_pelikategoria(df: pd.DataFrame) -> pd.DataFrame:
    """
    Poistaa harjoituspelit (game_type == 'S'), JOS datassa on
    myös oikeita pelejä (R = runkosarja, P/D/L/W = pudotuspelit).

    Jos data koostuu PELKISTÄ harjoituspeleistä, käytetään niitä.
    """
    if "game_type" not in df.columns:
        print("   ⚠️  Saraketta 'game_type' ei löydy – käytetään kaikki rivit.")
        return df

    kategoriat  = df["game_type"].unique()
    oikeat      = [g for g in kategoriat if g not in ("S", None, "")]
    harjoitus   = (df["game_type"] == "S").sum()

    if oikeat:
        df_suodatettu = df[df["game_type"] != "S"].copy()
        print(
            f"   → Pelikategoriat: {sorted(kategoriat)}. "
            f"Poistettu {harjoitus:,} harjoituspeli-riviä. "
            f"Jäljellä: {len(df_suodatettu):,} riviä."
        )
        return df_suodatettu
    else:
        print(
            f"   ⚠️  Vain harjoituspelidataa (game_type='S'). "
            f"Käytetään kaikki {len(df):,} riviä."
        )
        return df


# ---------------------------------------------------------------------------
# 3. FIP-LASKENTA
# ---------------------------------------------------------------------------

def laske_out_maara(events_sarja: pd.Series) -> int:
    """Laskee syöttäjälle tilastoitujen syöjien (outs) kokonaismäärän."""
    return int(events_sarja.map(lambda e: OUT_PAINOT.get(e, 0)).sum())


def laske_fip_tilastot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Laskee kullekin syöttäjälle:
      K, BB, HBP, HR, Outs, IP ja lopullisen FIP-arvon.

    Palauttaa:
        pd.DataFrame, jossa sarakkeet: Name, K, BB, HBP, HR, Outs, IP, FIP
    """
    print("\n⚙️  Lasketaan FIP-tilastot syöttäjittäin ...")

    # Ryhmittely syöttäjän mukaan
    rivit = []
    ryhmittely = df.groupby("player_name", sort=True)
    yhteensa   = len(ryhmittely)

    for idx, (nimi, ryhma) in enumerate(ryhmittely, start=1):
        tapahtumat = ryhma["events"]

        # ── Tilastokomponentit ──
        k    = int((tapahtumat == "strikeout").sum())
        bb   = int((tapahtumat == "walk").sum())
        hbp  = int((tapahtumat == "hit_by_pitch").sum())
        hr   = int((tapahtumat == "home_run").sum())
        outs = laske_out_maara(tapahtumat)
        ip   = round(outs / 3, 2)

        # ── FIP-kaava ──
        if ip < 0.01:
            # Vältetään nollajako – syöttäjällä ei käytännössä IP:tä
            fip = None
        else:
            fip = round(
                ((13 * hr) + (3 * (bb + hbp)) - (2 * k)) / ip + FIP_VAKIO,
                2,
            )

        rivit.append({
            "Name": nimi,
            "K":    k,
            "BB":   bb,
            "HBP":  hbp,
            "HR":   hr,
            "Outs": outs,
            "IP":   ip,
            "FIP":  fip,
        })

        # Edistymisviesti joka 100. syöttäjä
        if idx % 100 == 0 or idx == yhteensa:
            print(f"   [{idx:>4}/{yhteensa}] laskettu ...", end="\r")

    print()  # Rivinvaihto edistymisrivin jälkeen
    return pd.DataFrame(rivit)


# ---------------------------------------------------------------------------
# 4. SUODATUS JA MUOTOILU
# ---------------------------------------------------------------------------

def suodata_ja_muotoile(df: pd.DataFrame) -> pd.DataFrame:
    """
    Poistaa syöttäjät joilla IP < MIN_IP ja FIP on None.
    Järjestää tulokset FIP:n mukaan (paras ensin).
    """
    ennen = len(df)

    # Poistetaan None-FIP (käytännössä IP = 0)
    df = df.dropna(subset=["FIP"])

    # IP-alaraja
    df = df[df["IP"] >= MIN_IP].copy()

    jalkeen = len(df)
    print(f"   → IP-suodatus (>= {MIN_IP}): {ennen} → {jalkeen} syöttäjää")

    # Järjestetään FIP-arvojärjestykseen (pienin = paras)
    df = df.sort_values("FIP").reset_index(drop=True)

    # Valitaan tallennettavat sarakkeet järjestyksessä
    return df[["Name", "FIP", "IP", "K", "BB", "HR"]]


# ---------------------------------------------------------------------------
# 5. TALLENNUS TIETOKANTAAN
# ---------------------------------------------------------------------------

def tallenna_kantaan(df: pd.DataFrame, db_polku: str = DB_POLKU) -> None:
    """Tallentaa FIP-taulun SQLite-tietokantaan."""
    try:
        yhteys = sqlite3.connect(db_polku)
        df.to_sql(KOHDE_TAULU, yhteys, if_exists="replace", index=False)
        yhteys.close()
        print(f"\n✅ Tallennettu {len(df):,} syöttäjää tauluun '{KOHDE_TAULU}'")
    except sqlite3.Error as e:
        raise RuntimeError(f"❌ Tietokantavirhe tallennuksessa: {e}") from e


# ---------------------------------------------------------------------------
# 6. TULOSTUS
# ---------------------------------------------------------------------------

def tulosta_yhteenveto(df: pd.DataFrame) -> None:
    """Tulostaa loppuyhteenvedon ja top-5-listan."""
    viiva = "─" * 60

    print(f"\n{viiva}")
    print(f"  📊 FIP-LASKENTA VALMIS  –  {len(df):,} syöttäjää")
    print(viiva)

    # ── Top-5 (min MIN_IP_LISTAUS IP, pienin FIP = paras) ──
    top5 = df[df["IP"] >= MIN_IP_LISTAUS].nsmallest(5, "FIP")

    if top5.empty:
        print(f"  ⚠️  Ei syöttäjiä joilla IP ≥ {MIN_IP_LISTAUS}")
    else:
        print(f"\n  🏆 TOP-5 parhaat FIP-arvot (IP ≥ {MIN_IP_LISTAUS})\n")
        print(f"  {'#':<4} {'Nimi':<28} {'FIP':>6} {'IP':>7} {'K':>6} {'BB':>5} {'HR':>5}")
        print(f"  {'-'*4} {'-'*28} {'-'*6} {'-'*7} {'-'*6} {'-'*5} {'-'*5}")
        for rank, (_, rivi) in enumerate(top5.iterrows(), start=1):
            print(
                f"  {rank:<4} {rivi['Name']:<28} "
                f"{rivi['FIP']:>6.2f} {rivi['IP']:>7.1f} "
                f"{int(rivi['K']):>6} {int(rivi['BB']):>5} {int(rivi['HR']):>5}"
            )

    print(f"\n{viiva}")
    print(f"  Taulu: '{KOHDE_TAULU}'  ·  Tietokanta: '{DB_POLKU}'")
    print(f"  FIP-vakio: {FIP_VAKIO}  ·  Min-IP: {MIN_IP}")
    print(f"{viiva}\n")


# ---------------------------------------------------------------------------
# PÄÄOHJELMA
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Askel 1: Lue raakadata
    df_raa    = lue_data()

    # Askel 2: Pelikategoriasuodatus
    df_siisti = suodata_pelikategoria(df_raa)

    # Askel 3 & 4: Laske FIP
    df_fip    = laske_fip_tilastot(df_siisti)

    # Askel 5: Suodata ja muotoile
    df_valmis = suodata_ja_muotoile(df_fip)

    # Askel 6: Tallenna ja tulosta
    tallenna_kantaan(df_valmis)
    tulosta_yhteenveto(df_valmis)