"""
laske_fip.py  –  v3.0  (Time Decay xFIP)
==========================================
Laskee syöttäjäkohtaisen xFIP:n ja joukkuekohtaisen Bullpen-xFIP:n
Statcast 2025 -raakadatasta käyttäen aikapainotettua laskentaa.

Time Decay -logiikka:
    max_date = datan tuorein päivämäärä
    days_ago = (max_date - game_date).days
    weight   = 0.5 ** (days_ago / 60.0)   → 60 pv puoliintumisaika

Painotettu xFIP-kaava:
    K, BB, HBP, fly_balls = weight-summat (ei rivimäärä)
    Outs_w = sum(out_value * weight)
    IP_w   = Outs_w / 3
    xHR    = fly_balls * 0.105
    xFIP   = ((13 * xHR) + (3 * (BB + HBP)) - (2 * K)) / IP_w + 3.20

Tallennettava IP = painottamaton (Outs_raw / 3), jotta luvut ovat
vertailukelpoisia todellisiin inning-määriin.

Käyttö:
    python laske_fip.py
"""

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# VAKIOT
# ---------------------------------------------------------------------------
DB_POLKU        = "mlb_historical.db"
LAHDE_TAULU     = "statcast_2025"
TAULU_SYOTTAJAT = "syottajat_statcast"
TAULU_BULLPEN   = "bullpen_statcast"

FIP_VAKIO       = 3.20     # Kalibrointivakio
HR_FB_SUHDE     = 0.105    # MLB:n historiallinen HR/fly_ball-suhde
PUOLIINTUMISAIKA = 60.0    # Päiviä: paino putoaa puoleen 60 pv:ssä
MIN_IP          = 10.0     # Minimi-IP syöttäjätauluun
MIN_IP_LISTAUS  = 20.0     # Minimi-IP top-5-listauksia varten
BULLPEN_INNING  = 6        # Bullpen alkaa tästä vuoroparista

# ---------------------------------------------------------------------------
# OUT-PAINOSANAKIRJA  (identtinen v1.0 / v2.0:n kanssa)
# ---------------------------------------------------------------------------
OUT_PAINOT: dict[str, int] = {
    "triple_play":                           3,
    "double_play":                           2,
    "grounded_into_double_play":             2,
    "strikeout_double_play":                 2,
    "sac_fly_double_play":                   2,
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
    Lukee taulusta 'statcast_2025' xFIP-laskentaan tarvittavat sarakkeet.
    Nyt mukana myös game_date aikapainotusta varten.
    Suodattaa tyhjät events-rivit SQL-tasolla.
    """
    if not Path(db_polku).exists():
        raise FileNotFoundError(
            f"Tietokantaa '{db_polku}' ei löydy. Aja ensin fetch_statcast.py."
        )

    kysely = """
        SELECT
            player_name,
            events,
            game_type,
            bb_type,
            home_team,
            away_team,
            inning_topbot,
            inning,
            game_pk,
            game_date
        FROM statcast_2025
        WHERE events      IS NOT NULL
          AND events      != ''
          AND player_name IS NOT NULL
          AND game_date   IS NOT NULL
    """

    print(f"📂 Luetaan '{LAHDE_TAULU}' ...")
    yhteys = sqlite3.connect(db_polku)
    try:
        df = pd.read_sql_query(kysely, yhteys)
    finally:
        yhteys.close()

    print(f"   → {len(df):,} riviä luettu (events IS NOT NULL)")
    return df


# ---------------------------------------------------------------------------
# 2. PELIKATEGORIASUODATUS
# ---------------------------------------------------------------------------

def suodata_pelikategoria(df: pd.DataFrame) -> pd.DataFrame:
    """
    Poistaa harjoituspelit (game_type == 'S') jos oikeita pelejä löytyy.
    Jos data on pelkästään harjoituspelejä, käytetään niitä.
    """
    if "game_type" not in df.columns:
        return df

    oikeat = [g for g in df["game_type"].unique() if g not in ("S", None, "")]
    if oikeat:
        poistetaan = (df["game_type"] == "S").sum()
        df = df[df["game_type"] != "S"].copy()
        print(f"   → Poistettu {poistetaan:,} harjoituspeliriviä. Jäljellä: {len(df):,}")
    else:
        print(f"   ⚠️  Vain harjoituspelidataa – käytetään kaikki {len(df):,} riviä.")
    return df


# ---------------------------------------------------------------------------
# 3. JOUKKUEEN PÄÄTTELY
# ---------------------------------------------------------------------------

def lisaa_joukkue(df: pd.DataFrame) -> pd.DataFrame:
    """
    Päättelee syöttävän joukkueen lyhenteen:
      inning_topbot == 'Top' → home_team syöttää
      inning_topbot == 'Bot' → away_team syöttää
    """
    for sarake in ("inning_topbot", "home_team", "away_team"):
        if sarake not in df.columns:
            print(f"   ⚠️  Sarake '{sarake}' puuttuu – Team jää tyhjäksi.")
            df["Team"] = None
            return df

    df = df.copy()
    df["Team"] = np.where(
        df["inning_topbot"] == "Top",
        df["home_team"],
        df["away_team"],
    )
    return df


# ---------------------------------------------------------------------------
# 4. TIME DECAY -PAINOJEN LASKENTA
# ---------------------------------------------------------------------------

def lisaa_painot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Laskee jokaiselle riville aikapainon 60 päivän puoliintumisajalla.

    Logiikka:
        max_date = koko datan tuorein game_date
        days_ago = (max_date - game_date).days
        weight   = 0.5 ** (days_ago / 60.0)

    Tuorein peli saa painon 1.0, 60 päivää vanha 0.5,
    120 päivää vanha 0.25 jne.

    Lisää DataFrameen sarakkeet: 'game_date' (datetime), 'days_ago', 'weight'.
    """
    df = df.copy()

    # Muunnetaan game_date datetime-muotoon (toleroidaan eri formaatit)
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    # Poistetaan rivit joilla päivämäärä ei parsittunut
    puuttuvat = df["game_date"].isna().sum()
    if puuttuvat > 0:
        print(f"   ⚠️  Poistettu {puuttuvat:,} riviä joilla game_date puuttuu/virheellinen.")
        df = df.dropna(subset=["game_date"])

    # Tuorein päivämäärä referenssipisteenä
    max_date = df["game_date"].max()
    print(f"   → Tuorein peli: {max_date.date()}  |  Vanhin: {df['game_date'].min().date()}")

    df["days_ago"] = (max_date - df["game_date"]).dt.days
    df["weight"]   = 0.5 ** (df["days_ago"] / PUOLIINTUMISAIKA)

    # Tilastoinfo painotuksista
    w_min  = df["weight"].min()
    w_mean = df["weight"].mean()
    print(
        f"   → Paino-alue: {w_min:.4f} – 1.0000  |  "
        f"Keskiarvo: {w_mean:.4f}  |  "
        f"Puoliintumisaika: {int(PUOLIINTUMISAIKA)} pv"
    )

    return df


# ---------------------------------------------------------------------------
# 5. PAINOTETTU XFIP-LASKENTA (APUFUNKTIO)
# ---------------------------------------------------------------------------

def laske_xfip_komponentit(ryhma: pd.DataFrame) -> dict:
    """
    Laskee xFIP-komponentit PAINOTETUSTI käyttäen 'weight'-saraketta.

    Painotuslogiikka:
      - K, BB, HBP, fly_balls: weight-summa (ei rivimäärä)
      - Outs_w: sum(out_value * weight)   → painotettu IP-laskentaan
      - Outs_raw: sum(out_value)           → tallennetaan aitona IP:nä
      - IP_w (painotettu): Outs_w / 3     → xFIP-laskentaan
      - IP_raw (aito):     Outs_raw / 3   → tallennetaan kantaan

    Palauttaa dict kaikilla komponenteilla.
    """
    w = ryhma["weight"]

    # ── Painotetut tapahtumamäärät ──
    k_w         = ryhma.loc[ryhma["events"] == "strikeout",    "weight"].sum()
    bb_w        = ryhma.loc[ryhma["events"] == "walk",         "weight"].sum()
    hbp_w       = ryhma.loc[ryhma["events"] == "hit_by_pitch", "weight"].sum()

    # fly_ball-tieto on bb_type-sarakkeessa (ei events-sarakkeessa)
    if "bb_type" in ryhma.columns:
        fly_w   = ryhma.loc[ryhma["bb_type"] == "fly_ball",    "weight"].sum()
    else:
        fly_w   = 0.0

    # ── Painotettu out-laskenta ──
    # Luodaan väliaikainen out_value-sarake ryhmälle
    out_vals        = ryhma["events"].map(lambda e: OUT_PAINOT.get(e, 0))
    outs_w          = float((out_vals * w).sum())   # xFIP-laskentaan
    outs_raw        = float(out_vals.sum())          # aitoon IP:hen

    ip_w            = outs_w   / 3.0   # painotettu IP
    ip_raw          = outs_raw / 3.0   # aito IP

    xhr             = fly_w * HR_FB_SUHDE

    # ── xFIP (painotetulla IP:llä) ──
    if ip_w < 0.01:
        xfip = None
    else:
        xfip = round(
            ((13 * xhr) + (3 * (bb_w + hbp_w)) - (2 * k_w)) / ip_w + FIP_VAKIO,
            2,
        )

    return {
        "K_w":      round(k_w,   2),
        "BB_w":     round(bb_w,  2),
        "HBP_w":    round(hbp_w, 2),
        "fly_w":    round(fly_w, 2),
        "xHR":      round(xhr,   2),
        "Outs_w":   round(outs_w,  2),
        "Outs_raw": round(outs_raw, 0),
        "IP_w":     round(ip_w,  2),
        "IP_raw":   round(ip_raw, 2),
        "xFIP":     xfip,
    }


# ---------------------------------------------------------------------------
# 6A. SYÖTTÄJÄTAULU
# ---------------------------------------------------------------------------

def laske_syottajat(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ryhmittelee datan syöttäjittäin ja laskee:
      xFIP (aikapainotettu), IP (aito), yleisin Team, IP_per_Start
    """
    print("\n⚙️  Lasketaan syöttäjien aikapainotettu xFIP ...")

    rivit      = []
    ryhmittely = df.groupby("player_name", sort=True)
    n          = len(ryhmittely)

    for idx, (nimi, ryhma) in enumerate(ryhmittely, start=1):
        komp = laske_xfip_komponentit(ryhma)

        # Yleisin joukkue (mode)
        team_mode = ryhma["Team"].mode()
        team      = str(team_mode.iloc[0]) if len(team_mode) > 0 else ""

        # IP_per_Start: aito IP / uniikkeja pelejä
        pelit        = ryhma["game_pk"].nunique()
        ip_per_start = round(komp["IP_raw"] / pelit, 2) if pelit > 0 else 0.0

        rivit.append({
            "Name":         nimi,
            "Team":         team,
            "xFIP":         komp["xFIP"],
            "IP":           komp["IP_raw"],    # tallennetaan aito IP
            "IP_per_Start": ip_per_start,
        })

        if idx % 100 == 0 or idx == n:
            print(f"   [{idx:>4}/{n}] laskettu ...", end="\r")

    print()

    df_out = pd.DataFrame(rivit).dropna(subset=["xFIP"])
    ennen  = len(df_out)
    df_out = df_out[df_out["IP"] >= MIN_IP].copy()
    print(f"   → IP-suodatus (≥ {MIN_IP}): {ennen} → {len(df_out)} syöttäjää")

    return df_out.sort_values("xFIP").reset_index(drop=True)[
        ["Name", "Team", "xFIP", "IP", "IP_per_Start"]
    ]


# ---------------------------------------------------------------------------
# 6B. BULLPEN-TAULU
# ---------------------------------------------------------------------------

def laske_bullpen(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ottaa vain syötöt joissa inning >= BULLPEN_INNING.
    Ryhmittelee joukkueittain ja laskee kollektiivisen aikapainotetun xFIP.
    Tallentaa aidon (painottamattoman) IP:n.
    """
    print(f"\n⚙️  Lasketaan bullpen-xFIP (inning ≥ {BULLPEN_INNING}, aikapainotettu) ...")

    if "inning" not in df.columns:
        print("   ⚠️  Sarake 'inning' puuttuu – bullpen-laskenta ohitetaan.")
        return pd.DataFrame(columns=["Team", "Bullpen_xFIP", "IP"])

    df = df.copy()
    df["inning"] = pd.to_numeric(df["inning"], errors="coerce")
    df_bp        = df[df["inning"] >= BULLPEN_INNING].copy()
    print(f"   → {len(df_bp):,} syöttöä inning ≥ {BULLPEN_INNING}")

    rivit = []
    for team, ryhma in df_bp.groupby("Team", sort=True):
        if not team or pd.isna(team):
            continue
        komp = laske_xfip_komponentit(ryhma)
        if komp["xFIP"] is None:
            continue
        rivit.append({
            "Team":         team,
            "Bullpen_xFIP": komp["xFIP"],
            "IP":           komp["IP_raw"],    # aito IP
        })

    return (
        pd.DataFrame(rivit)
        .dropna(subset=["Bullpen_xFIP"])
        .sort_values("Bullpen_xFIP")
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# 7. TALLENNUS
# ---------------------------------------------------------------------------

def tallenna(df: pd.DataFrame, taulu: str, db_polku: str = DB_POLKU) -> None:
    """Tallentaa DataFramen SQLite-tauluun (korvaa vanhan)."""
    try:
        yhteys = sqlite3.connect(db_polku)
        df.to_sql(taulu, yhteys, if_exists="replace", index=False)
        yhteys.close()
        print(f"   ✅ {len(df):>4} riviä → taulu '{taulu}'")
    except sqlite3.Error as e:
        raise RuntimeError(f"❌ SQLite-virhe (taulu '{taulu}'): {e}") from e


# ---------------------------------------------------------------------------
# 8. TULOSTUS
# ---------------------------------------------------------------------------

def tulosta_top5_syottajat(df: pd.DataFrame) -> None:
    viiva = "─" * 66
    print(f"\n{viiva}")
    print(f"  🏆 TOP-5 ALOITUSSYÖTTÄJÄT – aikapainotettu xFIP  (IP ≥ {MIN_IP_LISTAUS})")
    print(viiva)
    top = df[df["IP"] >= MIN_IP_LISTAUS].nsmallest(5, "xFIP")
    if top.empty:
        print(f"  Ei syöttäjiä joilla IP ≥ {MIN_IP_LISTAUS}")
        return
    print(f"  {'#':<3} {'Nimi':<26} {'Joukkue':<8} {'xFIP':>6} {'IP':>7} {'IP/GS':>7}")
    print(f"  {'─'*3} {'─'*26} {'─'*8} {'─'*6} {'─'*7} {'─'*7}")
    for rank, (_, r) in enumerate(top.iterrows(), start=1):
        print(
            f"  {rank:<3} {r['Name']:<26} {str(r['Team']):<8} "
            f"{r['xFIP']:>6.2f} {r['IP']:>7.1f} {r['IP_per_Start']:>7.2f}"
        )
    print(viiva)


def tulosta_top5_bullpen(df: pd.DataFrame) -> None:
    viiva = "─" * 52
    print(f"\n{viiva}")
    print(f"  🏆 TOP-5 PARHAAT BULLPENIT – aikapainotettu xFIP")
    print(viiva)
    top = df.nsmallest(5, "Bullpen_xFIP")
    if top.empty:
        print("  Ei bullpen-dataa.")
        return
    print(f"  {'#':<3} {'Joukkue':<10} {'Bullpen xFIP':>13} {'IP':>8}")
    print(f"  {'─'*3} {'─'*10} {'─'*13} {'─'*8}")
    for rank, (_, r) in enumerate(top.iterrows(), start=1):
        print(
            f"  {rank:<3} {str(r['Team']):<10} "
            f"{r['Bullpen_xFIP']:>13.2f} {r['IP']:>8.1f}"
        )
    print(viiva)


# ---------------------------------------------------------------------------
# PÄÄOHJELMA
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    viiva = "═" * 62
    print(f"\n{viiva}")
    print(f"  ⚾  xFIP v3.0 – TIME DECAY  |  Statcast 2025")
    print(f"  Puoliintumisaika: {int(PUOLIINTUMISAIKA)} pv  |  FIP-vakio: {FIP_VAKIO}")
    print(viiva)

    # Askel 1: Lue data
    df_raa = lue_data()

    # Askel 2: Suodata kausikoodi
    df = suodata_pelikategoria(df_raa)

    # Askel 3: Päättele joukkue
    df = lisaa_joukkue(df)

    # Askel 4: Lisää aikapainot
    print("\n⏳ Lasketaan aikapainot ...")
    df = lisaa_painot(df)

    # Askel 5: Syöttäjät
    print(f"\n{'─'*62}")
    df_syottajat = laske_syottajat(df)

    # Askel 6: Bullpen
    df_bullpen = laske_bullpen(df)

    # Tallennus
    print(f"\n💾 Tallennetaan taulut ...")
    tallenna(df_syottajat, TAULU_SYOTTAJAT)
    tallenna(df_bullpen,   TAULU_BULLPEN)

    # Tulostus
    tulosta_top5_syottajat(df_syottajat)
    tulosta_top5_bullpen(df_bullpen)

    print(f"\n  Tietokanta : {DB_POLKU}")
    print(f"  xFIP-vakio : {FIP_VAKIO}  |  HR/FB : {HR_FB_SUHDE}")
    print(f"  Bullpen    : inning ≥ {BULLPEN_INNING}")
    print(f"  Decay      : paino = 0.5 ^ (days_ago / {int(PUOLIINTUMISAIKA)})")
    print(f"{viiva}\n")