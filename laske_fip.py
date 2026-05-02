"""
laske_fip.py  –  v4.2  (Time Decay xFIP + Platoon Splits + K-BB%)
===========================================================
Laskee syöttäjäkohtaisen xFIP:n ja joukkuekohtaisen Bullpen-xFIP:n
Statcast 2025 -raakadatasta käyttäen aikapainotettua laskentaa.
Lisäksi lasketaan Platoon Splits: xFIP erikseen vasenkätisiä (L)
ja oikeakätisiä (R) lyöjiä vastaan. Lisätty K-BB% syöttäjän dominanssin mittariksi.
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

FIP_VAKIO       = 3.65     # Kalibrointivakio
HR_FB_SUHDE     = 0.115    # MLB:n historiallinen HR/fly_ball-suhde
PUOLIINTUMISAIKA = 60.0    # Päiviä: paino putoaa puoleen 60 pv:ssä
MIN_IP          = 10.0     # Minimi-IP syöttäjätauluun
MIN_IP_LISTAUS  = 10.0     # Minimi-IP top-5-listauksia varten
MIN_IP_SPLIT    = 1.0      # Minimi painotettu IP split-laskennalle (alle → fallback)
BULLPEN_INNING  = 6        # Bullpen alkaa tästä vuoroparista

WEIGHT_SPRING_TRAINING = 0.20

# ---------------------------------------------------------------------------
# OUT-PAINOSANAKIRJA  
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
            game_date,
            stand,
            p_throws 
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
    if "game_type" in df.columns:
        df = df[df["game_type"].isin(["R", "P"])]
        counts = df["game_type"].value_counts().to_dict()
        tyypit_str = ", ".join([f"{k}: {v:,}" for k, v in counts.items()])
        print(f"   → Pelityypit (game_type): {tyypit_str}")
    return df

# ---------------------------------------------------------------------------
# 3. JOUKKUEEN PÄÄTTELY
# ---------------------------------------------------------------------------
def lisaa_joukkue(df: pd.DataFrame) -> pd.DataFrame:
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
    import numpy as np
    
    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    puuttuvat = df["game_date"].isna().sum()
    if puuttuvat > 0:
        print(f"   ⚠️  Poistettu {puuttuvat:,} riviä joilla game_date puuttuu/virheellinen.")
        df = df.dropna(subset=["game_date"])

    nykyhetki = pd.to_datetime('today')
    nykyinen_vuosi = nykyhetki.year

    menneet_kaudet = df[df['game_date'].dt.year < nykyinen_vuosi]
    nykyinen_kausi = df[df['game_date'].dt.year == nykyinen_vuosi]

    df["days_ago"] = (nykyhetki - df["game_date"]).dt.days

    if not menneet_kaudet.empty:
        t_last = menneet_kaudet['game_date'].max()

        if not nykyinen_kausi.empty:
            tosipelit = nykyinen_kausi[nykyinen_kausi['game_date'].dt.month >= 3]
            tosipelit = tosipelit[tosipelit['game_date'].dt.day >= 20]
            
            if not tosipelit.empty:
                t_first = tosipelit['game_date'].min()
                offseason_tauko = max(0, (t_first - t_last).days - 30)
            else:
                offseason_tauko = max(0, (nykyhetki - t_last).days - 30)
        else:
            offseason_tauko = max(0, (nykyhetki - t_last).days - 30)

        df['days_ago'] = np.where(
            df['game_date'].dt.year < nykyinen_vuosi,
            df['days_ago'] - offseason_tauko,
            df['days_ago']
        )

    df['days_ago'] = df['days_ago'].clip(lower=0)
    df["time_weight"]   = 0.5 ** (df["days_ago"] / PUOLIINTUMISAIKA)

    if "game_type" in df.columns:
        df['game_weight'] = np.where(df['game_type'] == 'S', WEIGHT_SPRING_TRAINING, 1.0)
    else:
        df['game_weight'] = 1.0
        
    df["weight"] = df["time_weight"] * df["game_weight"]

    w_min  = df["weight"].min()
    w_mean = df["weight"].mean()
    max_date_print = df['game_date'].max().date()
    min_date_print = df['game_date'].min().date()
    
    print(f"   → Tuorein peli: {max_date_print}  |  Vanhin: {min_date_print}")
    print(
        f"   → Lopullinen paino-alue (Time * GameType): {w_min:.4f} – 1.0000  |  "
        f"Keskiarvo: {w_mean:.4f}  |  "
        f"Puoliintumisaika: {int(PUOLIINTUMISAIKA)} pv"
    )

    return df

# ---------------------------------------------------------------------------
# 5. PAINOTETTU XFIP & K-BB% LASKENTA
# ---------------------------------------------------------------------------
def laske_xfip_komponentit(ryhma: pd.DataFrame) -> dict:
    w = ryhma["weight"]

    k_w         = ryhma.loc[ryhma["events"] == "strikeout",    "weight"].sum()
    bb_w        = ryhma.loc[ryhma["events"] == "walk",         "weight"].sum()
    hbp_w       = ryhma.loc[ryhma["events"] == "hit_by_pitch", "weight"].sum()

    if "bb_type" in ryhma.columns:
        fly_w   = ryhma.loc[ryhma["bb_type"] == "fly_ball",    "weight"].sum()
    else:
        fly_w   = 0.0

    out_vals        = ryhma["events"].map(lambda e: OUT_PAINOT.get(e, 0))
    outs_w          = float((out_vals * w).sum())
    outs_raw        = float(out_vals.sum())

    ip_w            = outs_w   / 3.0
    ip_raw          = outs_raw / 3.0
    xhr             = fly_w * HR_FB_SUHDE

    # UUSI: LASKETAAN K-BB% (Strikeout% miinus Walk%)
    tbf_w = ryhma["weight"].sum() # Total Batters Faced (painotettu)
    k_bb_pct = (k_w - bb_w) / tbf_w if tbf_w > 0 else 0.0

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
        "K_BB_pct": round(k_bb_pct, 4), # UUSI: Tallennetaan sanakirjaan
    }

# ---------------------------------------------------------------------------
# 5B. PLATOON SPLIT -APUFUNKTIO
# ---------------------------------------------------------------------------
def laske_split_xfip(
    ryhma: pd.DataFrame,
    katisyys: str,
    fallback_xfip: float,
) -> float:
    if "stand" not in ryhma.columns:
        return fallback_xfip

    osajoukko = ryhma[ryhma["stand"] == katisyys]

    if len(osajoukko) == 0:
        return fallback_xfip

    komp = laske_xfip_komponentit(osajoukko)

    if komp["xFIP"] is None or komp["IP_w"] < MIN_IP_SPLIT:
        return fallback_xfip

    return komp["xFIP"]

# ---------------------------------------------------------------------------
# 6A. SYÖTTÄJÄTAULU
# ---------------------------------------------------------------------------
def laske_syottajat(df: pd.DataFrame) -> pd.DataFrame:
    print("\n⚙️  Lasketaan syöttäjien aikapainotettu xFIP, K-BB% + Platoon Splits ...")

    rivit      = []
    ryhmittely = df.groupby("player_name", sort=True)
    n          = len(ryhmittely)

    for idx, (nimi, ryhma) in enumerate(ryhmittely, start=1):
        komp     = laske_xfip_komponentit(ryhma)
        xfip_all = komp["xFIP"]

        team_mode = ryhma["Team"].mode()
        team      = str(team_mode.iloc[0]) if len(team_mode) > 0 else ""
        
        if "p_throws" in ryhma.columns:
            p_throws_mode = ryhma["p_throws"].dropna().mode()
            katisyys = str(p_throws_mode.iloc[0]) if len(p_throws_mode) > 0 else "R"
        else:
            katisyys = "R"

        pelit        = ryhma["game_pk"].nunique()
        ip_per_start = round(komp["IP_raw"] / pelit, 2) if pelit > 0 else 0.0

        fallback     = xfip_all if xfip_all is not None else FIP_VAKIO
        xfip_vs_l    = laske_split_xfip(ryhma, "L", fallback)
        xfip_vs_r    = laske_split_xfip(ryhma, "R", fallback)

        rivit.append({
            "Name":         nimi,
            "Team":         team,
            "xFIP_All":     xfip_all,
            "xFIP_vs_L":    xfip_vs_l,
            "xFIP_vs_R":    xfip_vs_r,
            "K_BB_pct":     komp["K_BB_pct"], # UUSI
            "IP":           komp["IP_raw"],
            "IP_per_Start": ip_per_start,
            "p_throws":     katisyys, 
        })

        if idx % 100 == 0 or idx == n:
            print(f"   [{idx:>4}/{n}] laskettu ...", end="\r")

    print()

    df_out = pd.DataFrame(rivit).dropna(subset=["xFIP_All"])
    ennen  = len(df_out)
    df_out = df_out[df_out["IP"] >= MIN_IP].copy()
    print(f"   → IP-suodatus (≥ {MIN_IP}): {ennen} → {len(df_out)} syöttäjää")

    # UUSI: Varmistetaan K_BB_pct sarakkeen järjestys
    return df_out.sort_values("xFIP_All").reset_index(drop=True)[
        ["Name", "Team", "xFIP_All", "xFIP_vs_L", "xFIP_vs_R", "K_BB_pct", "IP", "IP_per_Start", "p_throws"]
    ]

# ---------------------------------------------------------------------------
# 6B. BULLPEN-TAULU
# ---------------------------------------------------------------------------
def laske_bullpen(df: pd.DataFrame) -> pd.DataFrame:
    print(f"\n⚙️  Lasketaan bullpen-xFIP ja K-BB% (inning ≥ {BULLPEN_INNING}) ...")

    if "inning" not in df.columns:
        print("   ⚠️  Sarake 'inning' puuttuu – bullpen-laskenta ohitetaan.")
        return pd.DataFrame(
            columns=["Team", "Bullpen_xFIP_All", "Bullpen_xFIP_vs_L", "Bullpen_xFIP_vs_R", "Bullpen_K_BB_pct", "IP"]
        )

    df = df.copy()
    df["inning"] = pd.to_numeric(df["inning"], errors="coerce")
    df_bp        = df[df["inning"] >= BULLPEN_INNING].copy()
    print(f"   → {len(df_bp):,} syöttöä inning ≥ {BULLPEN_INNING}")

    rivit = []
    for team, ryhma in df_bp.groupby("Team", sort=True):
        if not team or pd.isna(team):
            continue
        komp     = laske_xfip_komponentit(ryhma)
        xfip_all = komp["xFIP"]
        if xfip_all is None:
            continue

        fallback   = xfip_all
        xfip_vs_l  = laske_split_xfip(ryhma, "L", fallback)
        xfip_vs_r  = laske_split_xfip(ryhma, "R", fallback)

        rivit.append({
            "Team":               team,
            "Bullpen_xFIP_All":   xfip_all,
            "Bullpen_xFIP_vs_L":  xfip_vs_l,
            "Bullpen_xFIP_vs_R":  xfip_vs_r,
            "Bullpen_K_BB_pct":   komp["K_BB_pct"], # UUSI
            "IP":                 komp["IP_raw"],
        })

    return (
        pd.DataFrame(rivit)
        .dropna(subset=["Bullpen_xFIP_All"])
        .sort_values("Bullpen_xFIP_All")
        .reset_index(drop=True)
    )

# ---------------------------------------------------------------------------
# 7. TALLENNUS
# ---------------------------------------------------------------------------
def tallenna(df: pd.DataFrame, taulu: str, db_polku: str = DB_POLKU) -> None:
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
    viiva = "─" * 84
    print(f"\n{viiva}")
    print(f"  🏆 TOP-5 ALOITUSSYÖTTÄJÄT – aikapainotettu xFIP  (IP ≥ {MIN_IP_LISTAUS})")
    print(viiva)
    top = df[df["IP"] >= MIN_IP_LISTAUS].nsmallest(5, "xFIP_All")
    if top.empty:
        print(f"  Ei syöttäjiä joilla IP ≥ {MIN_IP_LISTAUS}")
        return
    print(
        f"  {'#':<3} {'Nimi':<24} {'TM':<5} {'xFIP_All':>9} "
        f"{'vs_L':>7} {'vs_R':>7} {'K-BB%':>7} {'IP':>7} {'IP/GS':>6}"
    )
    print(f"  {'─'*3} {'─'*24} {'─'*5} {'─'*9} {'─'*7} {'─'*7} {'─'*7} {'─'*7} {'─'*6}")
    for rank, (_, r) in enumerate(top.iterrows(), start=1):
        print(
            f"  {rank:<3} {r['Name']:<24} {str(r['Team']):<5} "
            f"{r['xFIP_All']:>9.2f} {r['xFIP_vs_L']:>7.2f} {r['xFIP_vs_R']:>7.2f} "
            f"{r['K_BB_pct']*100:>6.1f}% {r['IP']:>7.1f} {r['IP_per_Start']:>6.2f}"
        )
    print(viiva)

def tulosta_top5_bullpen(df: pd.DataFrame) -> None:
    viiva = "─" * 74
    print(f"\n{viiva}")
    print(f"  🏆 TOP-5 PARHAAT BULLPENIT – aikapainotettu xFIP")
    print(viiva)
    top = df.nsmallest(5, "Bullpen_xFIP_All")
    if top.empty:
        print("  Ei bullpen-dataa.")
        return
    print(
        f"  {'#':<3} {'Joukkue':<8} {'xFIP_All':>9} "
        f"{'vs_L':>7} {'vs_R':>7} {'K-BB%':>7} {'IP':>8}"
    )
    print(f"  {'─'*3} {'─'*8} {'─'*9} {'─'*7} {'─'*7} {'─'*7} {'─'*8}")
    for rank, (_, r) in enumerate(top.iterrows(), start=1):
        print(
            f"  {rank:<3} {str(r['Team']):<8} "
            f"{r['Bullpen_xFIP_All']:>9.2f} "
            f"{r['Bullpen_xFIP_vs_L']:>7.2f} {r['Bullpen_xFIP_vs_R']:>7.2f} "
            f"{r['Bullpen_K_BB_pct']*100:>6.1f}% {r['IP']:>8.1f}"
        )
    print(viiva)

# ---------------------------------------------------------------------------
# PÄÄOHJELMA
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    viiva = "═" * 62
    print(f"\n{viiva}")
    print(f"  ⚾  xFIP v4.2 – TIME DECAY + SPLITS + K-BB% | Statcast 2025")
    print(f"  Puoliintumisaika: {int(PUOLIINTUMISAIKA)} pv  |  FIP-vakio: {FIP_VAKIO}")
    print(viiva)

    df_raa = lue_data()
    df = suodata_pelikategoria(df_raa)
    df = lisaa_joukkue(df)
    
    print("\n⏳ Lasketaan aikapainot ...")
    df = lisaa_painot(df)

    print(f"\n{'─'*62}")
    df_syottajat = laske_syottajat(df)

    df_bullpen = laske_bullpen(df)

    print(f"\n💾 Tallennetaan taulut ...")
    tallenna(df_syottajat, TAULU_SYOTTAJAT)
    tallenna(df_bullpen,   TAULU_BULLPEN)

    tulosta_top5_syottajat(df_syottajat)
    tulosta_top5_bullpen(df_bullpen)
    print(f"\n  Yhdistetty onnistuneesti! K-BB% sarakkeet lisätty tietokantaan.")