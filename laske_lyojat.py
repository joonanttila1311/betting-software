"""
laske_lyojat.py  –  v4.2  (wOBA + Platoon Splits + ISO)
===========================================================
Laskee lyöjäkohtaisen aikapainotetun wOBA:n (Weighted On-Base Average),
Platoon Splits -arvot syöttäjän kätisyyden mukaan,
ja lisäksi tyrmäysvoiman (ISO = Isolated Power).
"""

import sqlite3
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# VAKIOT
# ---------------------------------------------------------------------------
DB_POLKU        = "mlb_historical.db"
LAHDE_TAULU     = "statcast_2025"
KOHDE_TAULU     = "lyojat_statcast"

PUOLIINTUMISAIKA = 90.0    # päiviä: paino putoaa puoleen 90 pv:ssä
MIN_PA_W_KOKO    = 20.0    # minimi painotettu PA koko kaudelle (alle → poistetaan)
MIN_PA_W_SPLIT   = 5.0    # minimi painotettu PA splitille (alle → fallback)
MIN_PA_LISTAUS   = 50      # minimi aito PA top-10-listauksia varten

WEIGHT_SPRING_TRAINING = 0.20

# ---------------------------------------------------------------------------
# wOBA-PAINOT  
# ---------------------------------------------------------------------------
WOBA_PAINOT: dict[str, float] = {
    "walk":          0.69,
    "hit_by_pitch":  0.72,
    "single":        0.89,
    "double":        1.27,
    "triple":        1.62,
    "home_run":      2.10,
}

# ---------------------------------------------------------------------------
# PLATE APPEARANCE -TAPAHTUMAT
# ---------------------------------------------------------------------------
PA_TAPAHTUMAT: frozenset[str] = frozenset({
    "walk", "hit_by_pitch", "single", "double", "triple", "home_run",
    "strikeout",
    "field_out",
    "force_out",
    "grounded_into_double_play",
    "double_play",
    "strikeout_double_play",
    "sac_fly_double_play",
    "fielders_choice_out",
    "fielders_choice",
    "sac_fly",
})

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
            batter,
            events,
            p_throws,
            game_date,
            game_type
        FROM statcast_2025
        WHERE events   IS NOT NULL
          AND events   != ''
          AND batter   IS NOT NULL
    """

    print(f"📂 Luetaan '{LAHDE_TAULU}' (lyöjädata) ...")
    yhteys = sqlite3.connect(db_polku)
    try:
        df = pd.read_sql_query(kysely, yhteys)
    finally:
        yhteys.close()

    print(f"   → {len(df):,} riviä luettu")
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
# 3. TIME DECAY -PAINOT
# ---------------------------------------------------------------------------
def lisaa_painot(df: pd.DataFrame) -> pd.DataFrame:
    import numpy as np
    
    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    puuttuvat = df["game_date"].isna().sum()
    if puuttuvat > 0:
        print(f"   ⚠️  Poistettu {puuttuvat:,} riviä joilla game_date puuttuu.")
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

    max_date_print = df['game_date'].max().date()
    min_date_print = df['game_date'].min().date()

    print(f"   → Tuorein peli: {max_date_print}  |  Vanhin: {min_date_print}")
    print(
        f"   → Lopullinen paino-alue (Time * GameType): {df['weight'].min():.4f} – 1.0000  |  "
        f"Keskiarvo: {df['weight'].mean():.4f}  |  "
        f"Puoliintumisaika: {int(PUOLIINTUMISAIKA)} pv"
    )
    return df

# ---------------------------------------------------------------------------
# 4. wOBA & ISO LASKENTA 
# ---------------------------------------------------------------------------
def laske_woba(osajoukko: pd.DataFrame) -> dict:
    pa_maski = osajoukko["events"].isin(PA_TAPAHTUMAT)
    pa_df    = osajoukko[pa_maski]

    pa_raw = len(pa_df)
    pa_w   = float(pa_df["weight"].sum())

    if pa_w < 0.01:
        return {"wOBA": None, "PA_w": 0.0, "PA_raw": 0, "ISO": 0.0}

    osoittaja = float(
        pa_df.apply(
            lambda rivi: WOBA_PAINOT.get(rivi["events"], 0.0) * rivi["weight"],
            axis=1,
        ).sum()
    )

    woba = round(osoittaja / pa_w, 3)

    # UUSI: LASKETAAN ISO (Isolated Power)
    double_w = pa_df.loc[pa_df["events"] == "double", "weight"].sum()
    triple_w = pa_df.loc[pa_df["events"] == "triple", "weight"].sum()
    hr_w     = pa_df.loc[pa_df["events"] == "home_run", "weight"].sum()
    
    # ISO = (Tuplat + 2*Triplat + 3*Kunnarit) / PA
    iso_osoittaja = double_w + (2.0 * triple_w) + (3.0 * hr_w)
    iso = round(iso_osoittaja / pa_w, 3)

    return {
        "wOBA":    woba,
        "PA_w":    round(pa_w, 2),
        "PA_raw":  pa_raw,
        "ISO":     iso, # UUSI: Tallennetaan sanakirjaan
    }

# ---------------------------------------------------------------------------
# 5. PLATOON SPLIT -APUFUNKTIO
# ---------------------------------------------------------------------------
def laske_split_woba(
    ryhma: pd.DataFrame,
    p_throws_arvo: str,
    fallback: float,
) -> float:
    if "p_throws" not in ryhma.columns:
        return fallback

    osajoukko = ryhma[ryhma["p_throws"] == p_throws_arvo]
    if len(osajoukko) == 0:
        return fallback

    tulos = laske_woba(osajoukko)

    if tulos["wOBA"] is None or tulos["PA_w"] < MIN_PA_W_SPLIT:
        return fallback

    return tulos["wOBA"]

# ---------------------------------------------------------------------------
# 6. PÄÄLOGIIKKA: LYÖJÄTILASTOT
# ---------------------------------------------------------------------------
def laske_lyojatilastot(df: pd.DataFrame) -> pd.DataFrame:
    print("\n⚙️  Lasketaan lyöjien aikapainotettu wOBA, ISO + Platoon Splits ...")

    rivit      = []
    ryhmittely = df.groupby("batter", sort=True)
    n          = len(ryhmittely)

    for idx, (batter_id, ryhma) in enumerate(ryhmittely, start=1):

        koko = laske_woba(ryhma)

        if koko["wOBA"] is None or koko["PA_w"] < MIN_PA_W_KOKO:
            if idx % 100 == 0 or idx == n:
                print(f"   [{idx:>5}/{n}] laskettu ...", end="\r")
            continue

        woba_all = koko["wOBA"]
        fallback = woba_all

        woba_vs_l = laske_split_woba(ryhma, "L", fallback)
        woba_vs_r = laske_split_woba(ryhma, "R", fallback)

        rivit.append({
            "Batter_ID":  int(batter_id),
            "wOBA_All":   woba_all,
            "wOBA_vs_L":  woba_vs_l,
            "wOBA_vs_R":  woba_vs_r,
            "ISO":        koko["ISO"], # UUSI
            "PA_raw":     koko["PA_raw"],
        })

        if idx % 100 == 0 or idx == n:
            print(f"   [{idx:>5}/{n}] laskettu ...", end="\r")

    print()

    df_out = pd.DataFrame(rivit)
    print(f"   → {n:,} lyöjästä {len(df_out):,} läpäisi PA_w ≥ {MIN_PA_W_KOKO} -suodatuksen")
    return df_out.sort_values("wOBA_All", ascending=False).reset_index(drop=True)

# ---------------------------------------------------------------------------
# 7. TALLENNUS
# ---------------------------------------------------------------------------
def tallenna(df: pd.DataFrame, db_polku: str = DB_POLKU) -> None:
    try:
        yhteys = sqlite3.connect(db_polku)
        df.to_sql(KOHDE_TAULU, yhteys, if_exists="replace", index=False)
        yhteys.close()
        print(f"\n✅ Tallennettu {len(df):,} lyöjää tauluun '{KOHDE_TAULU}'")
    except sqlite3.Error as e:
        raise RuntimeError(f"❌ SQLite-virhe tallennuksessa: {e}") from e

# ---------------------------------------------------------------------------
# 8. TULOSTUS
# ---------------------------------------------------------------------------
def tulosta_top10(df: pd.DataFrame) -> None:
    viiva = "─" * 78
    print(f"\n{viiva}")
    print(
        f"  🏆 TOP-10 LYÖJÄT – aikapainotettu wOBA  "
        f"(PA ≥ {MIN_PA_LISTAUS})"
    )
    print(viiva)

    top = df[df["PA_raw"] >= MIN_PA_LISTAUS].nlargest(10, "wOBA_All")

    if top.empty:
        print(f"  Ei lyöjiä joilla PA ≥ {MIN_PA_LISTAUS}")
        return

    print(
        f"  {'#':<4} {'Batter_ID':<12} {'wOBA_All':>9} "
        f"{'vs_L':>8} {'vs_R':>8} {'ISO':>6} {'PA':>6}"
    )
    print(f"  {'─'*4} {'─'*12} {'─'*9} {'─'*8} {'─'*8} {'─'*6} {'─'*6}")

    for rank, (_, r) in enumerate(top.iterrows(), start=1):
        l_flag = " " if r["wOBA_vs_L"] != r["wOBA_All"] else "~"
        r_flag = " " if r["wOBA_vs_R"] != r["wOBA_All"] else "~"
        print(
            f"  {rank:<4} {int(r['Batter_ID']):<12} {r['wOBA_All']:>9.3f} "
            f"  {r['wOBA_vs_L']:>5.3f}{l_flag}  {r['wOBA_vs_R']:>5.3f}{r_flag} "
            f"{r['ISO']:>6.3f} {int(r['PA_raw']):>6}"
        )

    print(f"\n  ~ = split-arvo on fallback (PA_w < {MIN_PA_W_SPLIT}, käytetään wOBA_All)")
    print(viiva)

def tulosta_yhteenveto(df: pd.DataFrame) -> None:
    viiva = "─" * 52
    riittava_pa = df[df["PA_raw"] >= MIN_PA_LISTAUS]
    print(f"\n{viiva}")
    print(f"  📊 YHTEENVETO – lyöjät_statcast")
    print(viiva)
    print(f"  Lyöjiä yhteensä (PA_w ≥ {MIN_PA_W_KOKO}): {len(df):>6,}")
    print(f"  Lyöjiä (PA ≥ {MIN_PA_LISTAUS}):             {len(riittava_pa):>6,}")
    if len(riittava_pa) > 0:
        print(f"  wOBA-alue: {riittava_pa['wOBA_All'].min():.3f} – "
              f"{riittava_pa['wOBA_All'].max():.3f}  |  "
              f"Keskiarvo: {riittava_pa['wOBA_All'].mean():.3f}")
    print(f"  Taulu: '{KOHDE_TAULU}'  |  Tietokanta: '{DB_POLKU}'")
    print(viiva)

# ---------------------------------------------------------------------------
# PÄÄOHJELMA
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    viiva = "═" * 62
    print(f"\n{viiva}")
    print(f"  ⚾  wOBA + SPLITS + ISO  –  Statcast 2025")
    print(f"  Puoliintumisaika: {int(PUOLIINTUMISAIKA)} pv  "
          f"|  Min PA_w: {MIN_PA_W_KOKO}  |  Split min PA_w: {MIN_PA_W_SPLIT}")
    print(viiva)

    df_raa = lue_data()
    df = suodata_pelikategoria(df_raa)
    
    print("\n⏳ Lasketaan aikapainot ...")
    df = lisaa_painot(df)

    print(f"\n{'─'*62}")
    df_lyojat = laske_lyojatilastot(df)

    tallenna(df_lyojat)

    tulosta_top10(df_lyojat)
    tulosta_yhteenveto(df_lyojat)
    print(f"\n  Yhdistetty onnistuneesti! ISO-sarake lisätty tietokantaan.")