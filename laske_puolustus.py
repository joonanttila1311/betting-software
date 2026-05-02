"""
laske_puolustus.py  –  v1.0  (Defensive Efficiency + Time Decay)
===========================================================
Laskee joukkuekohtaisen aikapainotetun puolustustilaston (DER)
Statcast 2025 -raakadatasta. Mittaa kuinka tehokkaasti joukkue
muuttaa kentälle lyödyt pallot (BIP) paloiksi.
"""

import sqlite3
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# VAKIOT
# ---------------------------------------------------------------------------
DB_POLKU        = "mlb_historical.db"
LAHDE_TAULU     = "statcast_2025"
KOHDE_TAULU     = "puolustus_statcast"

PUOLIINTUMISAIKA = 45.0    # päiviä: paino putoaa puoleen 90 pv:ssä
MIN_BIP_W_KOKO   = 20.0    # minimi painotettu BIP koko kaudelle (alle → poistetaan)
LIIGA_DER_KA     = 0.690    # MLB:n historiallinen keskiarvo (DER)

# Statcast-tapahtumat, jotka lasketaan peliin jääneiksi palloiksi (BIP)
BIP_EVENTS = [
    'field_out', 'single', 'double', 'triple', 'fielders_choice', 
    'fielders_choice_out', 'double_play', 'force_out', 'grounded_into_double_play',
    'field_error', 'sac_fly', 'sac_bunt'  # <-- LISÄTTY VIRHEET JA UHRILYÖNNIT
]

# Tapahtumat, joista puolustus onnistui tekemään palon
OUT_EVENTS = [
    'field_out', 'fielders_choice_out', 'double_play', 'force_out', 'grounded_into_double_play',
    'fielders_choice', 'sac_fly', 'sac_bunt' # <-- LISÄTTY FC JA UHRILYÖNNIT
]

# ---------------------------------------------------------------------------
# 1. DATAN HAKU (KORJATTU)
# ---------------------------------------------------------------------------

def lue_data(db_polku: str = DB_POLKU) -> pd.DataFrame:
    """Lukee Statcast-tapahtumat ja päättelee puolustavan joukkueen."""
    if not Path(db_polku).exists():
        raise FileNotFoundError(f"Tietokantaa '{db_polku}' ei löydy.")

    yhteys = sqlite3.connect(db_polku)
    
    # Haetaan tarvittavat sarakkeet field_team-päätelmää varten
    kysely = f"""
    SELECT home_team, away_team, inning_topbot, events, game_date, game_type 
    FROM {LAHDE_TAULU} 
    WHERE events IN ({','.join(["'"+e+"'" for e in BIP_EVENTS])})
    """
    df = pd.read_sql_query(kysely, yhteys)
    yhteys.close()

    if df.empty:
        return df

    # PÄÄTELLÄÄN PUOLUSTAVA JOUKKUE (field_team):
    # Jos inning_topbot on 'Top', kotijoukkue puolustaa. Muuten vierasjoukkue.
    df['field_team'] = np.where(df['inning_topbot'] == 'Top', df['home_team'], df['away_team'])
    
    return df

# ---------------------------------------------------------------------------
# 2. PELIKATEGORIASUODATUS
# ---------------------------------------------------------------------------

def suodata_pelikategoria(df: pd.DataFrame) -> pd.DataFrame:
    """Pitää vain runkosarjan (R) ja pudotuspelit (P)."""
    if "game_type" in df.columns:
        alku_lkm = len(df)
        df = df[df["game_type"].isin(["R", "P"])]
        counts = df["game_type"].value_counts().to_dict()
        tyypit_str = ", ".join([f"{k}: {v:,}" for k, v in counts.items()])
        print(f"    → Pelityypit (game_type): {tyypit_str} (Poistettu {alku_lkm - len(df):,} riviä)")
    return df

# ---------------------------------------------------------------------------
# 3. PUOLUSTUKSEN TIME DECAY (SYNCHRONIZED WITH HITTERS/PITCHERS)
# ---------------------------------------------------------------------------
def laske_puolustus_painot(df: pd.DataFrame) -> pd.DataFrame:
    import numpy as np
    from datetime import datetime
    
    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df = df.dropna(subset=["game_date"])

    nykyhetki = pd.to_datetime('today')
    nykyinen_vuosi = nykyhetki.year

    # Jaetaan data nykyiseen ja menneisiin kausiin
    menneet_kaudet = df[df['game_date'].dt.year < nykyinen_vuosi]
    nykyinen_kausi = df[df['game_date'].dt.year == nykyinen_vuosi]

    # Lasketaan kalenteripäivät tästä hetkestä
    df["days_ago"] = (nykyhetki - df["game_date"]).dt.days

    # Lasketaan offseason-tauon pituus (jäädytys)
    if not menneet_kaudet.empty:
        t_last = menneet_kaudet['game_date'].max() # Viime vuoden viimeinen peli

        if not nykyinen_kausi.empty:
            # Etsitään kauden virallinen alku (maaliskuun 20. jälkeen)
            tosipelit = nykyinen_kausi[(nykyinen_kausi['game_date'].dt.month >= 3) & 
                                       (nykyinen_kausi['game_date'].dt.day >= 20)]
            
            if not tosipelit.empty:
                t_first = tosipelit['game_date'].min()
                # Tauko on päivien erotus miinus 30 päivän puskuri
                offseason_tauko = max(0, (t_first - t_last).days - 30)
            else:
                offseason_tauko = max(0, (nykyhetki - t_last).days - 30)
        else:
            offseason_tauko = max(0, (nykyhetki - t_last).days - 30)

        # VÄHENNETÄÄN TAUKO VANHOISTA PELEISTÄ (Jäädytys)
        df['days_ago'] = np.where(
            df['game_date'].dt.year < nykyinen_vuosi,
            df['days_ago'] - offseason_tauko,
            df['days_ago']
        )

    df['days_ago'] = df['days_ago'].clip(lower=0)
    
    # Lasketaan lopullinen paino puoliintumisajan avulla
    df["time_weight"] = 0.5 ** (df["days_ago"] / PUOLIINTUMISAIKA)

    # Huomioidaan mahdolliset harjoituspelit (Spring Training)
    if "game_type" in df.columns:
        df['game_weight'] = np.where(df['game_type'] == 'S', 0.5, 1.0) # Esim. 0.5 paino harkkapeleille
    else:
        df['game_weight'] = 1.0
        
    df["weight"] = df["time_weight"] * df["game_weight"]

    return df

# ---------------------------------------------------------------------------
# 4. VARSINAINEN PUOLUSTUSLASKENTA
# ---------------------------------------------------------------------------

def laske_puolustus_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Laskee joukkuekohtaisen DER:n ja Puolustus_Kertoimen."""
    
    # Merkitään onko suoritus palo (1) vai hitti (0)
    df['is_out'] = df['events'].apply(lambda x: 1 if x in OUT_EVENTS else 0)

    rivit = []
    # Ryhmitellään puolustavan joukkueen (field_team) mukaan
    for joukkue, ryhma in df.groupby('field_team'):
        painotettu_bip = ryhma['weight'].sum()
        
        # 5. PÄÄLOGIIKKA (Suodatus ja Kaava)
        if painotettu_bip < MIN_BIP_W_KOKO:
            continue
            
        painotettu_palot = (ryhma['is_out'] * ryhma['weight']).sum()
        
        # DER = Defensive Efficiency Ratio
        der = painotettu_palot / painotettu_bip
        
        # Kerroin suhteessa liigan keskiarvoon
        # Pienempi kerroin (< 1.0) = parempi puolustus
        puolustus_kerroin = round(LIIGA_DER_KA / der, 3)
        
        rivit.append({
            'Team': joukkue,
            'DER': round(der, 4),
            'Puolustus_Kerroin': puolustus_kerroin,
            'BIP_raw': len(ryhma),
            'BIP_w': round(painotettu_bip, 1)
        })

    return pd.DataFrame(rivit)

# ---------------------------------------------------------------------------
# 6. TALLENNUS
# ---------------------------------------------------------------------------

def tallenna_kantaan(df: pd.DataFrame, taulu: str = KOHDE_TAULU):
    """Tallentaa tulokset SQLite-tietokantaan."""
    yhteys = sqlite3.connect(DB_POLKU)
    df.to_sql(taulu, yhteys, if_exists='replace', index=False)
    yhteys.close()
    print(f"    → Tulokset tallennettu tauluun: '{taulu}'")

# ---------------------------------------------------------------------------
# 7. TULOSTUS
# ---------------------------------------------------------------------------

def tulosta_yhteenveto(df: pd.DataFrame) -> None:
    viiva = "─" * 52
    print(f"\n{viiva}")
    print(f"  📊 YHTEENVETO – {KOHDE_TAULU}")
    print(viiva)
    print(f"  Joukkueita analysoitu: {len(df):>6,}")
    
    if len(df) > 0:
        top_5 = df.sort_values("Puolustus_Kerroin").head(5)
        print(f"\n  Parhaat puolustukset (Kerroin < 1.0):")
        for _, r in top_5.iterrows():
            print(f"  {r['Team']:<8} DER: {r['DER']:.3f} | Kerroin: {r['Puolustus_Kerroin']:.3f}")
            
        print(f"\n  Heikoimmat puolustukset (Kerroin > 1.0):")
        bottom_5 = df.sort_values("Puolustus_Kerroin", ascending=False).head(5)
        for _, r in bottom_5.iterrows():
            print(f"  {r['Team']:<8} DER: {r['DER']:.3f} | Kerroin: {r['Puolustus_Kerroin']:.3f}")

    print(f"\n  Taulu: '{KOHDE_TAULU}'  |  Keskiarvo (DER): {LIIGA_DER_KA}")
    print(viiva)

# ---------------------------------------------------------------------------
# PÄÄOHJELMA
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    viiva = "═" * 62
    print(f"\n{viiva}")
    print(f"  ⚾  DEFENSE v1.0 – TIME DECAY + DER | Statcast 2025")
    print(f"  Puoliintumisaika: {int(PUOLIINTUMISAIKA)} pv  |  Min BIP_w: {MIN_BIP_W_KOKO}")
    print(viiva)

    try:
        # 1. DATAN HAKU
        print("\n  1. Luetaan Statcast-dataa...")
        df_raaka = lue_data()
        print(f"    → Rivejä haettu: {len(df_raaka):,}")

        # 2. PELIKATEGORIASUODATUS
        print("  2. Suodatetaan pelikategoriat (R & P)...")
        df_suodatettu = suodata_pelikategoria(df_raaka)

        # 3. TIME DECAY -PAINOT
        print("  3. Lasketaan aikapainotetut arvot...")
        df_painotettu = laske_puolustus_painot(df_suodatettu)

        # 4 & 5. LASKENTA & PÄÄLOGIIKKA
        print("  4. Suoritetaan puolustuslaskenta (DER)...")
        df_tulokset = laske_puolustus_stats(df_painotettu)

        # 6. TALLENNUS
        print("  5. Tallennetaan tulokset...")
        tallenna_kantaan(df_tulokset)

        # 7. TULOSTUS
        tulosta_yhteenveto(df_tulokset)

    except Exception as e:
        print(f"\n❌ VIRHE: {e}")

    print(f"\n{viiva}\n")