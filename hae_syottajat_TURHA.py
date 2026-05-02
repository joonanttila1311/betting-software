"""
hae_syottajat.py (Versio 2 - MLB API)
=====================================
Hakee MLB-syöttäjien tilastot kaudelta 2025 MLB:n virallisesta rajapinnasta
ohittaen pybaseballin FanGraphs-ongelmat.
"""

import sqlite3
import pandas as pd
import requests

# VAKIOT
DB_POLKU = "mlb_historical.db"
TAULU = "syottajat_2025"
KAUSI = 2025

def hae_mlb_syottajat():
    print(f"⏳ Haetaan syöttäjätilastot kaudelta {KAUSI} MLB:n virallisesta API:sta...")
    
    # MLB Stats API -osoite, joka hakee koko kauden syöttäjätilastot
    url = f"https://statsapi.mlb.com/api/v1/stats?stats=season&group=pitching&season={KAUSI}&playerPool=All&limit=2000"
    
    vastaus = requests.get(url)
    vastaus.raise_for_status() # Kaatuu nätisti jos API on alhaalla
    
    data = vastaus.json()
    
    syottajat_lista = []
    
    # Käydään läpi API:n palauttama data
    # API palauttaa datan 'stats' -> 'splits' -rakenteen alla
    for stat_group in data.get('stats', []):
        for pelaaja_data in stat_group.get('splits', []):
            
            pelaaja = pelaaja_data.get('player', {})
            tilastot = pelaaja_data.get('stat', {})
            joukkue = pelaaja_data.get('team', {})
            
            # Otetaan vain ne, joilla on syötettyjä vuoropareja
            ip_str = tilastot.get('inningsPitched', '0.0')
            try:
                ip = float(ip_str)
            except ValueError:
                ip = 0.0
                
            if ip > 0:
                syottajat_lista.append({
                    "Name": pelaaja.get('fullName', 'Tuntematon'),
                    "Team": joukkue.get('name', 'Tuntematon'), # Tämä antaa koko nimen esim. "New York Yankees"
                    "G": tilastot.get('gamesPlayed', 0),
                    "GS": tilastot.get('gamesStarted', 0),
                    "IP": ip,
                    "ERA": float(tilastot.get('era', '0.00')),
                    "WHIP": float(tilastot.get('whip', '0.00'))
                })
                
    df = pd.DataFrame(syottajat_lista)
    print(f"✅ Data haettu! Löytyi {len(df)} syöttäjää.")
    return df

def tallenna_tietokantaan(df):
    print("⏳ Tallennetaan tietokantaan...")
    yhteys = sqlite3.connect(DB_POLKU)
    df.to_sql(TAULU, yhteys, if_exists="replace", index=False)
    yhteys.close()
    print(f"✅ Tallennettu tietokantaan tauluun '{TAULU}'.")

if __name__ == "__main__":
    df = hae_mlb_syottajat()
    
    # Suodatetaan pelkkää "kohinaa" pois: vain ne, jotka ovat pelanneet vähintään 3 peliä
    df_siivottu = df[df['G'] >= 3].reset_index(drop=True)
    
    tallenna_tietokantaan(df_siivottu)
    
    print("\nTässä 5 ensimmäistä riviä (järjestettynä ERA:n mukaan):")
    # Näytetään 5 parasta (joilla ERA on pienin)
    print(df_siivottu.sort_values('ERA').head())