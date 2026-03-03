import requests
import pandas as pd
import sqlite3

def hae_ottelutulokset_api():
    vuosi = 2025
    print(f"Odotas, haetaan koko MLB-kauden {vuosi} ottelutulokset virallisesta rajapinnasta...")
    
    # MLB:n virallinen ja ilmainen API-osoite otteluohjelmalle
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={vuosi}"
    
    # Haetaan data
    vastaus = requests.get(url)
    data = vastaus.json() # Muutetaan vastaus Pythonin ymmärtämään muotoon (JSON)
    
    pelit_lista = []
    
    # Käydään läpi kaikki päivät ja niiden pelit
    for paiva_data in data.get('dates', []):
        paivamaara = paiva_data['date']
        
        for peli in paiva_data.get('games', []):
            # Otetaan mukaan vain pelit, jotka on pelattu loppuun (Final)
            if peli['status']['abstractGameState'] == 'Final':
                
                # Poimitaan koti- ja vierasjoukkueen nimet ja juoksut
                koti = peli['teams']['home']['team']['name']
                koti_juoksut = peli['teams']['home'].get('score', 0)
                
                vieras = peli['teams']['away']['team']['name']
                vieras_juoksut = peli['teams']['away'].get('score', 0)
                
                # Lisätään peli listaan
                pelit_lista.append({
                    'Paivamaara': paivamaara,
                    'Kotijoukkue': koti,
                    'Koti_Juoksut': koti_juoksut,
                    'Vierasjoukkue': vieras,
                    'Vieras_Juoksut': vieras_juoksut
                })
                
    # Muutetaan lista Pandas-taulukoksi, jota on helppo käsitellä
    df = pd.DataFrame(pelit_lista)
    print(f"\nLoistavaa! Löytyi {len(df)} pelattua ottelua vuodelta {vuosi}.")
    
    # Tallennetaan SQLite-tietokantaan
    tietokanta_nimi = 'mlb_historical.db'
    conn = sqlite3.connect(tietokanta_nimi)
    taulun_nimi = f'ottelutulokset_{vuosi}'
    
    df.to_sql(taulun_nimi, conn, if_exists='replace', index=False)
    conn.close()
    
    print(f"Data tallennettu tietokantaan: '{tietokanta_nimi}' tauluun '{taulun_nimi}'!")
    print("\nTässä ensimmäiset 5 peliä:")
    print(df.head())

if __name__ == "__main__":
    hae_ottelutulokset_api()