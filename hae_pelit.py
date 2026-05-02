import requests
import pandas as pd
import sqlite3

def hae_ottelutulokset_api():
    vuosi = 2026
    print(f"Odotas, haetaan koko MLB-kauden {vuosi} ottelutulokset virallisesta rajapinnasta...")
    
    # LISÄTTY SUODATIN: &gameType=R,F,D,L,W (Vain runkosarja ja pudotuspelit)
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={vuosi}&gameType=R,F,D,L,W"
    
    # Haetaan data
    try:
        vastaus = requests.get(url, timeout=30)
        vastaus.raise_for_status()
        data = vastaus.json()
    except requests.RequestException as e:
        print(f"\n❌ Datan haku epäonnistui: {e}")
        print("   → Skripti lopetetaan, kantaa ei muuteta.")
        return
    
    pelit_lista = []
    
    # Käydään läpi kaikki päivät ja niiden pelit
    for paiva_data in data.get('dates', []):
        paivamaara = paiva_data['date']
        
        for peli in paiva_data.get('games', []):
            # Otetaan mukaan vain pelit, jotka on pelattu loppuun (Final)
            if peli['status']['abstractGameState'] == 'Final':
                
                # TUPLAVARMISTUS: Hylätään S (Spring) ja E (Exhibition)
                game_type = peli.get('gameType', '')
                if game_type not in ['R', 'F', 'D', 'L', 'W']:
                    continue
                
                # Poimitaan koti- ja vierasjoukkueen nimet, juoksut ja peli-ID
                game_pk = peli.get('gamePk')
                
                koti = peli['teams']['home']['team']['name']
                koti_juoksut = peli['teams']['home'].get('score')
                
                vieras = peli['teams']['away']['team']['name']
                vieras_juoksut = peli['teams']['away'].get('score')
                
                # Skipataan pelit joissa pisteet puuttuvat tai ovat None
                # (esim. forfeit-pelit, keskeytetyt pelit, rajapinnan poikkeustilat)
                if koti_juoksut is None or vieras_juoksut is None:
                    print(f"   ⚠️  Skipataan peli {paivamaara}: {vieras} @ {koti} – pisteet puuttuvat")
                    continue
                
                # Skipataan pelit joista puuttuu Game_Pk (ei pitäisi tapahtua, mutta varmistus)
                if game_pk is None:
                    print(f"   ⚠️  Skipataan peli {paivamaara}: {vieras} @ {koti} – Game_Pk puuttuu")
                    continue
                
                # Lisätään peli listaan
                pelit_lista.append({
                    'Paivamaara': paivamaara,
                    'Kotijoukkue': koti,
                    'Koti_Juoksut': koti_juoksut,
                    'Vierasjoukkue': vieras,
                    'Vieras_Juoksut': vieras_juoksut,
                    'Game_Pk': game_pk
                })
                
    # Muutetaan lista Pandas-taulukoksi, jota on helppo käsitellä
    df = pd.DataFrame(pelit_lista)
    
    if df.empty:
        print(f"\n⚠️  Rajapinta ei palauttanut yhtään peliä vuodelle {vuosi}.")
        print("   → Vanhaa kantaa ei kosketa, se säilyy ennallaan.")
        return
        
    print(f"\nLoistavaa! Löytyi {len(df)} pelattua kilpailullista ottelua vuodelta {vuosi}.")
    
    # Tallennetaan SQLite-tietokantaan (append + dedup -strategia)
    tietokanta_nimi = 'mlb_historical.db'
    yhteys = sqlite3.connect(tietokanta_nimi)
    taulun_nimi = f'ottelutulokset_{vuosi}'
    
    # Tarkistetaan, onko taulussa jo dataa ja onko siellä Game_Pk-sarake
    try:
        vanha_df = pd.read_sql_query(f"SELECT * FROM {taulun_nimi}", yhteys)
        taulu_olemassa = True
        sarakkeet_yhteensopivat = 'Game_Pk' in vanha_df.columns
    except Exception:
        vanha_df = pd.DataFrame()
        taulu_olemassa = False
        sarakkeet_yhteensopivat = False
    
    if not taulu_olemassa:
        # Ensimmäinen ajo: ei vanhaa dataa, luodaan taulu uutena
        print(f"   📌 Taulua ei ole vielä – luodaan se ja tallennetaan {len(df)} peliä.")
        df.to_sql(taulun_nimi, yhteys, if_exists='replace', index=False)
    elif not sarakkeet_yhteensopivat:
        # Vanha taulu on olemassa, mutta siitä puuttuu Game_Pk-sarake.
        # Korvataan se uudella, yhteensopivalla rakenteella.
        print(f"   🔄 Vanha taulu päivitetään uuteen rakenteeseen (lisätään Game_Pk).")
        df.to_sql(taulun_nimi, yhteys, if_exists='replace', index=False)
        rivit_tallennettu = len(df)
        print(f"   → Korvattu {len(vanha_df)} vanhaa riviä {rivit_tallennettu} uudella.")
    else:
        # Normaalitilanne: yhdistetään vanha + uusi, dedupataan, tallennetaan
        rivit_ennen = len(vanha_df)
        yhdistetty = pd.concat([vanha_df, df], ignore_index=True)
        # keep='last' = uusin haku voittaa, jos sama Game_Pk löytyy molemmista
        yhdistetty = yhdistetty.drop_duplicates(subset=['Game_Pk'], keep='last')
        rivit_jalkeen = len(yhdistetty)
        uudet_rivit = rivit_jalkeen - rivit_ennen
        
        yhdistetty.to_sql(taulun_nimi, yhteys, if_exists='replace', index=False)
        print(f"   📊 Vanhassa kannassa: {rivit_ennen} peliä")
        print(f"   📊 Uudessa kannassa:  {rivit_jalkeen} peliä  (+{uudet_rivit} uutta)")
        # Päivitetään df muuttujaa, jotta loppuviestit näyttävät oikean datan
        df = yhdistetty
    
    yhteys.close()
    
    print(f"Data tallennettu tietokantaan: '{tietokanta_nimi}' tauluun '{taulun_nimi}'!")
    print("\nTässä ensimmäiset 5 peliä:")
    print(df.head())

if __name__ == "__main__":
    hae_ottelutulokset_api()