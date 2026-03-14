"""
laskentamoottori.py – v5.0 (Dynamic wOBA & IP/GS Weighting)
==========================================================
Tämä on mallin aivot. Laskee voittotodennäköisyydet ja juoksuodotukset.
Sisältää 1% Momentum- ja H2H-painotuksen
"""

import sqlite3
import pandas as pd
import numpy as np

DB_POLKU = "mlb_historical.db"
LIIGA_XFIP_KA = 3.80
LIIGA_WOBA_KA = 0.310

def hae_momentum(koti_nimi, vieras_nimi):
    """
    Lukee ottelutulokset-taulusta joukkueiden historian.
    Laskee H2H-edun ja Kuntopuntari-edun (viimeiset 10 peliä).
    Palauttaa momentum-edun (Edge), joka on maksimissaan n. 0.015 (n. 1.5%).
    """
    vuosi = date.today().year
    taulu = f"ottelutulokset_{vuosi}"
    
    try:
        conn = sqlite3.connect(DB_POLKU)
        df = pd.read_sql_query(f"SELECT * FROM {taulu}", conn)
        conn.close()
    except:
        # Jos taulua ei ole (esim. kauden eka päivä), ei anneta momentum-etua
        return 0.0

    if df.empty:
        return 0.0

    # 1. KESKINÄISET OTTELUT (H2H)
    # Etsitään pelit, joissa nämä kaksi joukkuetta ovat kohdanneet
    h2h_pelit = df[
        ((df['Kotijoukkue'] == koti_nimi) & (df['Vierasjoukkue'] == vieras_nimi)) |
        ((df['Kotijoukkue'] == vieras_nimi) & (df['Vierasjoukkue'] == koti_nimi))
    ]
    
    koti_h2h_voitot = 0
    vieras_h2h_voitot = 0
    
    for _, peli in h2h_pelit.iterrows():
        if peli['Koti_Juoksut'] > peli['Vieras_Juoksut']:
            if peli['Kotijoukkue'] == koti_nimi: koti_h2h_voitot += 1
            else: vieras_h2h_voitot += 1
        else:
            if peli['Vierasjoukkue'] == koti_nimi: koti_h2h_voitot += 1
            else: vieras_h2h_voitot += 1
            
    h2h_yht = koti_h2h_voitot + vieras_h2h_voitot
    
    # 2. KUNTOPUNTARI (Viimeiset 10 peliä per joukkue)
    def laske_kunto(joukkue):
        pelit = df[(df['Kotijoukkue'] == joukkue) | (df['Vierasjoukkue'] == joukkue)].tail(10)
        voitot = 0
        for _, p in pelit.iterrows():
            if p['Kotijoukkue'] == joukkue and p['Koti_Juoksut'] > p['Vieras_Juoksut']: voitot += 1
            elif p['Vierasjoukkue'] == joukkue and p['Vieras_Juoksut'] > p['Koti_Juoksut']: voitot += 1
        return voitot / max(len(pelit), 1)

    koti_kunto = laske_kunto(koti_nimi)
    vieras_kunto = laske_kunto(vieras_nimi)
    
    # 3. YHDISTETÄÄN EDUKSI (Matemaattinen skaalaus)
    # Jos koti on voittanut kaikki H2H-pelit, h2h_etu on +0.005
    h2h_etu = 0.0
    if h2h_yht > 0:
        h2h_etu = ((koti_h2h_voitot / h2h_yht) - 0.5) * 0.01  # Max +/- 0.005 Edge
        
    # Jos koti on voittanut 10/10 ja vieras 0/10, kunto_etu on +0.005
    kunto_etu = (koti_kunto - vieras_kunto) * 0.005

    # Palautetaan yhteinen momentum-etu kotijoukkueen näkökulmasta
    return h2h_etu + kunto_etu

def laske_todennakoisyys(koti_nimi, vieras_nimi, df, koti_sp, koti_bp, koti_woba, vieras_sp, vieras_bp, vieras_woba):
    """
    Laskee ottelun lopputuloksen dynaamisilla painotuksilla.
    """
    
    # 1. Päätellään syöttäjien kesto (IP per Start)
    # MLB-peli kestää 9 vuoroparia.
    koti_sp_ip = koti_sp.get("IP", 5.5)
    vieras_sp_ip = vieras_sp.get("IP", 5.5)
    
    # 2. Lasketaan kummankin joukkueen syöttövoiman painotettu keskiarvo (xFIP)
    # Painotus perustuu siihen, kuinka suuren osan pelistä SP hoitaa
    def laske_tiimin_xfip(sp_data, bp_data, ip_start):
        sp_paino = max(0.1, min(0.9, ip_start / 9.0)) # Rajoitetaan välille 10% - 90%
        bp_paino = 1.0 - sp_paino
        
        # Käytetään syöttäjille aina heidän absoluuttista perustasoaan (All), 
        # koska kätisyysetu (Platoon split) on jo laskettu sisään lyöjien wOBA-arvoihin!
        sp_xfip = sp_data.get("xFIP_All", LIIGA_XFIP_KA)
        bp_xfip = bp_data.get("All", LIIGA_XFIP_KA)
        
        return (sp_xfip * sp_paino) + (bp_xfip * bp_paino)

    koti_syotto_total = laske_tiimin_xfip(koti_sp, koti_bp, koti_sp_ip)
    vieras_syotto_total = laske_tiimin_xfip(vieras_sp, vieras_bp, vieras_sp_ip)

    # 3. Voimasuhteiden vertailu (wOBA vs xFIP)
    # Lasketaan "Edge" kummallekin joukkueelle
    # Parempi hyökkäys (wOBA) ja huonompi vastustajan syöttö (xFIP) lisäävät etua
    koti_etu = (koti_woba / LIIGA_WOBA_KA) - (vieras_syotto_total / LIIGA_XFIP_KA)
    vieras_etu = (vieras_woba / LIIGA_WOBA_KA) - (koti_syotto_total / LIIGA_XFIP_KA)
    
    # Lisätään kotikenttäetu (historiallisesti n. 3-4%)
    koti_etu += 0.035

    # LISÄTÄÄN MOMENTUM JA H2H (Noin 1% vaikutus)
    momentum = hae_momentum(koti_nimi, vieras_nimi)
    koti_etu += momentum

    # 4. Muutetaan etu todennäköisyydeksi (Logistinen funktio)
    ero = koti_etu - vieras_etu
    # Kerroin 5.0 on kalibroitu MLB:n varianssiin
    koti_tod = 1 / (1 + np.exp(-5.0 * ero))
    vieras_tod = 1 - koti_tod

    # 5. Juoksuodotus (O/U)
    # Perustuu joukkueiden wOBA-tasoihin suhteessa syöttäjiin
    perus_odotus = 8.6 # MLB keskiarvo
    k_odotus = (perus_odotus / 2) * (koti_woba / LIIGA_WOBA_KA) * (vieras_syotto_total / LIIGA_XFIP_KA)
    v_odotus = (perus_odotus / 2) * (vieras_woba / LIIGA_WOBA_KA) * (koti_syotto_total / LIIGA_XFIP_KA)
    total_odotus = k_odotus + v_odotus

    return {
        "koti_voitto_tod": koti_tod,
        "vieras_voitto_tod": vieras_tod,
        "total_odotus": total_odotus,
        "k_odotus": k_odotus,
        "v_odotus": v_odotus,
        "koti_sp_dyn": koti_sp.get("xFIP_All", LIIGA_XFIP_KA),
        "koti_bp_dyn": koti_bp.get("vs_R", LIIGA_XFIP_KA),
        "koti_total_xfip": koti_syotto_total,
        "vieras_sp_dyn": vieras_sp.get("xFIP_All", LIIGA_XFIP_KA),
        "vieras_bp_dyn": vieras_bp.get("vs_R", LIIGA_XFIP_KA),
        "vieras_total_xfip": vieras_syotto_total,
        "momentum_edge": momentum # Palautetaan UI:lle näytettäväksi, jos halutaan!
    }
