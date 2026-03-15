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

def laske_todennakoisyys(koti_nimi, vieras_nimi, koti_sp, koti_bp, koti_woba, vieras_sp, vieras_bp, vieras_woba, koti_woba_bp=None, vieras_woba_bp=None):
    """
    Laskee ottelun lopputuloksen kahdessa dynaamisessa vaiheessa: 
    Aloitussyöttäjä-vaihe ja Bullpen-vaihe.
    """
    # TURVAVERKKO: Jos uutta BP-wobaa ei syötetä, käytetään vanhaa yhtenäistä tapaa
    if koti_woba_bp is None: koti_woba_bp = koti_woba
    if vieras_woba_bp is None: vieras_woba_bp = vieras_woba

    # HAETAAN SYÖTTÖVUOROT (IP) - Tämä määrittää prosentit!
    koti_sp_ip = koti_sp.get("IP", 5.5)
    vieras_sp_ip = vieras_sp.get("IP", 5.5)
    
    # LASKETAAN DYNAAMISET PAINOTUKSET
    koti_sp_paino = max(0.1, min(0.9, koti_sp_ip / 9.0))
    koti_bp_paino = 1.0 - koti_sp_paino
    
    vieras_sp_paino = max(0.1, min(0.9, vieras_sp_ip / 9.0))
    vieras_bp_paino = 1.0 - vieras_sp_paino

    # HAETAAN xFIP ARVOT (SP = All, BP = All)
    koti_sp_xfip = koti_sp.get("xFIP_All", LIIGA_XFIP_KA)
    koti_bp_xfip = koti_bp.get("All", LIIGA_XFIP_KA)
    vieras_sp_xfip = vieras_sp.get("xFIP_All", LIIGA_XFIP_KA)
    vieras_bp_xfip = vieras_bp.get("All", LIIGA_XFIP_KA)

    # -------------------------------------------------------------
    # 1. KOTIJOUKKUEEN HYÖKKÄYSETU (Vastassa Vieras_SP ja Vieras_BP)
    # -------------------------------------------------------------
    # Vaihe A: Koti hyökkää vieraan aloittajaa vastaan (Kätisyys-wOBA vs SP_xFIP)
    koti_etu_sp = (koti_woba / LIIGA_WOBA_KA) - (vieras_sp_xfip / LIIGA_XFIP_KA)
    
    # Vaihe B: Koti hyökkää vieraan bullpeniä vastaan (wOBA_All vs BP_xFIP)
    koti_etu_bp = (koti_woba_bp / LIIGA_WOBA_KA) - (vieras_bp_xfip / LIIGA_XFIP_KA)
    
    # Yhdistetään vastustajan kestävyyden (IP) mukaan!
    koti_etu_perus = (koti_etu_sp * vieras_sp_paino) + (koti_etu_bp * vieras_bp_paino)

    # -------------------------------------------------------------
    # 2. VIERASJOUKKUEEN HYÖKKÄYSETU (Vastassa Koti_SP ja Koti_BP)
    # -------------------------------------------------------------
    # Vaihe A: Vieras hyökkää kodin aloittajaa vastaan
    vieras_etu_sp = (vieras_woba / LIIGA_WOBA_KA) - (koti_sp_xfip / LIIGA_XFIP_KA)
    
    # Vaihe B: Vieras hyökkää kodin bullpeniä vastaan
    vieras_etu_bp = (vieras_woba_bp / LIIGA_WOBA_KA) - (koti_bp_xfip / LIIGA_XFIP_KA)
    
    # Yhdistetään kotijoukkueen kestävyyden (IP) mukaan!
    vieras_etu_perus = (vieras_etu_sp * koti_sp_paino) + (vieras_etu_bp * koti_bp_paino)

    # -------------------------------------------------------------
    # 3. KOTIKENTTÄ, MOMENTUM JA LOPPUTULOS
    # -------------------------------------------------------------
    koti_etu = koti_etu_perus + 0.035
    
    momentum = hae_momentum(koti_nimi, vieras_nimi)
    koti_etu += momentum

    ero = koti_etu - vieras_etu_perus
    koti_tod = 1 / (1 + np.exp(-5.0 * ero))
    vieras_tod = 1 - koti_tod

    # JUOKSUODOTUS (Sama kahden vaiheen logiikka)
    perus_odotus = 8.6 
    k_odotus_sp = (perus_odotus / 2) * (koti_woba / LIIGA_WOBA_KA) * (vieras_sp_xfip / LIIGA_XFIP_KA)
    k_odotus_bp = (perus_odotus / 2) * (koti_woba_bp / LIIGA_WOBA_KA) * (vieras_bp_xfip / LIIGA_XFIP_KA)
    k_odotus = (k_odotus_sp * vieras_sp_paino) + (k_odotus_bp * vieras_bp_paino)

    v_odotus_sp = (perus_odotus / 2) * (vieras_woba / LIIGA_WOBA_KA) * (koti_sp_xfip / LIIGA_XFIP_KA)
    v_odotus_bp = (perus_odotus / 2) * (vieras_woba_bp / LIIGA_WOBA_KA) * (koti_bp_xfip / LIIGA_XFIP_KA)
    v_odotus = (v_odotus_sp * koti_sp_paino) + (v_odotus_bp * koti_bp_paino)
    
    total_odotus = k_odotus + v_odotus

    # Lasketaan yhdistetty wOBA UI:ta varten näyttöön
    koti_woba_total = (koti_woba * vieras_sp_paino) + (koti_woba_bp * vieras_bp_paino)
    vieras_woba_total = (vieras_woba * koti_sp_paino) + (vieras_woba_bp * koti_bp_paino)

    return {
        "koti_voitto_tod": koti_tod,
        "vieras_voitto_tod": vieras_tod,
        "total_odotus": total_odotus,
        "k_odotus": k_odotus,
        "v_odotus": v_odotus,
        "koti_sp_dyn": koti_sp_xfip,
        "koti_bp_dyn": koti_bp_xfip,
        "koti_total_xfip": (koti_sp_xfip * koti_sp_paino) + (koti_bp_xfip * koti_bp_paino),
        "vieras_sp_dyn": vieras_sp_xfip,
        "vieras_bp_dyn": vieras_bp_xfip,
        "vieras_total_xfip": (vieras_sp_xfip * vieras_sp_paino) + (vieras_bp_xfip * vieras_bp_paino),
        "momentum_edge": momentum,
        "koti_woba_total": koti_woba_total,
        "vieras_woba_total": vieras_woba_total
    }
