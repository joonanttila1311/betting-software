"""
laskentamoottori.py – v5.0 (Dynamic wOBA & IP/GS Weighting)
==========================================================
Tämä on mallin aivot. Laskee voittotodennäköisyydet ja juoksuodotukset.
"""

import sqlite3
import pandas as pd
import numpy as np

DB_POLKU = "mlb_historical.db"
LIIGA_XFIP_KA = 3.80
LIIGA_WOBA_KA = 0.310

def lataa_data():
    """Lataa perustiedot tietokannasta laskentaa varten."""
    try:
        conn = sqlite3.connect(DB_POLKU)
        df = pd.read_sql_query("SELECT Team, Bullpen_xFIP_All FROM bullpen_statcast", conn)
        conn.close()
        return df
    except:
        return pd.DataFrame()

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
        "vieras_total_xfip": vieras_syotto_total
    }
