"""
hae_2026_rosterit.py
====================
Hakee MLB:n virallisesta Stats API:sta kaikkien 30 joukkueen
40-miehen rosterit kaudelle 2026 ja tallentaa tuloksen rosterit_2026.json -tiedostoon.
Ottaa mukaan KAIKKI 40 pelaajaa (myös syöttäjät ja uudet tulokkaat), 
jotta yksikään nimi ei puutu käyttöliittymästä ja wOBA-matematiikka saa kaikkien ID:t.
"""

import json
import time
from pathlib import Path
import requests

# ---------------------------------------------------------------------------
# VAKIOT
# ---------------------------------------------------------------------------
OUTPUT_JSON   = "rosterit_2026.json"
MLB_API_BASE  = "https://statsapi.mlb.com/api/v1"
TEAMS_URL     = f"{MLB_API_BASE}/teams?sportId=1"
ROSTER_URL    = f"{MLB_API_BASE}/teams/{{team_id}}/roster/40Man"

TAUKO_SEK     = 0.3    # Kohteliaisuustauko API-kutsujen välillä
TIMEOUT_SEK   = 10     # HTTP-timeout per kutsu

def hae_joukkueet() -> list[dict]:
    print(f"\n🌐 Haetaan joukkueet: {TEAMS_URL}")
    try:
        vastaus = requests.get(TEAMS_URL, timeout=TIMEOUT_SEK)
        vastaus.raise_for_status()
        data = vastaus.json()
    except requests.RequestException as e:
        raise RuntimeError(f"❌ Joukkuehaku epäonnistui: {e}") from e

    joukkueet = [
        {"id": t["id"], "name": t["name"], "abbreviation": t.get("abbreviation", "???")}
        for t in data.get("teams", []) if t.get("active", False)
    ]
    print(f"   → {len(joukkueet)} aktiivista joukkuetta löydetty")
    return sorted(joukkueet, key=lambda x: x["abbreviation"])

def hae_rosteri(team_id: int) -> list[dict]:
    url = ROSTER_URL.format(team_id=team_id)
    try:
        vastaus = requests.get(url, timeout=TIMEOUT_SEK)
        vastaus.raise_for_status()
        return vastaus.json().get("roster", [])
    except requests.RequestException as e:
        print(f"      ⚠️  Rosterihaku epäonnistui (team_id={team_id}): {e}")
        return []

def _muotoile_nimi(koko_nimi: str) -> str:
    """Muuntaa 'Etunimi Sukunimi' → 'Sukunimi, Etunimi'."""
    osat = koko_nimi.strip().split()
    if len(osat) <= 1:
        return koko_nimi

    liitteet = {"jr.", "sr.", "ii", "iii", "iv", "v"}
    if osat[-1].lower().rstrip(".") in liitteet:
        liite   = osat[-1]
        etunimi = osat[0]
        suku    = " ".join(osat[1:-1])
        return f"{suku} {liite}, {etunimi}"
    else:
        etunimi = osat[0]
        suku    = " ".join(osat[1:])
        return f"{suku}, {etunimi}"

def main() -> None:
    viiva = "═" * 62
    print(f"\n{viiva}\n  ⚾  MLB 40-MAN ROSTER -HAKU  –  2026 (KAIKKI PELAAJAT)\n{viiva}")

    joukkueet = hae_joukkueet()
    rosterit: dict[str, list[dict]] = {}
    kaikki_pelaajat_laskuri = 0

    print(f"\n{'─'*62}\n  Haetaan rosterit ({len(joukkueet)} joukkuetta) ...\n{'─'*62}")

    for i, joukkue in enumerate(joukkueet, start=1):
        tid    = joukkue["id"]
        abbr   = joukkue["abbreviation"]
        nimi   = joukkue["name"]

        rosteri_raa = hae_rosteri(tid)
        joukkueen_pelaajat = []

        # Otetaan sokeasti kaikki 40 pelaajaa mukaan
        for pelaaja in rosteri_raa:
            person = pelaaja.get("person") or {}
            pid = person.get("id")
            pelaaja_nimi = person.get("fullName", "Tuntematon")
            
            # Skipataan pelaaja, jos ID puuttuu (ei pitäisi tapahtua, mutta varmistus)
            if pid is None:
                print(f"      ⚠️  Skipataan pelaaja {pelaaja_nimi}: ID puuttuu")
                continue
            
            nimi_muotoiltu = _muotoile_nimi(pelaaja_nimi)
            
            joukkueen_pelaajat.append({"id": pid, "name": nimi_muotoiltu})

        rosterit[abbr] = joukkueen_pelaajat
        kaikki_pelaajat_laskuri += len(joukkueen_pelaajat)

        print(f"  [{i:>2}/30] {abbr:<5} {nimi:<30} → {len(joukkueen_pelaajat):>2} pelaajaa")
        time.sleep(TAUKO_SEK)

    print(f"\n{'─'*62}")
    
    # Tarkistetaan, jäikö jonkin joukkueen rosteri tyhjäksi
    tyhjat_joukkueet = [abbr for abbr, pelaajat in rosterit.items() if len(pelaajat) == 0]
    if tyhjat_joukkueet:
        print(f"⚠️  HUOM: {len(tyhjat_joukkueet)} joukkueen rosteri jäi tyhjäksi: {', '.join(tyhjat_joukkueet)}")
        print(f"   → Aja skripti uudestaan, jos haluat täydet rosterit.")
        print(f"{'─'*62}")
    
    # Tallennus
    try:
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(rosterit, f, ensure_ascii=False, indent=2)
        print(f"✅ Tallennettu → '{OUTPUT_JSON}' ({kaikki_pelaajat_laskuri} pelaajaa)")
    except OSError as e:
        print(f"❌ Tiedoston kirjoitus epäonnistui: {e}")

if __name__ == "__main__":
    main()