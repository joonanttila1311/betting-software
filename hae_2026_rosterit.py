"""
hae_2026_rosterit.py
====================
Hakee MLB:n virallisesta Stats API:sta kaikkien 30 joukkueen
40-miehen rosterit kaudelle 2026, suodattaa kenttäpelaajat ja
tallentaa tuloksen rosterit_2026.json -tiedostoon.

Ohtani-sääntö:
  - Kaikki ei-syöttäjät ('P') otetaan mukaan
  - Two-Way Playerit ('TWP') otetaan aina mukaan
  - Syöttäjä ('P') otetaan mukaan JOS hänen person.id löytyy
    tietokannan lyojat_statcast-taulusta (eli hänellä on lyöntidataa)

Käyttö:
    python hae_2026_rosterit.py
"""

import json
import sqlite3
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# VAKIOT
# ---------------------------------------------------------------------------
DB_POLKU      = "mlb_historical.db"
OUTPUT_JSON   = "rosterit_2026.json"

MLB_API_BASE  = "https://statsapi.mlb.com/api/v1"
TEAMS_URL     = f"{MLB_API_BASE}/teams?sportId=1"
ROSTER_URL    = f"{MLB_API_BASE}/teams/{{team_id}}/roster/40Man"

TAUKO_SEK     = 0.3    # Kohteliaisuustauko API-kutsujen välillä (sek)
TIMEOUT_SEK   = 10     # HTTP-timeout per kutsu


# ---------------------------------------------------------------------------
# 1. HAE VALIDIT LYÖJÄ-ID:T TIETOKANNASTA
# ---------------------------------------------------------------------------

def hae_validit_lyojat(db_polku: str = DB_POLKU) -> set[int]:
    """
    Lukee lyojat_statcast-taulusta kaikki Batter_ID:t set-rakenteeseen.
    Jos taulu puuttuu, palautetaan tyhjä setti (Ohtani-sääntö ei aktivoidu).
    """
    if not Path(db_polku).exists():
        print(f"   ⚠️  Tietokantaa '{db_polku}' ei löydy – "
              "Ohtani-sääntö ei aktivoidu syöttäjille.")
        return set()

    try:
        yhteys = sqlite3.connect(db_polku)
        df_ids = yhteys.execute(
            "SELECT DISTINCT Batter_ID FROM lyojat_statcast"
        ).fetchall()
        yhteys.close()
        validit = {int(row[0]) for row in df_ids}
        print(f"   ✅ {len(validit):,} validia lyöjä-ID:tä luettu tietokannasta")
        return validit
    except sqlite3.Error as e:
        print(f"   ⚠️  Tietokantavirhe: {e} – Ohtani-sääntö ei aktivoidu.")
        return set()


# ---------------------------------------------------------------------------
# 2. HAE JOUKKUEET MLB API:STA
# ---------------------------------------------------------------------------

def hae_joukkueet() -> list[dict]:
    """
    Hakee kaikki MLB-joukkueet Stats API:sta.
    Palauttaa listan dict-objekteja: [{id, name, abbreviation}, ...]
    """
    print(f"\n🌐 Haetaan joukkueet: {TEAMS_URL}")
    try:
        vastaus = requests.get(TEAMS_URL, timeout=TIMEOUT_SEK)
        vastaus.raise_for_status()
        data = vastaus.json()
    except requests.RequestException as e:
        raise RuntimeError(f"❌ Joukkuehaku epäonnistui: {e}") from e

    joukkueet = [
        {
            "id":           t["id"],
            "name":         t["name"],
            "abbreviation": t.get("abbreviation", "???"),
        }
        for t in data.get("teams", [])
        if t.get("active", False)
    ]
    print(f"   → {len(joukkueet)} aktiivista joukkuetta löydetty")
    return sorted(joukkueet, key=lambda x: x["abbreviation"])


# ---------------------------------------------------------------------------
# 3. HAE 40-MIEHEN ROSTERI YHDELLE JOUKKUEELLE
# ---------------------------------------------------------------------------

def hae_rosteri(team_id: int) -> list[dict]:
    """
    Hakee yhden joukkueen 40-miehen rosterin Stats API:sta.
    Palauttaa raakalistan roster-objekteja tai tyhjän listan virhetilanteessa.
    """
    url = ROSTER_URL.format(team_id=team_id)
    try:
        vastaus = requests.get(url, timeout=TIMEOUT_SEK)
        vastaus.raise_for_status()
        return vastaus.json().get("roster", [])
    except requests.RequestException as e:
        print(f"      ⚠️  Rosterihaku epäonnistui (team_id={team_id}): {e}")
        return []


# ---------------------------------------------------------------------------
# 4. SUODATA KENTTÄPELAAJAT (OHTANI-SÄÄNTÖ)
# ---------------------------------------------------------------------------

# Syöttäjien tunnetut koodit MLB Stats API:ssa:
#   "1"       – numeerinen position.code (yleisin 40-man rosterilla)
#   "P"       – tekstimuotoinen lyhenne (abbreviation tai vanhempi code)
#   "Pitcher" – pitkä muoto (type.displayName)
SYOTTAJA_KOODIT: frozenset[str] = frozenset({"1", "P", "Pitcher"})
TWP_KOODIT:      frozenset[str] = frozenset({"TWP", "Y"})


def _ratkaise_pelipaikka(pelaaja: dict) -> str:
    """
    Hakee pelaajan pelipaikkakoodin kaikista tunnetuista API-poluista.
    Varmistaa myös, ettei kaadu, jos API palauttaa merkkijonon sanakirjan sijaan.
    """
    position   = pelaaja.get("position")  or {}
    person     = pelaaja.get("person")      or {}
    primary    = person.get("primaryPosition") or {}
    
    # Haetaan tyyppi. Se voi olla suoraan merkkijono (esim. "Pitcher").
    pos_type   = position.get("type", "")

    # Varmistetaan primaryPositionin tyyppi (API voi joskus palauttaa senkin stringinä)
    if isinstance(primary, str):
        primary_code = primary
        primary_abbr = primary
    else:
        primary_code = primary.get("code", "")
        primary_abbr = primary.get("abbreviation", "")

    for arvo in (
        position.get("code",         ""),   # polku 1 – "1" tai "CF" jne.
        position.get("abbreviation", ""),   # polku 2 – "P" tai "CF" jne.
        pos_type,                           # polku 3 – lisätyyppi (suoraan merkkijonona)
        primary_code,                       # polku 4 – henkilötason koodi
        primary_abbr,                       # polku 5 – henkilötason lyhenne
    ):
        # Varmistetaan että arvo on varmasti tekstiä ja poistetaan tyhjät välit
        arvo = str(arvo).strip()
        if arvo:
            return arvo

    return ""  # täysin tuntematon positio


def suodata_kenttapelaajat(
    rosteri: list[dict],
    validit_lyojat: set[int],
    team_abbr: str,
) -> tuple[list[dict], list[str]]:
    """
    Suodattaa 40-miehen rosterilta kenttäpelaajat Ohtani-säännöllä.

    Pelipaikan ratkaisu: _ratkaise_pelipaikka() tarkistaa 3 API-polkua.

    Mukaan otetaan pelaaja jos:
      (a) pos == 'TWP'                         → two-way player aina mukaan
      (b) pos ei ole 'P' (eikä tyhjä)          → normaali kenttäpelaaja
      (c) pos == 'P' JA person.id löytyy
          validit_lyojat-setistä               → Ohtani-sääntö

    Tyhjä pos (tuntematon) → otetaan mukaan, ei pudoteta vahingossa.

    Palauttaa:
        - lista kenttäpelaajia: [{"id": int, "name": str}, ...]
        - lista erikoistapauksia (TWP / Ohtani-sääntö) tekstimuodossa
    """
    kenttapelaajat   = []
    erikoistapaukset = []

    for pelaaja in rosteri:
        person        = pelaaja.get("person") or {}
        pid           = person.get("id")
        nimi          = person.get("fullName", "Tuntematon")
        pos           = _ratkaise_pelipaikka(pelaaja)
        pos_label     = pos or "?"

        nimi_muotoiltu = _muotoile_nimi(nimi)

        if pos in TWP_KOODIT:
            # Two-Way Player ("TWP" tai "Y") – aina mukaan
            kenttapelaajat.append({"id": pid, "name": nimi_muotoiltu})
            erikoistapaukset.append(
                f"      🔄 TWP: {nimi_muotoiltu} ({pos_label})"
            )

        elif pos in SYOTTAJA_KOODIT:
            # Syöttäjä ("1", "P" tai "Pitcher") – vain Ohtani-säännöllä mukaan
            if pid and int(pid) in validit_lyojat:
                kenttapelaajat.append({"id": pid, "name": nimi_muotoiltu})
                erikoistapaukset.append(
                    f"      ⚾ Ohtani-sääntö: {nimi_muotoiltu} "
                    f"({pos_label}, Batter_ID={pid} löytyy kannasta)"
                )
            # else: puhdas syöttäjä → jätetään hiljaa pois

        else:
            # Normaali kenttäpelaaja (tai täysin tuntematon pos → ei pudoteta)
            kenttapelaajat.append({"id": pid, "name": nimi_muotoiltu})

    return kenttapelaajat, erikoistapaukset


def _muotoile_nimi(koko_nimi: str) -> str:
    """
    Muuntaa 'Etunimi Sukunimi' → 'Sukunimi, Etunimi'.
    Käsittelee myös välilyönnilliset sukunimet (esim. 'Jazz Chisholm Jr.').
    """
    osat = koko_nimi.strip().split()
    if len(osat) == 0:
        return koko_nimi
    if len(osat) == 1:
        return osat[0]

    # Tunnistetaan Jr./Sr./II/III/IV -liitteet
    liitteet = {"jr.", "sr.", "ii", "iii", "iv", "v"}
    if osat[-1].lower().rstrip(".") in liitteet:
        # Esim. "Jazz Chisholm Jr." → "Chisholm Jr., Jazz"
        liite   = osat[-1]
        etunimi = osat[0]
        suku    = " ".join(osat[1:-1])
        return f"{suku} {liite}, {etunimi}"
    else:
        etunimi = osat[0]
        suku    = " ".join(osat[1:])
        return f"{suku}, {etunimi}"


# ---------------------------------------------------------------------------
# 5. PÄÄLOGIIKKA
# ---------------------------------------------------------------------------

def main() -> None:
    viiva = "═" * 62
    print(f"\n{viiva}")
    print(f"  ⚾  MLB 40-MAN ROSTER -HAKU  –  2026")
    print(viiva)

    # Askel 1: Hae validit lyöjä-ID:t kannasta
    print("\n📂 Luetaan validit lyöjä-ID:t tietokannasta ...")
    validit_lyojat = hae_validit_lyojat()

    # Askel 2: Hae joukkueet
    joukkueet = hae_joukkueet()

    # Askel 3–4: Käy joukkueet läpi
    rosterit: dict[str, list[dict]] = {}
    kaikki_erikoistapaukset: list[str] = []

    print(f"\n{'─'*62}")
    print(f"  Haetaan rosterit ({len(joukkueet)} joukkuetta) ...")
    print(f"{'─'*62}")

    for i, joukkue in enumerate(joukkueet, start=1):
        tid    = joukkue["id"]
        abbr   = joukkue["abbreviation"]
        nimi   = joukkue["name"]

        rosteri_raa = hae_rosteri(tid)
        kenttapelaajat, erikoistapaukset = suodata_kenttapelaajat(
            rosteri_raa, validit_lyojat, abbr
        )

        rosterit[abbr] = kenttapelaajat

        # Tulostus per joukkue
        erikois_merkki = " 🌟" if erikoistapaukset else ""
        print(
            f"  [{i:>2}/30] {abbr:<5} {nimi:<30} "
            f"→ {len(kenttapelaajat):>2} kenttäpelaajaa{erikois_merkki}"
        )

        if erikoistapaukset:
            for e in erikoistapaukset:
                print(e)
            kaikki_erikoistapaukset.extend(erikoistapaukset)

        time.sleep(TAUKO_SEK)

    # Askel 5: Tallenna JSON
    print(f"\n{'─'*62}")
    tallenna_json(rosterit)

    # Yhteenveto
    tulosta_yhteenveto(rosterit, kaikki_erikoistapaukset)

    print(f"\n{viiva}\n")


# ---------------------------------------------------------------------------
# 6. TALLENNUS
# ---------------------------------------------------------------------------

def tallenna_json(rosterit: dict, polku: str = OUTPUT_JSON) -> None:
    """Tallentaa rosteri-dictin JSON-tiedostoon."""
    try:
        with open(polku, "w", encoding="utf-8") as f:
            json.dump(rosterit, f, ensure_ascii=False, indent=2)
        koko = Path(polku).stat().st_size / 1024
        print(f"\n✅ Tallennettu → '{polku}'  ({koko:.1f} KB)")
    except OSError as e:
        raise RuntimeError(f"❌ Tiedoston kirjoitus epäonnistui: {e}") from e


# ---------------------------------------------------------------------------
# 7. YHTEENVETO
# ---------------------------------------------------------------------------

def tulosta_yhteenveto(
    rosterit: dict,
    erikoistapaukset: list[str],
) -> None:
    """Tulostaa loppuyhteenvedon."""
    viiva = "─" * 52
    kaikki_pelaajat = sum(len(v) for v in rosterit.values())

    print(f"\n{viiva}")
    print(f"  📊 YHTEENVETO")
    print(viiva)
    print(f"  Joukkueita:          {len(rosterit):>4}")
    print(f"  Pelaajia yhteensä:   {kaikki_pelaajat:>4}")
    print(f"  Keskiarvo/joukkue:   {kaikki_pelaajat/max(len(rosterit),1):>6.1f}")

    if erikoistapaukset:
        print(f"\n  🌟 Ohtani-säännöllä / TWP mukaan otetut ({len(erikoistapaukset)}):")
        for e in erikoistapaukset:
            print(f"  {e.strip()}")
    else:
        print(f"\n  ℹ️  Ei Ohtani-sääntö- tai TWP-aktivointeja.")

    print(viiva)


# ---------------------------------------------------------------------------
# PÄÄOHJELMA
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()