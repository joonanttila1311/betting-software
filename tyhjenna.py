import sqlite3

yhteys = sqlite3.connect("mlb_historical.db")
kursori = yhteys.cursor()
kursori.execute("DROP TABLE IF EXISTS statcast_2025")
yhteys.commit()
yhteys.close()
print("✅ Taulu 'statcast_2025' on tuhottu! Voit nyt ajaa fetch_statcast.py puhtaalta pöydältä.")