import os
import sys
import psycopg2
import psycopg2.extras
import feedparser
import requests
from bs4 import BeautifulSoup

def esegui_scansione_notturna():
    # 1. Recupera la stringa di connessione dalle variabili d'ambiente di GitHub
    db_url = os.getenv("DB_URL")
    if not db_url:
        print("❌ Errore: Variabile DB_URL non configurata nei Secrets di GitHub.")
        sys.exit(1)

    print("📡 Connessione al database cloud Neon.tech...")
    try:
        conn = psycopg2.connect(db_url)
        # Carica le fonti attuali registrate nel DB
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM sources")
        fonti = cur.fetchall()
        
        if not fonti:
            print("📭 Nessuna fonte registrata nel database. Fine processo.")
            cur.close()
            conn.close()
            return

        print(f"📚 Trovate {len(fonti)} fonti da scansionare nel Radar.")
        articoli_scovati = []

        # 2. Avvia lo scraping dei feed RSS
        for f in fonti:
            print(f"🔍 Scansione canale: {f['nome']}...")
            try:
                feed = feedparser.parse(f['url'])
                for entry in feed.entries[:10]: # Leggiamo fino a 10 articoli per sicurezza
                    sommario = entry.summary if hasattr(entry, 'summary') else ""
                    preview = BeautifulSoup(sommario, "html.parser").get_text()[:250] + "..."
                    
                    articoli_scovati.append((
                        entry.title, 
                        entry.link, 
                        preview, 
                        f['macro'], 
                        f['area'], 
                        f['nome']
                    ))
            except Exception as e:
                print(f"⚠️ Errore durante lo scraping di {f['nome']}: {e}")
                continue

        # 3. Scrittura incrementale nel Database (Evita duplicati automaticamente)
        if articoli_scovati:
            print(f"💾 Inserimento di {len(articoli_scovati)} potenziali articoli nello storico...")
            query_insert = """
                INSERT INTO articles (titolo, link, preview, macro, area, fonte) 
                VALUES (%s, %s, %s, %s, %s, %s) 
                ON CONFLICT (link) DO NOTHING
            """
            cur.executemany(query_insert, articoli_scovati)
            conn.commit()
            print("✅ Archivio storico aggiornato con successo e senza duplicati!")
        else:
            print("ℹ️ Nessun nuovo articolo rilevato dai feed RSS.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Errore critico di database: {e}")
        sys.exit(1)

if __name__ == "__main__":
    esegui_scansione_notturna()
