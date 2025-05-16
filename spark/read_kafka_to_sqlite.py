from kafka import KafkaConsumer
import sqlite3
import threading

# --- Création des tables une seule fois ---
conn = sqlite3.connect('inspections.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS results_summary (
    key TEXT,
    value INTEGER
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS daily_inspections (
    date TEXT,
    count INTEGER
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS monthly_trend (
    month TEXT,
    count INTEGER
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS failures_by_location (
    street_number TEXT,
    result TEXT,
    count INTEGER
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS results_by_date (
    date TEXT,
    count INTEGER
)
''')

conn.commit()
conn.close()


# --- Fonction pour consommer les messages Kafka et les insérer dans SQLite ---
def consume_and_store(topic, table):
    conn = sqlite3.connect('inspections.db')
    cursor = conn.cursor()

    consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092')
    print(f"[{topic}] En attente de données...")

    for msg in consumer:
        try:
            key = msg.key.decode('utf-8') if msg.key else "unknown"
            value = msg.value.decode('utf-8').strip()

            if topic == "failures_by_location":
                parts = value.split('-')
                if len(parts) == 3:
                    street_number, result, count = parts
                    if count.isdigit():
                        cursor.execute(f"""
                            INSERT INTO {table} (street_number, result, count)
                            VALUES (?, ?, ?)
                        """, (street_number, result, int(count)))
                        print(f"[{topic}] Reçu : {street_number} - {result} - {count}")
                    else:
                        print(f"[{topic}] Count invalide : {value}")
                else:
                    print(f"[{topic}] Format incorrect : {value}")

            elif topic in ["daily_inspections", "results_by_date"]:
                parts = value.split('-')
                if len(parts) == 2:
                    date, count = parts
                    if count.isdigit():
                        cursor.execute(f"""
                            INSERT INTO {table} (date, count)
                            VALUES (?, ?)
                        """, (date, int(count)))
                        print(f"[{topic}] Reçu : Date : {date}, Count : {count}")
                    else:
                        print(f"[{topic}] Count invalide : {value}")
                else:
                    print(f"[{topic}] Format incorrect : {value}")

            elif topic == "monthly_trend":
                parts = value.split('-')
                if len(parts) == 2:
                    month, count = parts
                    if count.isdigit():
                        cursor.execute(f"""
                            INSERT INTO {table} (month, count)
                            VALUES (?, ?)
                        """, (month, int(count)))
                        print(f"[{topic}] Reçu : Mois : {month}, Count : {count}")
                    else:
                        print(f"[{topic}] Count invalide : {value}")
                else:
                    print(f"[{topic}] Format incorrect : {value}")

            elif topic == "results_summary":
                parts = value.split('-', 1)
                if len(parts) == 2:
                    result, count = parts
                    if count.isdigit():
                        cursor.execute(f"""
                            INSERT INTO {table} (key, value)
                            VALUES (?, ?)
                        """, (result, int(count)))
                        print(f"[{topic}] Reçu : Résultat = {result}, Count = {count}")
                    else:
                        print(f"[{topic}] Count invalide : {value}")
                else:
                    print(f"[{topic}] Format incorrect : {value}")

            else:
                print(f"[{topic}] Topic inconnu ou non géré encore.")

            conn.commit()

        except Exception as e:
            print(f"[{topic}] Erreur : {e}")

    conn.close()


# --- Lancement des threads pour chaque topic ---
topics = [
    ("results_summary", "results_summary"),
    ("daily_inspections", "daily_inspections"),
    ("monthly_trend", "monthly_trend"),
    ("failures_by_location", "failures_by_location"),
    ("results_by_date", "results_by_date"),
]

for topic, table in topics:
    threading.Thread(target=lambda t=topic, tb=table: consume_and_store(t, tb)).start()
