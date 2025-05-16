from kafka import KafkaProducer
import csv
import time

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'inspections'
CSV_FILE_PATH = r"C:\Users\houai\OneDrive\Documents\RT4\sem2\bigdata\project\sparkproject\cdph_environment.csv"

# Créer le producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: str(v).encode('utf-8')  # Encoder en UTF-8
)

# Lire le CSV et envoyer les lignes
with open(CSV_FILE_PATH, 'r', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)  # Sauter l'en-tête

    for row in reader:
        line = ",".join(row)  # Convertir la ligne en string
        print(f"Envoi : {line}")
        producer.send(KAFKA_TOPIC, value=line)  # Envoyer au topic
        time.sleep(0.5)  # Pause pour simuler un flux (ajuster selon besoin)

producer.close()