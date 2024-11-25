from kafka import KafkaProducer
import json
import time
import csv

# Initialisation du producteur Kafka
# Le producteur envoie des messages au cluster Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  
    value_serializer=lambda v: json.dumps(v).encode('utf-8') 
)

# Fonction pour streamer les données vers Kafka
def stream_population_data(file_path):
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)  
        batch = []  
        batch_size = 100  

        for idx, row in enumerate(reader):
            batch.append(row)  
            
            if (idx + 1) % batch_size == 0:
                producer.send('population_topic', batch)  
                print(f"Envoi d'un lot de {batch_size} enregistrements à Kafka")
                batch = []  
                time.sleep(10)  

       
        if batch:
            producer.send('population_topic', batch)
            print(f"Envoi du dernier lot de {len(batch)} enregistrements à Kafka")


file_path = "./data/raw/total-population.csv"
stream_population_data(file_path)


producer.flush()  
producer.close()  
